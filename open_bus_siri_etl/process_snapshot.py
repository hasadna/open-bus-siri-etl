import os
import json
import time
import datetime
from collections import defaultdict

import pytz
import sqlalchemy

from open_bus_stride_db.db import session_decorator, get_session, Session
from open_bus_stride_db.model import (
    SiriSnapshot, SiriSnapshotEtlStatusEnum, VehicleLocation, Ride, RouteStop, Route, Stop
)
import open_bus_siri_requester.storage
from .graceful_killer import GracefulKiller
from . import config
from . import logs


def iterate_monitored_stop_visits(snapshot_data):
    for stop_monitoring_delivery in snapshot_data['Siri']['ServiceDelivery']['StopMonitoringDelivery']:
        for monitored_stop_visit in stop_monitoring_delivery['MonitoredStopVisit']:
            yield monitored_stop_visit


def get_or_create_siri_snapshot(session, snapshot_id, force_reload):
    is_new_snapshot = False
    siri_snapshot = session.query(SiriSnapshot).filter(SiriSnapshot.snapshot_id == snapshot_id).one_or_none()
    if siri_snapshot is None:
        is_new_snapshot = True
        siri_snapshot = SiriSnapshot(
            snapshot_id=snapshot_id,
            etl_status=SiriSnapshotEtlStatusEnum.loading,
            etl_start_time=datetime.datetime.now(pytz.UTC)
        )
        session.add(siri_snapshot)
        session.commit()
    if siri_snapshot.etl_status == SiriSnapshotEtlStatusEnum.loading and not is_new_snapshot and not force_reload:
        raise Exception("snapshot is already in loading status (snapshot_id={})".format(snapshot_id))
    if not is_new_snapshot:
        siri_snapshot.etl_status = SiriSnapshotEtlStatusEnum.loading
        session.query(VehicleLocation).filter(VehicleLocation.siri_snapshot==siri_snapshot).delete()
        session.commit()
    return siri_snapshot


def parse_timestr(timestr):
    return datetime.datetime.strptime(timestr.replace(':', ''), '%Y-%m-%dT%H%M%S%z').astimezone(pytz.UTC)


def get_monitored_stop_visit_parse_errors_filename(snapshot_id):
    return os.path.join(config.OPEN_BUS_SIRI_ETL_ROOTPATH, 'monitored_stop_visits_parse_failed', snapshot_id, 'jsonlines')


def save_monitored_stop_visit_parse_error(monitored_stop_visit, snapshot_id):
    with open(get_monitored_stop_visit_parse_errors_filename(snapshot_id), 'a') as f:
        f.write(json.dumps(monitored_stop_visit)+"\n")


class ObjectsMaker:

    def __init__(self, stats=None):
        self.stats = stats if stats else defaultdict(int)
        self.routes_cache = {}
        self.stops_cache = {}
        self.route_stops_cache = {}
        self.rides_cache = {}

    def get_existing_object(self, objname, session, **kwargs):
        if objname == 'route':
            recorded_at_time = kwargs['recorded_at_time']
            recorded_at_datestr = recorded_at_time.strftime('%Y%m%d')
            if recorded_at_datestr not in self.routes_cache:
                self.routes_cache[recorded_at_datestr] = {}
                for route in session.query(Route).filter(
                    Route.min_date <= recorded_at_time.date(),
                    recorded_at_time.date() <= Route.max_date,
                    Route.is_from_gtfs == False
                ):
                    self.routes_cache[recorded_at_datestr].setdefault(route.operator_ref, {})[route.line_ref] = route
            return self.routes_cache[recorded_at_datestr].get(kwargs['operator_ref'], {}).get(kwargs['line_ref'])
        elif objname == 'stop':
            recorded_at_time = kwargs['recorded_at_time']
            recorded_at_datestr = recorded_at_time.strftime('%Y%m%d')
            if recorded_at_datestr not in self.stops_cache:
                self.stops_cache[recorded_at_datestr] = {}
                for stop in session.query(Stop).filter(
                        Stop.min_date <= recorded_at_time.date(),
                        recorded_at_time.date() <= Stop.max_date,
                        Stop.is_from_gtfs == False
                ):
                    self.stops_cache[recorded_at_datestr][int(stop.code)] = stop
            return self.stops_cache[recorded_at_datestr].get(int(kwargs['stop_point_ref']))
        elif objname == 'route_stop':
            recorded_at_time = kwargs['recorded_at_time']
            recorded_at_datestr = recorded_at_time.strftime('%Y%m%d')
            if recorded_at_datestr not in self.route_stops_cache:
                self.route_stops_cache[recorded_at_datestr] = {}
                for route_stop in session.query(RouteStop).join(Route).filter(
                    Route.min_date <= recorded_at_time.date(),
                    recorded_at_time.date() <= Route.max_date,
                    RouteStop.is_from_gtfs == False
                ):
                    self.route_stops_cache[recorded_at_datestr]['{}-{}-{}'.format(route_stop.stop.id, route_stop.route.id, route_stop.order)] = route_stop
            return self.route_stops_cache[recorded_at_datestr].get('{}-{}-{}'.format(kwargs['stop'].id, kwargs['route'].id, kwargs['order']))
        elif objname == 'ride':
            scheduled_start_time = kwargs['scheduled_start_time']
            scheduled_start_datestr = scheduled_start_time.strftime('%Y%m%d')
            if scheduled_start_datestr not in self.rides_cache:
                self.rides_cache[scheduled_start_datestr] = {}
                for ride in session.query(Ride).filter(
                    Ride.scheduled_start_time.cast(sqlalchemy.Date) == scheduled_start_time.date(),
                    Ride.is_from_gtfs == False
                ):
                    self.rides_cache[scheduled_start_datestr]['{}-{}-{}'.format(ride.route.id, ride.journey_ref, ride.vehicle_ref)] = ride
            return self.rides_cache[scheduled_start_datestr].get('{}-{}-{}'.format(kwargs['route'].id, kwargs['journey_ref'], kwargs['vehicle_ref']))
        else:
            raise Exception("invalid objname: {}".format(objname))

    def get_existing_object_nocache(self, objname, session, **kwargs):
        if objname == 'route':
            recorded_at_time = kwargs['recorded_at_time']
            return session.query(Route).filter(
                Route.min_date <= recorded_at_time.date(),
                recorded_at_time.date() <= Route.max_date,
                Route.is_from_gtfs == False,
                Route.operator_ref == int(kwargs['operator_ref']),
                Route.line_ref == int(kwargs['line_ref'])
            ).one_or_none()
        elif objname == 'stop':
            recorded_at_time = kwargs['recorded_at_time']
            return session.query(Stop).filter(
                Stop.min_date <= recorded_at_time.date(),
                recorded_at_time.date() <= Stop.max_date,
                Stop.is_from_gtfs == False,
                Stop.code == int(kwargs['stop_point_ref'])
            ).one_or_none()
        elif objname == 'route_stop':
            recorded_at_time = kwargs['recorded_at_time']
            return session.query(RouteStop).join(Route).join(Stop).filter(
                Route.min_date <= recorded_at_time.date(),
                recorded_at_time.date() <= Route.max_date,
                RouteStop.is_from_gtfs == False,
                Stop.id == kwargs['stop'].id,
                Route.id == kwargs['route'].id,
                RouteStop.order == int(kwargs['order'])
            ).one_or_none()
        elif objname == 'ride':
            scheduled_start_time = kwargs['scheduled_start_time']
            return session.query(Ride).join(Route).filter(
                Ride.scheduled_start_time == scheduled_start_time,
                Ride.is_from_gtfs == False,
                Route.id == kwargs['route'].id,
                Ride.journey_ref == kwargs['journey_ref'],
                Ride.vehicle_ref == kwargs['vehicle_ref']
            ).one_or_none()
        else:
            raise Exception("invalid objname: {}".format(objname))

    def create_new_object(self, objname, session, use_objectsmaker_cache, **kwargs):
        if objname == 'stop':
            recorded_at_time = kwargs['recorded_at_time']
            recorded_at_datestr = recorded_at_time.strftime('%Y%m%d')
            new_object = Stop(
                min_date=kwargs['recorded_at_time'].date(),
                max_date=kwargs['recorded_at_time'].date(),
                code=kwargs['stop_point_ref'],
                is_from_gtfs=False
            )
            if use_objectsmaker_cache:
                self.stops_cache.setdefault(recorded_at_datestr, {})[int(kwargs['stop_point_ref'])] = new_object
        elif objname == 'route':
            recorded_at_time = kwargs['recorded_at_time']
            recorded_at_datestr = recorded_at_time.strftime('%Y%m%d')
            new_object = Route(
                min_date=kwargs['recorded_at_time'].date(),
                max_date=kwargs['recorded_at_time'].date(),
                line_ref=kwargs['line_ref'],
                operator_ref=kwargs['operator_ref'],
                is_from_gtfs=False
            )
            if use_objectsmaker_cache:
                self.routes_cache.setdefault(recorded_at_datestr, {}).setdefault(kwargs['operator_ref'], {})[kwargs['line_ref']] = new_object
        elif objname == 'ride':
            scheduled_start_time = kwargs['scheduled_start_time']
            scheduled_start_datestr = scheduled_start_time.strftime('%Y%m%d')
            new_object = Ride(
                route=kwargs['route'],
                journey_ref=kwargs['journey_ref'],
                scheduled_start_time=kwargs['scheduled_start_time'],
                vehicle_ref=kwargs['vehicle_ref'],
                is_from_gtfs=False
            )
            if use_objectsmaker_cache:
                self.rides_cache.setdefault(scheduled_start_datestr, {})['{}-{}-{}'.format(kwargs['route'].id, kwargs['journey_ref'], kwargs['vehicle_ref'])] = new_object
        elif objname == 'route_stop':
            recorded_at_time = kwargs['recorded_at_time']
            recorded_at_datestr = recorded_at_time.strftime('%Y%m%d')
            new_object = RouteStop(
                stop=kwargs['stop'],
                route=kwargs['route'],
                order=kwargs['order'],
                is_from_gtfs=False
            )
            if use_objectsmaker_cache:
                self.route_stops_cache.setdefault(recorded_at_datestr, {})['{}-{}-{}'.format(kwargs['stop'].id, kwargs['route'].id, kwargs['order'])] = new_object
        else:
            raise Exception('invalid objname: {}'.format(objname))
        session.add(new_object)
        return new_object

    def get_or_create(self, objname, session, use_objectsmaker_cache, **kwargs):
        with logs.debug_time_stats('{}_get'.format(objname), self.stats, log_if_more_then_seconds=1):
            if use_objectsmaker_cache:
                existing_object = self.get_existing_object(objname, session, **kwargs)
            else:
                existing_object = self.get_existing_object_nocache(objname, session, **kwargs)
        if existing_object:
            return existing_object
        else:
            with logs.debug_time_stats('{}_add'.format(objname), self.stats, log_if_more_then_seconds=1):
                new_object = self.create_new_object(objname, session, use_objectsmaker_cache, **kwargs)
            return new_object


def parse_monitored_stop_visit(session, monitored_stop_visit, snapshot_id, objects_maker, use_objectsmaker_cache):
    try:
        recorded_at_time = parse_timestr(monitored_stop_visit['RecordedAtTime'])
        line_ref = int(monitored_stop_visit['MonitoredVehicleJourney']['LineRef'])
        operator_ref = int(monitored_stop_visit['MonitoredVehicleJourney']['OperatorRef'])
        journey_ref = monitored_stop_visit['MonitoredVehicleJourney']['FramedVehicleJourneyRef']['DataFrameRef'] + '-' + monitored_stop_visit['MonitoredVehicleJourney']['FramedVehicleJourneyRef']['DatedVehicleJourneyRef']
        scheduled_start_time = parse_timestr(monitored_stop_visit['MonitoredVehicleJourney']['OriginAimedDepartureTime'])
        vehicle_ref = monitored_stop_visit['MonitoredVehicleJourney'].get('VehicleRef', '')
        order = monitored_stop_visit['MonitoredVehicleJourney']['MonitoredCall']['Order']
        stop_point_ref = monitored_stop_visit['MonitoredVehicleJourney']['MonitoredCall']['StopPointRef']
        lon = float(monitored_stop_visit['MonitoredVehicleJourney']['VehicleLocation']['Longitude'])
        lat = float(monitored_stop_visit['MonitoredVehicleJourney']['VehicleLocation']['Latitude'])
        bearing = int(monitored_stop_visit['MonitoredVehicleJourney'].get('Bearing', -1))
        velocity = int(monitored_stop_visit['MonitoredVehicleJourney'].get('Velocity', -1))
        distance_from_journey_start = int(monitored_stop_visit['MonitoredVehicleJourney']['MonitoredCall'].get('DistanceFromStop', -1))
    except:
        save_monitored_stop_visit_parse_error(monitored_stop_visit, snapshot_id)
        if config.DEBUG:
            print("Failed to parse monitored stop visit: {}".format(monitored_stop_visit))
        return None
    route = objects_maker.get_or_create('route', session, use_objectsmaker_cache, recorded_at_time=recorded_at_time, line_ref=line_ref, operator_ref=operator_ref)
    ride = objects_maker.get_or_create('ride', session, use_objectsmaker_cache, journey_ref=journey_ref, route=route, scheduled_start_time=scheduled_start_time, vehicle_ref=vehicle_ref)
    stop = objects_maker.get_or_create('stop', session, use_objectsmaker_cache, recorded_at_time=recorded_at_time, stop_point_ref=stop_point_ref)
    route_stop = objects_maker.get_or_create('route_stop', session, use_objectsmaker_cache, stop=stop, route=route, order=order, recorded_at_time=recorded_at_time)
    return {
        'ride': ride,
        'route_stop': route_stop,
        'recorded_at_time': recorded_at_time,
        'lon': lon,
        'lat': lat,
        'bearing': bearing,
        'velocity': velocity,
        'distance_from_journey_start': distance_from_journey_start
    }


def process_snapshot(snapshot_id, session=None, force_reload=False, snapshot_data=None, objects_maker=None,
                     force_cache=False, force_no_cache=False):
    if force_cache:
        assert not force_no_cache
        use_objectsmaker_cache = True
    elif force_no_cache:
        use_objectsmaker_cache = False
    else:
        use_objectsmaker_cache = config.OPEN_BUS_SIRI_ETL_USE_OBJECTSMAKER_CACHE
    with logs.debug_time('process_snapshot', snapshot_id=snapshot_id):
        print("Processing snapshot: {} (use_objectsmaker_cache={})".format(snapshot_id, use_objectsmaker_cache))
        with get_session(session) as session:
            if objects_maker is None:
                objects_maker = ObjectsMaker()
            if snapshot_data is None:
                with logs.debug_time('open_bus_siri_requester.storage.read', snapshot_id=snapshot_id):
                    snapshot_data = open_bus_siri_requester.storage.read(snapshot_id)
            monitored_stop_visit_parse_errors_filename = get_monitored_stop_visit_parse_errors_filename(snapshot_id)
            if os.path.exists(monitored_stop_visit_parse_errors_filename):
                os.unlink(monitored_stop_visit_parse_errors_filename)
            else:
                os.makedirs(os.path.dirname(monitored_stop_visit_parse_errors_filename), exist_ok=True)
            error, monitored_stop_visit, siri_snapshot, is_new_snapshot = None, None, None, None
            num_failed_parse_vehicle_locations, num_successful_parse_vehicle_locations = 0, 0
            stats = objects_maker.stats = defaultdict(int)
            try:
                for monitored_stop_visit in iterate_monitored_stop_visits(snapshot_data):
                    if siri_snapshot is None:
                        with logs.debug_time('get_or_create_siri_snapshot', snapshot_id=snapshot_id):
                            siri_snapshot = get_or_create_siri_snapshot(session, snapshot_id, force_reload)
                    parsed_monitored_stop_visit = parse_monitored_stop_visit(session, monitored_stop_visit, snapshot_id, objects_maker, use_objectsmaker_cache)
                    if parsed_monitored_stop_visit:
                        num_successful_parse_vehicle_locations += 1
                        vehicle_location = VehicleLocation(
                            siri_snapshot=siri_snapshot,
                            **parsed_monitored_stop_visit
                        )
                        with logs.debug_time_stats('vehicle_location_add', stats, log_if_more_then_seconds=1):
                            session.add(vehicle_location)
                    else:
                        num_failed_parse_vehicle_locations += 1
                if config.DEBUG:
                    for title in [
                        'ride_get', 'ride_add', 'route_get', 'route_add',
                        'route_stop_get', 'route_stop_add',
                        'stop_get', 'stop_add',
                        'vehicle_location_add'
                    ]:
                        total_seconds = stats['{}-total-seconds'.format(title)]
                        total_calls = stats['{}-total-calls'.format(title)]
                        if total_calls > 0:
                            print('avg. {} call seconds: {} ({} / {})'.format(title, total_seconds / total_calls, total_seconds, total_calls))
            except Exception as e:
                print("Unexpected exception processing monitored_stop_visit {}".format(monitored_stop_visit))
                session.rollback()
                if siri_snapshot:
                    siri_snapshot.etl_status = SiriSnapshotEtlStatusEnum.error
                    siri_snapshot.error = str(e)
                    siri_snapshot.etl_end_time = datetime.datetime.now(pytz.UTC)
                    siri_snapshot.num_failed_parse_vehicle_locations = num_failed_parse_vehicle_locations
                    siri_snapshot.num_successful_parse_vehicle_locations = num_successful_parse_vehicle_locations
                session.commit()
                raise
            else:
                siri_snapshot.etl_status = SiriSnapshotEtlStatusEnum.loaded
                siri_snapshot.error = ''
                siri_snapshot.etl_end_time = datetime.datetime.now(pytz.UTC)
                siri_snapshot.num_failed_parse_vehicle_locations = num_failed_parse_vehicle_locations
                siri_snapshot.num_successful_parse_vehicle_locations = num_successful_parse_vehicle_locations
                with logs.debug_time('session.commit', snapshot_id=snapshot_id):
                    session.commit()


@session_decorator
def process_new_snapshots(session, limit=None, last_snapshots_timedelta=None, now=None, graceful_killer=None):
    if limit:
        limit = int(limit)
    if not last_snapshots_timedelta:
        last_snapshots_timedelta = dict(days=7)
    if not now:
        now = datetime.datetime.now(pytz.UTC)
    last_loaded_snapshot = session.query(SiriSnapshot)\
        .filter(SiriSnapshot.etl_status == SiriSnapshotEtlStatusEnum.loaded)\
        .order_by(sqlalchemy.desc(SiriSnapshot.snapshot_id))\
        .first()
    if last_loaded_snapshot:
        print('last loaded snapshot_id: {}'.format(last_loaded_snapshot.snapshot_id))
        cur_datetime = datetime.datetime.strptime(last_loaded_snapshot.snapshot_id + 'z+0000', '%Y/%m/%d/%H/%Mz%z') + datetime.timedelta(minutes=1)
    else:
        print('no last loaded snapshot, getting snapshots from last {}'.format(last_snapshots_timedelta))
        cur_datetime = now - datetime.timedelta(**last_snapshots_timedelta)
    stats = defaultdict(int)
    objects_maker = ObjectsMaker()
    while cur_datetime <= now and (not limit or stats['processed'] <= limit):
        if graceful_killer and graceful_killer.kill_now:
            break
        stats['attempted'] += 1
        snapshot_id = cur_datetime.strftime('%Y/%m/%d/%H/%M')
        try:
            snapshot_data = open_bus_siri_requester.storage.read(snapshot_id)
        except:
            snapshot_data = None
        if snapshot_data:
            process_snapshot(session=session, snapshot_id=snapshot_id, snapshot_data=snapshot_data, objects_maker=objects_maker)
            stats['processed'] += 1
        cur_datetime = cur_datetime + datetime.timedelta(minutes=1)
    if config.DEBUG:
        print('processed {} snapshots out of {} attempted'.format(stats['processed'], stats['attempted']))
    return stats


def start_process_new_snapshots_daemon():
    graceful_killer = GracefulKiller()
    while not graceful_killer.kill_now:
        start_time = datetime.datetime.now(pytz.UTC)
        stats = process_new_snapshots(graceful_killer=graceful_killer)
        if stats['processed'] > 0:
            print('processed {} snapshots (attempted {})'.format(stats['processed'], stats['attempted']))
        elif stats['attempted'] > 0:
            print('attempted {} snapshots'.format(stats['attempted']))
        if graceful_killer.kill_now:
            break
        elapsed_seconds = (datetime.datetime.now(pytz.UTC) - start_time).total_seconds()
        if elapsed_seconds < 60:
            time.sleep(60 - elapsed_seconds)
        else:
            time.sleep(5)
