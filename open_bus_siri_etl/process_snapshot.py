import os
import subprocess
import sys
import json
import time
import datetime
import traceback
from collections import defaultdict

import pytz
import sqlalchemy

from open_bus_stride_db.db import session_decorator, get_session, Session
from open_bus_stride_db.model import (
    SiriSnapshot, SiriSnapshotEtlStatusEnum,
    SiriVehicleLocation, SiriRide, SiriRideStop, SiriRoute, SiriStop
)
import open_bus_siri_requester.storage
from .graceful_killer import GracefulKiller
from . import config
from . import logs
from . import common


def iterate_monitored_stop_visits(snapshot_data):
    for stop_monitoring_delivery in snapshot_data['Siri']['ServiceDelivery']['StopMonitoringDelivery']:
        for monitored_stop_visit in stop_monitoring_delivery['MonitoredStopVisit']:
            yield monitored_stop_visit


def parse_timestr(timestr):
    return datetime.datetime.strptime(timestr, '%Y-%m-%dT%H:%M:%S%z')


def get_monitored_stop_visit_parse_errors_filename(snapshot_id):
    # this path is periodically cleared and backed-up by open-bus-stride-etl siri backup-cleanup task
    return os.path.join(config.OPEN_BUS_SIRI_ETL_ROOTPATH, 'monitored_stop_visits_parse_failed', snapshot_id, 'jsonlines')


def save_monitored_stop_visit_parse_error(monitored_stop_visit, snapshot_id):
    with open(get_monitored_stop_visit_parse_errors_filename(snapshot_id), 'a') as f:
        f.write(json.dumps(monitored_stop_visit)+"\n")


class ObjectsMaker:

    def __init__(self, stats=None):
        self.stats = stats if stats else defaultdict(int)
        self.siri_routes_cache = {}
        self.siri_stops_cache = {}
        self.siri_ride_stops_cache = {}
        self.siri_rides_cache = {}

    def __sizeof__(self):
        return sum(map(sys.getsizeof, (
            self.stats, self.siri_routes_cache, self.siri_stops_cache,
            self.siri_ride_stops_cache, self.siri_rides_cache
        )))

    def get_cache_key(self, objname, pmsv):
        if objname == 'siri_route':
            return '{}-{}'.format(pmsv['operator_ref'], pmsv['line_ref'])
        elif objname == 'siri_stop':
            return int(pmsv['stop_point_ref'])
        elif objname == 'siri_ride':
            siri_route = self.get_cache_value('siri_route', pmsv)
            return '{}-{}-{}'.format(siri_route.id, pmsv['journey_ref'], pmsv['vehicle_ref'])
        elif objname == 'siri_ride_stop':
            siri_ride = self.get_cache_value('siri_ride', pmsv)
            siri_stop = self.get_cache_value('siri_stop', pmsv)
            return '{}-{}-{}'.format(siri_ride.id, siri_stop.id, pmsv['order'])
        else:
            raise Exception('unknown objname: {}'.format(objname))

    def get_cache_value(self, objname, pmsv, required=True):
        if objname == 'siri_route':
            res = self.siri_routes_cache.get(self.get_cache_key(objname, pmsv))
        elif objname == 'siri_stop':
            res = self.siri_stops_cache.get(self.get_cache_key(objname, pmsv))
        elif objname == 'siri_ride':
            res = self.siri_rides_cache.get(self.get_cache_key(objname, pmsv))
        elif objname == 'siri_ride_stop':
            res = self.siri_ride_stops_cache.get(self.get_cache_key(objname, pmsv))
        else:
            raise Exception('unknown objname: {}'.format(objname))
        if required and res is None:
            raise Exception('Failed to get object {} from cache with key {}'.format(objname, self.get_cache_key(objname, pmsv)))
        return res

    def set_cache_value(self, objname, pmsv, value):
        if objname == 'siri_route':
            self.siri_routes_cache[self.get_cache_key(objname, pmsv)] = value
        elif objname == 'siri_stop':
            self.siri_stops_cache[self.get_cache_key(objname, pmsv)] = value
        elif objname == 'siri_ride':
            self.siri_rides_cache[self.get_cache_key(objname, pmsv)] = value
        elif objname == 'siri_ride_stop':
            self.siri_ride_stops_cache[self.get_cache_key(objname, pmsv)] = value
        else:
            raise Exception('unknown objname: {}'.format(objname))

    def is_cache_value_exists(self, objname, pmsv):
        return self.get_cache_value(objname, pmsv, required=False) is not None

    def get_or_create_siri_routes_stops(self, session, parsed_monitored_stop_visits, heartbeat):
        siri_route_fetch_keys = set()
        siri_stop_fetch_keys = set()
        for pmsv in parsed_monitored_stop_visits:
            if not self.is_cache_value_exists('siri_route', pmsv):
                siri_route_fetch_keys.add((int(pmsv['operator_ref']), int(pmsv['line_ref'])))
            if not self.is_cache_value_exists('siri_stop', pmsv):
                siri_stop_fetch_keys.add(int(pmsv['stop_point_ref']))
        heartbeat()
        for siri_route in session.query(SiriRoute).filter(
                sqlalchemy.tuple_(SiriRoute.operator_ref, SiriRoute.line_ref).in_(siri_route_fetch_keys)
        ):
            self.siri_routes_cache['{}-{}'.format(siri_route.operator_ref, siri_route.line_ref)] = siri_route
            heartbeat()
        for siri_stop in session.query(SiriStop).filter(
                SiriStop.code.in_(siri_stop_fetch_keys)
        ):
            self.siri_stops_cache[siri_stop.code] = siri_stop
        heartbeat()
        for pmsv in parsed_monitored_stop_visits:
            if not self.is_cache_value_exists('siri_route', pmsv):
                siri_route = SiriRoute(operator_ref=int(pmsv['operator_ref']), line_ref=int(pmsv['line_ref']))
                session.add(siri_route)
                self.set_cache_value('siri_route', pmsv, siri_route)
                self.stats['num_added_siri_routes'] += 1
            if not self.is_cache_value_exists('siri_stop', pmsv):
                siri_stop = SiriStop(code=int(pmsv['stop_point_ref']))
                session.add(siri_stop)
                self.set_cache_value('siri_stop', pmsv, siri_stop)
                self.stats['num_added_siri_stops'] += 1
            heartbeat()

    def get_or_create_siri_rides(self, session, parsed_monitored_stop_visits, heartbeat):
        siri_ride_fetch_keys = set()
        for pmsv in parsed_monitored_stop_visits:
            if not self.is_cache_value_exists('siri_ride', pmsv):
                siri_ride_fetch_keys.add(
                    (
                        self.get_cache_value('siri_route', pmsv).id,
                        pmsv['journey_ref'],
                        pmsv['vehicle_ref']
                    )
                )
        heartbeat()
        for siri_ride in session.query(SiriRide).filter(
                sqlalchemy.tuple_(SiriRide.siri_route_id, SiriRide.journey_ref, SiriRide.vehicle_ref).in_(siri_ride_fetch_keys)
        ):
            self.siri_rides_cache['{}-{}-{}'.format(
                siri_ride.siri_route_id,
                siri_ride.journey_ref,
                siri_ride.vehicle_ref
            )] = siri_ride
            heartbeat()
        for pmsv in parsed_monitored_stop_visits:
            if not self.is_cache_value_exists('siri_ride', pmsv):
                siri_ride = SiriRide(
                    siri_route=self.get_cache_value('siri_route', pmsv),
                    journey_ref=pmsv['journey_ref'],
                    scheduled_start_time=pmsv['scheduled_start_time'],
                    vehicle_ref=pmsv['vehicle_ref']
                )
                session.add(siri_ride)
                self.set_cache_value('siri_ride', pmsv, siri_ride)
                self.stats['num_added_siri_rides'] += 1
            heartbeat()

    def get_or_create_siri_ride_stops(self, session, parsed_monitored_stop_visits, heartbeat):
        siri_ride_stop_fetch_keys = set()
        for pmsv in parsed_monitored_stop_visits:
            if not self.is_cache_value_exists('siri_ride_stop', pmsv):
                siri_ride_stop_fetch_keys.add(
                    (
                        self.get_cache_value('siri_ride', pmsv).id,
                        self.get_cache_value('siri_stop', pmsv).id,
                        int(pmsv['order'])
                    )
                )
        heartbeat()
        for siri_ride_stop in session.query(SiriRideStop).filter(
                sqlalchemy.tuple_(
                    SiriRideStop.siri_ride_id, SiriRideStop.siri_stop_id, SiriRideStop.order
                ).in_(siri_ride_stop_fetch_keys)
        ):
            self.siri_ride_stops_cache['{}-{}-{}'.format(
                siri_ride_stop.siri_ride_id, siri_ride_stop.siri_stop_id, siri_ride_stop.order
            )] = siri_ride_stop
            heartbeat()
        for pmsv in parsed_monitored_stop_visits:
            if not self.is_cache_value_exists('siri_ride_stop', pmsv):
                siri_ride_stop = SiriRideStop(
                    siri_stop=self.get_cache_value('siri_stop', pmsv),
                    siri_ride=self.get_cache_value('siri_ride', pmsv),
                    order=int(pmsv['order'])
                )
                session.add(siri_ride_stop)
                self.set_cache_value('siri_ride_stop', pmsv, siri_ride_stop)
                self.stats['num_added_siri_ride_stops'] += 1
            heartbeat()

    def get_or_create_objects(self, session, parsed_monitored_stop_visits, heartbeat):
        self.get_or_create_siri_routes_stops(session, parsed_monitored_stop_visits, heartbeat)
        self.get_or_create_siri_rides(session, parsed_monitored_stop_visits, heartbeat)
        self.get_or_create_siri_ride_stops(session, parsed_monitored_stop_visits, heartbeat)


def parse_monitored_stop_visit(monitored_stop_visit, snapshot_id):
    try:
        return dict(
            recorded_at_time=parse_timestr(monitored_stop_visit['RecordedAtTime']),
            line_ref=int(monitored_stop_visit['MonitoredVehicleJourney']['LineRef']),
            operator_ref=int(monitored_stop_visit['MonitoredVehicleJourney']['OperatorRef']),
            journey_ref=monitored_stop_visit['MonitoredVehicleJourney']['FramedVehicleJourneyRef']['DataFrameRef'] + '-' + monitored_stop_visit['MonitoredVehicleJourney']['FramedVehicleJourneyRef']['DatedVehicleJourneyRef'],
            scheduled_start_time=parse_timestr(monitored_stop_visit['MonitoredVehicleJourney']['OriginAimedDepartureTime']),
            vehicle_ref=monitored_stop_visit['MonitoredVehicleJourney'].get('VehicleRef', ''),
            order=monitored_stop_visit['MonitoredVehicleJourney']['MonitoredCall']['Order'],
            stop_point_ref=monitored_stop_visit['MonitoredVehicleJourney']['MonitoredCall']['StopPointRef'],
            lon=float(monitored_stop_visit['MonitoredVehicleJourney']['VehicleLocation']['Longitude']),
            lat=float(monitored_stop_visit['MonitoredVehicleJourney']['VehicleLocation']['Latitude']),
            bearing=int(monitored_stop_visit['MonitoredVehicleJourney'].get('Bearing', -1)),
            velocity=int(monitored_stop_visit['MonitoredVehicleJourney'].get('Velocity', -1)),
            distance_from_journey_start=int(monitored_stop_visit['MonitoredVehicleJourney']['MonitoredCall'].get('DistanceFromStop', -1)),
        )
    except:
        save_monitored_stop_visit_parse_error(monitored_stop_visit, snapshot_id)
        if config.DEBUG:
            print("Failed to parse monitored stop visit: {}".format(monitored_stop_visit))
        return None


def get_or_create_siri_snapshot(session, snapshot_id, force_reload):
    created_by = ''
    try:
        res, out = subprocess.getstatusoutput('hostname')
        if res == 0:
            created_by = out
    except:
        traceback.print_exc()
    is_new_snapshot = False
    siri_snapshot = session.query(SiriSnapshot).filter(SiriSnapshot.snapshot_id == snapshot_id).one_or_none()
    if siri_snapshot is None:
        is_new_snapshot = True
        siri_snapshot = SiriSnapshot(
            snapshot_id=snapshot_id,
            etl_status=SiriSnapshotEtlStatusEnum.loading,
            etl_start_time=common.now(),
            last_heartbeat=common.now(),
            created_by=created_by
        )
        session.add(siri_snapshot)
        session.commit()
    if (
        siri_snapshot.etl_status == SiriSnapshotEtlStatusEnum.loading
        and not is_new_snapshot
        and not force_reload
        and siri_snapshot.last_heartbeat
        and (common.now() - siri_snapshot.last_heartbeat).total_seconds() < 120
    ):
        raise Exception("snapshot is already in loading status and last heartbeat was less then 2 minutes ago (snapshot_id={})".format(snapshot_id))
    if not is_new_snapshot:
        siri_snapshot.etl_status = SiriSnapshotEtlStatusEnum.loading
        siri_snapshot.etl_start_time = common.now()
        siri_snapshot.last_heartbeat = common.now()
        siri_snapshot.created_by = created_by
        for attr in ['error', 'num_successful_parse_vehicle_locations', 'num_failed_parse_vehicle_locations',
                     'num_added_siri_rides', 'num_added_siri_ride_stops', 'num_added_siri_routes',
                     'num_added_siri_stops']:
            setattr(siri_snapshot, attr, None)
        session.query(SiriVehicleLocation).filter(SiriVehicleLocation.siri_snapshot==siri_snapshot).delete()
        session.commit()
    return siri_snapshot


def update_siri_snapshot_stats(siri_snapshot, stats):
    for objname in ['siri_stop', 'siri_route', 'siri_ride', 'siri_ride_stop']:
        stat = 'num_added_{}s'.format(objname)
        setattr(siri_snapshot, stat, stats[stat])


def update_siri_snapshot_error(session, siri_snapshot, error_str,
                               num_failed_parse_vehicle_locations,
                               num_successful_parse_vehicle_locations,
                               stats):
    siri_snapshot.etl_status = SiriSnapshotEtlStatusEnum.error
    siri_snapshot.error = error_str
    siri_snapshot.etl_end_time = datetime.datetime.now(datetime.timezone.utc)
    siri_snapshot.num_failed_parse_vehicle_locations = num_failed_parse_vehicle_locations
    siri_snapshot.num_successful_parse_vehicle_locations = num_successful_parse_vehicle_locations
    update_siri_snapshot_stats(siri_snapshot, stats)
    session.commit()


def update_siri_snapshot_loaded(session, siri_snapshot,
                                num_failed_parse_vehicle_locations,
                                num_successful_parse_vehicle_locations,
                                stats):
    siri_snapshot.etl_status = SiriSnapshotEtlStatusEnum.loaded
    siri_snapshot.error = ''
    siri_snapshot.etl_end_time = datetime.datetime.now(datetime.timezone.utc)
    siri_snapshot.num_failed_parse_vehicle_locations = num_failed_parse_vehicle_locations
    siri_snapshot.num_successful_parse_vehicle_locations = num_successful_parse_vehicle_locations
    update_siri_snapshot_stats(siri_snapshot, stats)
    session.commit()


def update_siri_snapshot_heartbeat(session, siri_snapshot):
    now = common.now()
    if (now - siri_snapshot.last_heartbeat).total_seconds() > 5:
        print('updating heartbeat')
        siri_snapshot.last_heartbeat = now
        session.commit()


def process_snapshot(snapshot_id, session=None, force_reload=False, snapshot_data=None, objects_maker=None):
    with logs.debug_time('process_snapshot', snapshot_id=snapshot_id):
        print("Processing snapshot: {}".format(snapshot_id))
        with get_session() as siri_snapshot_session:
            with get_session(session) as session:
                if objects_maker is None:
                    objects_maker = ObjectsMaker()
                if snapshot_data is None:
                    snapshot_data = open_bus_siri_requester.storage.read(snapshot_id)
                monitored_stop_visit_parse_errors_filename = get_monitored_stop_visit_parse_errors_filename(snapshot_id)
                if os.path.exists(monitored_stop_visit_parse_errors_filename):
                    os.unlink(monitored_stop_visit_parse_errors_filename)
                else:
                    os.makedirs(os.path.dirname(monitored_stop_visit_parse_errors_filename), exist_ok=True)
                error, monitored_stop_visit, is_new_snapshot = None, None, None
                num_failed_parse_vehicle_locations = 0
                stats = objects_maker.stats = defaultdict(int)
                with logs.debug_time('get_or_create_siri_snapshot', snapshot_id=snapshot_id):
                    siri_snapshot = get_or_create_siri_snapshot(siri_snapshot_session, snapshot_id, force_reload)

                def heartbeat():
                    update_siri_snapshot_heartbeat(siri_snapshot_session, siri_snapshot)

                try:
                    parsed_monitored_stop_visits = []
                    num_failed_parse_vehicle_locations = 0
                    with logs.debug_time('parse_monitored_stop_visits', snapshot_id=snapshot_id):
                        for monitored_stop_visit in iterate_monitored_stop_visits(snapshot_data):
                            parsed_monitored_stop_visit = parse_monitored_stop_visit(monitored_stop_visit, snapshot_id)
                            if parsed_monitored_stop_visit:
                                parsed_monitored_stop_visits.append(parsed_monitored_stop_visit)
                            else:
                                num_failed_parse_vehicle_locations += 1
                            heartbeat()
                    with logs.debug_time('objects_maker_get_or_create_objects', snapshot_id=snapshot_id):
                        objects_maker.get_or_create_objects(session, parsed_monitored_stop_visits, heartbeat)
                    with logs.debug_time('process_monitored_stop_visits', snapshot_id=snapshot_id):
                        for pmsv in parsed_monitored_stop_visits:
                            siri_vehicle_location = SiriVehicleLocation(
                                siri_snapshot_id=siri_snapshot.id,
                                siri_ride_stop=objects_maker.get_cache_value('siri_ride_stop', pmsv),
                                recorded_at_time=pmsv['recorded_at_time'],
                                lon=pmsv['lon'],
                                lat=pmsv['lat'],
                                bearing=pmsv['bearing'],
                                velocity=pmsv['velocity'],
                                distance_from_journey_start=pmsv['distance_from_journey_start']
                            )
                            with logs.debug_time_stats('vehicle_location_add', stats, log_if_more_then_seconds=1):
                                session.add(siri_vehicle_location)
                            heartbeat()
                    if config.DEBUG:
                        for title in [
                            'siri_ride_get', 'siri_ride_add', 'siri_route_get', 'siri_route_add',
                            'siri_ride_stop_get', 'siri_ride_stop_add',
                            'siri_stop_get', 'siri_stop_add',
                            'vehicle_location_add'
                        ]:
                            total_seconds = stats['{}-total-seconds'.format(title)]
                            total_calls = stats['{}-total-calls'.format(title)]
                            if total_calls > 0:
                                print('avg. {} call seconds: {} ({} / {})'.format(title, total_seconds / total_calls, total_seconds, total_calls))
                    with logs.debug_time('session.commit', snapshot_id=snapshot_id):
                        session.commit()
                except Exception:
                    print("Unexpected exception processing monitored_stop_visit {}".format(monitored_stop_visit))
                    update_siri_snapshot_error(siri_snapshot_session, siri_snapshot, traceback.format_exc(),
                                               num_failed_parse_vehicle_locations,
                                               len(parsed_monitored_stop_visits),
                                               stats)
                    raise
                else:
                    update_siri_snapshot_loaded(siri_snapshot_session, siri_snapshot,
                                                num_failed_parse_vehicle_locations,
                                                len(parsed_monitored_stop_visits),
                                                stats)
        if config.DEBUG:
            for objname in ['siri_stop', 'siri_route', 'siri_ride', 'siri_ride_stop']:
                key = 'num_added_{}s'.format(objname)
                if stats[key] > 0:
                    print('{}: {}'.format(key, stats[key]))
            print('objects_maker size: {} bytes'.format(sys.getsizeof(objects_maker)))


@session_decorator
def process_new_snapshots(session, limit=None, last_snapshots_timedelta=None, now=None, graceful_killer=None):
    if limit:
        limit = int(limit)
    if not last_snapshots_timedelta:
        last_snapshots_timedelta = dict(days=7)
    if not now:
        now = datetime.datetime.now(datetime.timezone.utc)
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
        start_time = datetime.datetime.now(datetime.timezone.utc)
        stats = process_new_snapshots(graceful_killer=graceful_killer)
        if stats['processed'] > 0:
            print('processed {} snapshots (attempted {})'.format(stats['processed'], stats['attempted']))
        elif stats['attempted'] > 0:
            print('attempted {} snapshots'.format(stats['attempted']))
        if graceful_killer.kill_now:
            break
        elapsed_seconds = (datetime.datetime.now(datetime.timezone.utc) - start_time).total_seconds()
        if elapsed_seconds < 60:
            time.sleep(60 - elapsed_seconds)
        else:
            time.sleep(5)
