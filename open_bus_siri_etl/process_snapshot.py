import time
import datetime
from collections import defaultdict

import pytz
import sqlalchemy

from open_bus_stride_db.db import session_decorator
from open_bus_stride_db.model import (
    SiriSnapshot, SiriSnapshotEtlStatusEnum, VehicleLocation, Ride, RouteStop, Route, Stop
)
import open_bus_siri_requester.storage
from .graceful_killer import GracefulKiller
from . import config


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
        raise Exception("snapshot is already in loading status")
    if not is_new_snapshot:
        siri_snapshot.etl_status = SiriSnapshotEtlStatusEnum.loading
        session.query(VehicleLocation).filter(VehicleLocation.siri_snapshot==siri_snapshot).delete()
        session.commit()
    return siri_snapshot


def get_or_create_route(session, recorded_at_time, line_ref, operator_ref):
    route = session.query(Route).filter(
        Route.min_date <= recorded_at_time.date(),
        recorded_at_time.date() <= Route.max_date,
        Route.line_ref == line_ref,
        Route.operator_ref == operator_ref,
        Route.is_from_gtfs == False
    ).one_or_none()
    if not route:
        route = Route(
            min_date=recorded_at_time.date(),
            max_date=recorded_at_time.date(),
            line_ref=line_ref,
            operator_ref=operator_ref,
            is_from_gtfs=False
        )
        session.add(route)
    return route


def get_or_create_ride(session, journey_ref, route, scheduled_start_time, vehicle_ref):
    ride = session.query(Ride).filter(
        Ride.route_id == route.id,
        Ride.journey_ref == journey_ref,
        Ride.scheduled_start_time == scheduled_start_time,
        Ride.vehicle_ref == vehicle_ref,
        Ride.is_from_gtfs == False
    ).one_or_none()
    if not ride:
        ride = Ride(
            route=route,
            journey_ref=journey_ref,
            scheduled_start_time=scheduled_start_time,
            vehicle_ref=vehicle_ref,
            is_from_gtfs=False
        )
        session.add(ride)
    return ride


def get_or_create_route_stop(session, stop, route, order):
    route_stop = session.query(RouteStop).filter(
        RouteStop.stop_id == stop.id,
        RouteStop.route_id == route.id,
        RouteStop.order == order,
        RouteStop.is_from_gtfs == False
    ).one_or_none()
    if not route_stop:
        route_stop = RouteStop(
            stop=stop,
            route=route,
            order=order,
            is_from_gtfs=False
        )
        session.add(route_stop)
    return route_stop


def get_or_create_stop(session, recorded_at_time, stop_point_ref):
    stop = session.query(Stop).filter(
        Stop.min_date <= recorded_at_time.date(),
        recorded_at_time.date() <= Stop.max_date,
        Stop.code == stop_point_ref,
        Stop.is_from_gtfs == False
    ).one_or_none()
    if not stop:
        stop = Stop(
            min_date=recorded_at_time.date(),
            max_date=recorded_at_time.date(),
            code=stop_point_ref,
            is_from_gtfs=False
        )
        session.add(stop)
    return stop


def parse_timestr(timestr):
    return datetime.datetime.strptime(timestr.replace(':', ''), '%Y-%m-%dT%H%M%S%z').astimezone(pytz.UTC)


def parse_monitored_stop_visit(session, monitored_stop_visit):
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
        print("Failed to parse monitored stop visit: {}".format(monitored_stop_visit))
        return None
    route = get_or_create_route(session, recorded_at_time, line_ref, operator_ref)
    ride = get_or_create_ride(session, journey_ref, route, scheduled_start_time, vehicle_ref)
    stop = get_or_create_stop(session, recorded_at_time, stop_point_ref)
    route_stop = get_or_create_route_stop(session, stop, route, order)
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


@session_decorator
def process_snapshot(session, snapshot_id, force_reload=False, snapshot_data=None):
    if snapshot_data is None:
        snapshot_data = open_bus_siri_requester.storage.read(snapshot_id)
    error, monitored_stop_visit, siri_snapshot, is_new_snapshot = None, None, None, None
    num_failed_parse_vehicle_locations, num_successful_parse_vehicle_locations = 0, 0
    try:
        for monitored_stop_visit in iterate_monitored_stop_visits(snapshot_data):
            if siri_snapshot is None:
                siri_snapshot = get_or_create_siri_snapshot(session, snapshot_id, force_reload)
            parsed_monitored_stop_visit = parse_monitored_stop_visit(session, monitored_stop_visit)
            if parsed_monitored_stop_visit:
                num_successful_parse_vehicle_locations += 1
                vehicle_location = VehicleLocation(
                    siri_snapshot=siri_snapshot,
                    **parsed_monitored_stop_visit
                )
                session.add(vehicle_location)
            else:
                num_failed_parse_vehicle_locations += 1
    except Exception as e:
        print("Unexpected exception processing monitored_stop_visit {}".format(monitored_stop_visit))
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
        session.commit()


@session_decorator
def process_new_snapshots(session, limit=None, last_snapshots_timedelta=None, now=None):
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
        if config.DEBUG:
            print('last loaded snapshot_id: {}'.format(last_loaded_snapshot.snapshot_id))
        cur_datetime = datetime.datetime.strptime(last_loaded_snapshot.snapshot_id + 'z+0000', '%Y/%m/%d/%H/%Mz%z') + datetime.timedelta(minutes=1)
    else:
        if config.DEBUG:
            print('no last loaded snapshot, getting snapshots from last {}'.format(last_snapshots_timedelta))
        cur_datetime = now - datetime.timedelta(**last_snapshots_timedelta)
    stats = defaultdict(int)
    while cur_datetime <= now and (not limit or stats['processed'] <= limit):
        stats['attempted'] += 1
        snapshot_id = cur_datetime.strftime('%Y/%m/%d/%H/%M')
        try:
            snapshot_data = open_bus_siri_requester.storage.read(snapshot_id)
        except:
            snapshot_data = None
        if snapshot_data:
            process_snapshot(snapshot_id=snapshot_id, snapshot_data=snapshot_data)
            stats['processed'] += 1
        cur_datetime = cur_datetime + datetime.timedelta(minutes=1)
    if config.DEBUG:
        print('processed {} snapshots out of {} attempted'.format(stats['processed'], stats['attempted']))
    return stats


def start_process_new_snapshots_daemon():
    graceful_killer = GracefulKiller()
    while not graceful_killer.kill_now:
        start_time = datetime.datetime.now(pytz.UTC)
        stats = process_new_snapshots()
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
