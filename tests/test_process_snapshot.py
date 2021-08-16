import os
import json
import datetime
import tempfile
from copy import deepcopy

import pytz
import pytest

import open_bus_siri_requester.config, open_bus_siri_requester.storage
from open_bus_siri_etl.process_snapshot import process_snapshot, process_new_snapshots
from open_bus_stride_db.model import SiriSnapshot, Route, Stop, RouteStop, Ride, SiriSnapshotEtlStatusEnum
from open_bus_siri_etl.config import OPEN_BUS_SIRI_ETL_ROOTPATH

from . import common


TEST_SNAPSHOT_ID = '2019/05/05/16/00'

TEST_SNAPSHOT_DATA = {
    "Siri": {
        "ServiceDelivery": {
            "ResponseTimestamp": "2019-05-05T16:00:45+03:00",
            "ProducerRef": "Moran",
            "ResponseMessageIdentifier": "ed4c5b10-759c-458a-9f21-99458c5648ec",
            "RequestMessageRef": "1620199645019",
            "Status": "true",
            "StopMonitoringDelivery": [
                {
                    "-version": "2.8",
                    "ResponseTimestamp": "2019-05-05T16:00:45+03:00",
                    "Status": "true",
                    "MonitoredStopVisit": [
                        {
                            "RecordedAtTime": "2019-05-05T16:00:15+03:00",
                            "MonitoredVehicleJourney": {
                                "LineRef": "1",
                                "FramedVehicleJourneyRef": {"DataFrameRef": "2019-05-05", "DatedVehicleJourneyRef": "56644704"},
                                "OperatorRef": "25",
                                "OriginAimedDepartureTime": "2019-05-05T15:45:00+03:00",
                                "VehicleLocation": {"Longitude": "34.749191", "Latitude": "31.874036"},
                                "Bearing": "186", "Velocity": "50", "VehicleRef": "8245384",
                                "MonitoredCall": {
                                    "StopPointRef": "32043", "Order": "13", "DistanceFromStop": "4903"
                                }
                            }
                        },
                        {
                            "RecordedAtTime": "2019-05-05T16:00:00+03:00", "MonitoredVehicleJourney": {"LineRef": "3", "FramedVehicleJourneyRef": {"DataFrameRef": "2019-05-05", "DatedVehicleJourneyRef": "27659116"}, "OperatorRef": "25", "OriginAimedDepartureTime": "2019-05-05T16:05:00+03:00", "VehicleLocation": {"Longitude": "34.731296", "Latitude": "31.890804"}, "Bearing": "246", "Velocity": "0", "VehicleRef": "8245484", "MonitoredCall": {"StopPointRef": "37471", "Order": "1", "DistanceFromStop": "0"}}
                        },
                        {
                            "RecordedAtTime": "2019-05-05T16:00:51+03:00", "MonitoredVehicleJourney": {"LineRef": "5", "FramedVehicleJourneyRef": {"DataFrameRef": "2019-05-05", "DatedVehicleJourneyRef": "49957061"}, "OperatorRef": "25", "OriginAimedDepartureTime": "2019-05-05T15:35:00+03:00", "VehicleLocation": {"Longitude": "34.734901", "Latitude": "31.897741"}, "Bearing": "200", "Velocity": "50", "VehicleRef": "56269001", "MonitoredCall": {"StopPointRef": "32521", "Order": "30", "DistanceFromStop": "8779"}}
                        },
                        # invalid monitored_stop_visit - will fail to parse:
                        {
                            'RecordedAtTime': '2019-05-05T16:00:53+03:00', 'MonitoredVehicleJourney': {'LineRef': '26149', 'FramedVehicleJourneyRef': {'DataFrameRef': '2019-05-05', 'DatedVehicleJourneyRef': '58736023'}, 'OperatorRef': '2', 'OriginAimedDepartureTime': '2019-05-05T15:45:00+03:00', 'Bearing': '0', 'Velocity': '0', 'VehicleRef': '404', 'MonitoredCall': {'StopPointRef': '17016', 'Order': '23', 'DistanceFromStop': '0'}}
                        },
                        {
                            'RecordedAtTime': '2019-05-05T16:00:53+03:00', 'MonitoredVehicleJourney': {'LineRef': '26149', 'FramedVehicleJourneyRef': {'DataFrameRef': '2019-05-05', 'DatedVehicleJourneyRef': '58736023'}, 'OperatorRef': '2', 'OriginAimedDepartureTime': '2019-05-05T15:45:00+03:00', 'Bearing': '0', 'Velocity': '0', 'VehicleRef': '404', 'MonitoredCall': {'StopPointRef': '17016', 'Order': '23', 'DistanceFromStop': '0'}}
                        }
                    ]
                }
            ]
        }
    }
}


def get_test_snapshot_data(date=None, time=None):
    data = deepcopy(TEST_SNAPSHOT_DATA)
    if date:
        data = json.loads(json.dumps(data).replace('2019-05-05', date.strftime('%Y-%m-%d')))
    if time:
        data = json.loads(json.dumps(data).replace('16:00', time.strftime('%H:%M')))
    return data


def assert_first_vehicle_location(vehicle_location, date=None, time=None, with_assert_ids=True):
    if not date:
        date = datetime.date(2019, 5, 5)
    if not time:
        time = datetime.time(16, 0)
    assert vehicle_location.recorded_at_time == datetime.datetime(date.year, date.month, date.day, time.hour-3, time.minute, 15, 0)
    assert vehicle_location.lon, vehicle_location.lat == (34.749191, 31.874036)
    assert vehicle_location.bearing, vehicle_location.velocity == (186, 50)
    assert vehicle_location.distance_from_journey_start == 4903

    route_stop = vehicle_location.route_stop
    assert route_stop.order == 13
    assert route_stop.is_from_gtfs == False

    route = route_stop.route
    assert route.min_date == datetime.date(date.year, date.month, date.day)
    assert route.max_date == datetime.date(date.year, date.month, date.day)
    assert route.line_ref, route.operator_ref == (1, 25)
    assert route.is_from_gtfs == False

    stop = route_stop.stop
    assert stop.min_date == datetime.date(date.year, date.month, date.day)
    assert stop.max_date == datetime.date(date.year, date.month, date.day)
    assert stop.code == 32043
    assert stop.is_from_gtfs == False

    ride = vehicle_location.ride
    assert ride.journey_ref == '2019-05-05-56644704'
    assert ride.scheduled_start_time == datetime.datetime(date.year, date.month, date.day, 12, 45, 00, 0)
    assert ride.vehicle_ref == '8245384'
    assert ride.is_from_gtfs == False

    if with_assert_ids:
        assert {o.id for o in route_stop.vehicle_locations} == {vehicle_location.id}
        assert {o.id for o in route.route_stops} == {route_stop.id}
        assert {o.id for o in route.rides} == {ride.id}
        assert {o.id for o in ride.vehicle_locations} == {vehicle_location.id}


def assert_test_siri_snapshot(session, snapshot_id=TEST_SNAPSHOT_ID, first_vehicle_location_date=None, first_vehicle_location_time=None, first_vehicle_location_with_assert_ids=True):
    siri_snapshot = session.query(SiriSnapshot).filter(SiriSnapshot.snapshot_id == snapshot_id).one()
    assert siri_snapshot.snapshot_id == snapshot_id
    assert siri_snapshot.etl_status == SiriSnapshotEtlStatusEnum.loaded
    assert pytz.UTC.localize(siri_snapshot.etl_start_time) >= datetime.datetime.now(pytz.UTC) - datetime.timedelta(minutes=5)
    assert pytz.UTC.localize(siri_snapshot.etl_end_time) <= datetime.datetime.now(pytz.UTC) + datetime.timedelta(minutes=5)
    assert siri_snapshot.error == ''
    assert siri_snapshot.num_successful_parse_vehicle_locations == 3
    assert siri_snapshot.num_failed_parse_vehicle_locations == 2
    assert len(siri_snapshot.vehicle_locations) == 3
    vehicle_location = siri_snapshot.vehicle_locations[0]
    assert_first_vehicle_location(vehicle_location, first_vehicle_location_date, first_vehicle_location_time, with_assert_ids=first_vehicle_location_with_assert_ids)
    with open(os.path.join(OPEN_BUS_SIRI_ETL_ROOTPATH, 'monitored_stop_visits_parse_failed', snapshot_id, 'jsonlines')) as f:
        monitored_stop_visits_parse_failures = [json.loads(line.strip()) for line in f if line.strip()]
    assert len(monitored_stop_visits_parse_failures) == 2
    assert monitored_stop_visits_parse_failures[0]['MonitoredVehicleJourney']['LineRef'] == '26149'
    assert monitored_stop_visits_parse_failures[1]['MonitoredVehicleJourney']['LineRef'] == '26149'
    return siri_snapshot, vehicle_location


def process_test_siri_snapshot(session, skip_clear=False, force_reload=False):
    if not skip_clear:
        common.clear_siri_data(session, datetime.date(2019, 5, 5))
    process_snapshot(session=session, snapshot_id=TEST_SNAPSHOT_ID, snapshot_data=TEST_SNAPSHOT_DATA, force_reload=force_reload)
    return assert_test_siri_snapshot(session)


def test_process_snapshot_all_new_objects(session):
    process_test_siri_snapshot(session)


def test_process_snapshot_existing_objects(session):
    common.clear_siri_data(session, datetime.date(2019, 5, 5))
    min_max_date_is_from_gtfs = dict(min_date=datetime.date(2019, 5, 5), max_date=datetime.date(2019, 5, 5), is_from_gtfs=False)
    stop = Stop(code=32043, **min_max_date_is_from_gtfs)
    session.add(stop)
    route = Route(line_ref=1, operator_ref=25, **min_max_date_is_from_gtfs)
    session.add(route)
    route_stop = RouteStop(route=route, stop=stop, order=13, is_from_gtfs=False)
    session.add(route_stop)
    ride = Ride(route=route, journey_ref='2019-05-05-56644704', scheduled_start_time=datetime.datetime(2019, 5, 5, 12, 45, 00, 0, tzinfo=pytz.UTC), vehicle_ref='8245384', is_from_gtfs=False)
    session.add(ride)
    session.commit()
    siri_snapshot, vehicle_location = process_test_siri_snapshot(session, skip_clear=True)
    assert vehicle_location.ride.id == ride.id
    assert vehicle_location.route_stop.id == route_stop.id


def test_existing_siri_snapshot_error(session):
    first_siri_snapshot, vehicle_location = process_test_siri_snapshot(session)
    first_siri_snapshot.etl_status = SiriSnapshotEtlStatusEnum.error
    session.commit()
    second_siri_snapshot, vehicle_location = process_test_siri_snapshot(session, skip_clear=True)
    assert first_siri_snapshot.id == second_siri_snapshot.id


def test_existing_siri_snapshot_loading(session):
    first_siri_snapshot, vehicle_location = process_test_siri_snapshot(session)
    first_siri_snapshot.etl_status = SiriSnapshotEtlStatusEnum.loading
    session.commit()
    with pytest.raises(Exception, match='snapshot is already in loading status'):
        process_test_siri_snapshot(session, skip_clear=True)
    second_siri_snapshot, vehicle_location = process_test_siri_snapshot(session, skip_clear=True, force_reload=True)
    assert first_siri_snapshot.id == second_siri_snapshot.id


def test_process_new_snapshots(session):
    common.clear_siri_data(session, datetime.date(2019, 5, 4))
    for siri_snapshot in session.query(SiriSnapshot).filter(SiriSnapshot.snapshot_id.startswith('2019/')):
        session.delete(siri_snapshot)
    session.commit()
    with tempfile.TemporaryDirectory() as tmpdir:
        open_bus_siri_requester.config.OPEN_BUS_SIRI_STORAGE_ROOTPATH = tmpdir
        # no snapshots available in storage from last snapshots
        stats = process_new_snapshots(last_snapshots_timedelta=dict(minutes=10))
        assert (stats['processed'], stats['attempted']) == (0, 11)
        # 1 snapshot available in storage from last snapshots
        open_bus_siri_requester.storage.store(TEST_SNAPSHOT_DATA, datetime.datetime(2019, 5, 5, 16, 0, tzinfo=pytz.UTC))
        stats = process_new_snapshots(last_snapshots_timedelta=dict(minutes=10), now=datetime.datetime(2019, 5, 5, 16, 5, tzinfo=pytz.UTC))
        assert (stats['processed'], stats['attempted']) == (1, 11)
        assert_test_siri_snapshot(session)
        # last snapshot is in DB, so next snapshot in storage will be processed (previous stored snapshot won't be processed)
        open_bus_siri_requester.storage.store(get_test_snapshot_data(time=datetime.time(16, 6)), datetime.datetime(2019, 5, 5, 16, 6, tzinfo=pytz.UTC))
        stats = process_new_snapshots(last_snapshots_timedelta=dict(minutes=10), now=datetime.datetime(2019, 5, 5, 16, 7, tzinfo=pytz.UTC))
        assert (stats['processed'], stats['attempted']) == (1, 7)
        assert_test_siri_snapshot(session, snapshot_id='2019/05/05/16/06', first_vehicle_location_time=datetime.time(16, 6), first_vehicle_location_with_assert_ids=False)
