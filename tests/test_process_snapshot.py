import os
import json
import datetime
import tempfile

import pytz
import pytest

import open_bus_siri_requester.config
from open_bus_siri_etl.process_snapshot import process_snapshot, process_new_snapshots
from open_bus_stride_db.model import SiriSnapshot, Route, Stop, RouteStop, Ride, SiriSnapshotEtlStatusEnum

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
                        }
                    ]
                }
            ]
        }
    }
}


def get_test_snapshot_data(date):
    return json.loads(json.dumps(TEST_SNAPSHOT_DATA).replace('2019-05-05', date.strftime('%Y-%m-%d')))


def assert_first_vehicle_location(vehicle_location):
    assert vehicle_location.recorded_at_time == datetime.datetime(2019, 5, 5, 13, 0, 15, 0)
    assert vehicle_location.lon, vehicle_location.lat == (34.749191, 31.874036)
    assert vehicle_location.bearing, vehicle_location.velocity == (186, 50)
    assert vehicle_location.distance_from_journey_start == 4903

    route_stop = vehicle_location.route_stop
    assert route_stop.order == 13
    assert route_stop.is_from_gtfs == False

    route = route_stop.route
    assert route.min_date == datetime.date(2019, 5, 5)
    assert route.max_date == datetime.date(2019, 5, 5)
    assert route.line_ref, route.operator_ref == (1, 25)
    assert route.is_from_gtfs == False

    stop = route_stop.stop
    assert stop.min_date == datetime.date(2019, 5, 5)
    assert stop.max_date == datetime.date(2019, 5, 5)
    assert stop.code == 32043
    assert stop.is_from_gtfs == False

    ride = vehicle_location.ride
    assert ride.journey_ref == '2019-05-05-56644704'
    assert ride.scheduled_start_time == datetime.datetime(2019, 5, 5, 12, 45, 00, 0)
    assert ride.vehicle_ref == '8245384'
    assert ride.is_from_gtfs == False

    assert {o.id for o in route_stop.vehicle_locations} == {vehicle_location.id}
    assert {o.id for o in route.route_stops} == {route_stop.id}
    assert {o.id for o in route.rides} == {ride.id}
    assert {o.id for o in ride.vehicle_locations} == {vehicle_location.id}


def process_test_siri_snapshot(session, skip_clear=False, force_reload=False):
    if not skip_clear:
        common.clear_siri_data(session, datetime.date(2019, 5, 5))
    process_snapshot(TEST_SNAPSHOT_ID, snapshot_data=TEST_SNAPSHOT_DATA, force_reload=force_reload)
    siri_snapshot = session.query(SiriSnapshot).filter(SiriSnapshot.snapshot_id == TEST_SNAPSHOT_ID).one()
    assert siri_snapshot.snapshot_id == TEST_SNAPSHOT_ID
    assert siri_snapshot.etl_status == SiriSnapshotEtlStatusEnum.loaded
    assert pytz.UTC.localize(siri_snapshot.etl_start_time) >= datetime.datetime.now(pytz.UTC) - datetime.timedelta(minutes=5)
    assert pytz.UTC.localize(siri_snapshot.etl_end_time) <= datetime.datetime.now(pytz.UTC) + datetime.timedelta(minutes=5)
    assert siri_snapshot.error == ''
    assert siri_snapshot.num_successful_parse_vehicle_locations == 3
    assert siri_snapshot.num_failed_parse_vehicle_locations == 0
    assert len(siri_snapshot.vehicle_locations) == 3
    vehicle_location = siri_snapshot.vehicle_locations[0]
    assert_first_vehicle_location(vehicle_location)
    return siri_snapshot, vehicle_location


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
    ride = Ride(route=route, journey_ref='2019-05-05-56644704', scheduled_start_time=datetime.datetime(2019, 5, 5, 12, 45, 00, 0), vehicle_ref='8245384', is_from_gtfs=False)
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
    common.clear_siri_data(session, datetime.date(2019, 5, 5))
    common.clear_siri_data(session, datetime.date(2019, 5, 4))
    for siri_snapshot in session.query(SiriSnapshot).filter(SiriSnapshot.snapshot_id.startswith('2019/')):
        session.delete(siri_snapshot)
    session.commit()
    with tempfile.TemporaryDirectory() as tmpdir:
        os.makedirs(os.path.join(tmpdir, '2019', '04', '01', '04', '03'))
        os.makedirs(os.path.join(tmpdir, '2019', '05', '01', '04', '03'))
        open_bus_siri_requester.config.OPEN_BUS_SIRI_STORAGE_ROOTPATH = tmpdir
        process_new_snapshots(session)
