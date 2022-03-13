import os
import datetime
from collections import defaultdict

import dataflows as DF

from open_bus_stride_db.db import get_session
from open_bus_stride_db import model

from . import process_snapshot, config


def validate_snapshot(session, snapshot_data, snapshot_id, stats):
    num_db_items = 0
    db_data = {}
    for siri_vehicle_location, siri_ride_stop, siri_ride, siri_stop in (
        session.query(model.SiriVehicleLocation)
        .select_from(model.SiriVehicleLocation)
        .add_entity(model.SiriRideStop)
        .add_entity(model.SiriRide)
        .add_entity(model.SiriStop)
        .join(model.SiriSnapshot, model.SiriSnapshot.id==model.SiriVehicleLocation.siri_snapshot_id)
        .join(model.SiriRideStop, model.SiriRideStop.id==model.SiriVehicleLocation.siri_ride_stop_id)
        .join(model.SiriRide, model.SiriRide.id==model.SiriRideStop.siri_ride_id)
        .join(model.SiriStop, model.SiriStop.id==model.SiriRideStop.siri_stop_id)
        .filter(model.SiriSnapshot.snapshot_id==snapshot_id)
    ):
        key = (
            siri_vehicle_location.recorded_at_time.strftime('%Y%m%d%H%M%S'),
            str(siri_vehicle_location.lon),
            str(siri_vehicle_location.lat),
            str(siri_vehicle_location.bearing),
            str(siri_vehicle_location.velocity),
            str(siri_vehicle_location.distance_from_journey_start)
        )
        db_data[key] = (siri_vehicle_location, siri_ride_stop, siri_ride, siri_stop)
        num_db_items += 1
    if num_db_items != len(db_data):
        stats['invalid snapshots: num_db_items mismatch'] += 1
        print("Invalid snapshot: num_db_items mismatch")
        yield {
            'snapshot_id': snapshot_id,
            'recorded_at_time': '',
            'lon': '',
            'lat': '',
            'bearing': '',
            'velocity': '',
            'distance_from_journey_start': '',
            'field': '',
            'expected': 'matching num_db_items',
            'actual': 'mismatch'
        }
    else:
        pmsv_data = {}
        num_pmsv_items = 0
        for monitored_stop_visit in process_snapshot.iterate_monitored_stop_visits(snapshot_data):
            pmsv = process_snapshot.parse_monitored_stop_visit(monitored_stop_visit)
            key = (
                pmsv['recorded_at_time'].astimezone(datetime.timezone.utc).strftime('%Y%m%d%H%M%S'),
                str(pmsv['lon']),
                str(pmsv['lat']),
                str(pmsv['bearing']),
                str(pmsv['velocity']),
                str(pmsv['distance_from_journey_start']),
            )
            pmsv_data[key] = pmsv
            num_pmsv_items += 1
        if num_pmsv_items != len(pmsv_data):
            stats['invalid snapshots: num_pmsv_items mismatch'] += 1
            print("Invalid snapshot: num_pmsv_items mismatch")
            yield {
                'snapshot_id': snapshot_id,
                'recorded_at_time': '',
                'lon': '',
                'lat': '',
                'bearing': '',
                'velocity': '',
                'distance_from_journey_start': '',
                'field': '',
                'expected': 'matching num_pmsv_items',
                'actual': 'mismatch'
            }
        elif sorted(db_data.keys()) != sorted(pmsv_data.keys()):
            stats['invalid snapshots: keys mismatch'] += 1
            print("Invalid snapshot: keys mismatch")
            yield {
                'snapshot_id': snapshot_id,
                'recorded_at_time': '',
                'lon': '',
                'lat': '',
                'bearing': '',
                'velocity': '',
                'distance_from_journey_start': '',
                'field': '',
                'expected': 'matching db_data and pmsv_data keys',
                'actual': 'mismatch'
            }
        else:
            num_errors = 0
            for key, pmsv in pmsv_data.items():
                siri_vehicle_location, siri_ride_stop, siri_ride, siri_stop = db_data[key]
                assert siri_ride_stop.order == int(pmsv['order'])
                row = {
                    'snapshot_id': snapshot_id,
                    'recorded_at_time': key[0],
                    'lon': key[1],
                    'lat': key[2],
                    'bearing': key[3],
                    'velocity': key[4],
                    'distance_from_journey_start': key[5]
                }
                if siri_ride.journey_ref != pmsv['journey_ref']:
                    stats['journey_ref validation errors'] += 1
                    num_errors += 1
                    yield {
                        **row,
                        'field': 'siri_ride.journey_ref',
                        'expected': pmsv['journey_ref'],
                        'actual': siri_ride.journey_ref
                    }
                if siri_ride.vehicle_ref != pmsv['vehicle_ref']:
                    stats['vehicle_ref validation errors'] += 1
                    num_errors += 1
                    yield {
                        **row,
                        'field': 'siri_ride.vehicle_ref',
                        'expected': pmsv['vehicle_ref'],
                        'actual': siri_ride.vehicle_ref
                    }
                if siri_ride.scheduled_start_time != pmsv['scheduled_start_time'].astimezone(datetime.timezone.utc):
                    stats['scheduled_start_time validation errors'] += 1
                    num_errors += 1
                    yield {
                        **row,
                        'field': 'siri_ride.scheduled_start_time',
                        'expected': str(pmsv['scheduled_start_time'].astimezone(datetime.timezone.utc)),
                        'actual': str(siri_ride.scheduled_start_time)
                    }
                if siri_stop.code != int(pmsv['stop_point_ref']):
                    stats['stop code validation errors'] += 1
                    num_errors += 1
                    yield {
                        **row,
                        'field': 'siri_ride.scheduled_start_time',
                        'expected': str(siri_stop.code),
                        'actual': str(pmsv['stop_point_ref'])
                    }
            if num_errors == 0:
                yield {
                    'snapshot_id': snapshot_id,
                    'recorded_at_time': '',
                    'lon': '',
                    'lat': '',
                    'bearing': '',
                    'velocity': '',
                    'distance_from_journey_start': '',
                    'field': '',
                    'expected': 'no errors',
                    'actual': 'no errors'
                }


def validate_snapshots(dt_from, dt_to):
    stats = defaultdict(int)
    dt = dt_from
    with get_session() as session:
        while dt <= dt_to:
            snapshot_id = dt.strftime('%Y/%m/%d/%H/%M')
            snapshot_data = process_snapshot.download_snapshot_data(snapshot_id)
            if snapshot_data:
                yield from validate_snapshot(session, snapshot_data, snapshot_id, stats)
                stats['processed snapshots'] += 1
            else:
                stats['missing snapshots'] += 1
            dt = dt + datetime.timedelta(minutes=1)
    print(dict(stats))


def main(snapshot_id_from, snapshot_id_to):
    output_path = os.path.join(config.OPEN_BUS_SIRI_ETL_ROOTPATH, 'snapshots_validation', snapshot_id_from.replace('/', '_') + '-' + snapshot_id_to.replace('/', '_'))
    print("Generating validation report to {}".format(output_path))
    dt_from = datetime.datetime.strptime(snapshot_id_from, '%Y/%m/%d/%H/%M')
    dt_to = datetime.datetime.strptime(snapshot_id_to, '%Y/%m/%d/%H/%M')
    assert dt_from <= dt_to
    DF.Flow(
        validate_snapshots(dt_from, dt_to),
        *[DF.set_type(name, type='string') for name in [
            'snapshot_id', 'recorded_at_time', 'lon', 'lat',
            'bearing', 'velocity', 'distance_from_journey_start',
            'field', 'expected', 'actual'
        ]],
        DF.dump_to_path(output_path)
    ).process()
