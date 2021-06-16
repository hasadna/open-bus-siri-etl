import datetime

import sqlalchemy

from open_bus_stride_db.model import SiriSnapshot, VehicleLocation, Route, Stop, RouteStop, Ride


def clear_siri_data(session, date):
    min_datetime = datetime.datetime(date.year, date.month, date.day) - datetime.timedelta(days=1)
    max_datetime = min_datetime + datetime.timedelta(days=2)
    route_ids, stop_ids = set(), set()
    for route in session.query(Route).filter(sqlalchemy.or_(Route.min_date <= date, Route.max_date >= date)):
        route_ids.add(route.id)
        session.delete(route)
    for stop in session.query(Stop).filter(sqlalchemy.or_(Stop.min_date <= date, Stop.max_date >= date)):
        stop_ids.add(stop.id)
        session.delete(stop)
    for vehicle_location in session.query(VehicleLocation).filter(VehicleLocation.recorded_at_time >= min_datetime, VehicleLocation.recorded_at_time <= max_datetime):
        session.delete(vehicle_location)
    for route_stop in session.query(RouteStop).filter(sqlalchemy.or_(
        RouteStop.stop_id.is_(None), RouteStop.route_id.is_(None),
        RouteStop.stop_id.in_(stop_ids), RouteStop.route_id.in_(route_ids)
    )):
        session.delete(route_stop)
    for ride in session.query(Ride).filter(sqlalchemy.or_(
            *(Ride.journey_ref.startswith(dt.strftime('%Y-%m-%d-')) for dt in (date - datetime.timedelta(days=2), date - datetime.timedelta(days=1), date, date + datetime.timedelta(days=1), date + datetime.timedelta(days=2)))
    )):
        session.delete(ride)
    for siri_snapshot in session.query(SiriSnapshot).filter(sqlalchemy.or_(
            *(SiriSnapshot.snapshot_id.startswith(dt.strftime('%Y/%m/%d/')) for dt in (date - datetime.timedelta(days=2), date - datetime.timedelta(days=1), date, date + datetime.timedelta(days=1), date + datetime.timedelta(days=2)))
    )):
        session.delete(siri_snapshot)
    session.commit()
