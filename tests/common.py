import datetime

import sqlalchemy

from open_bus_stride_db.model import SiriSnapshot, VehicleLocation, Route, Stop, RouteStop, Ride


def clear_siri_data(session, date):
    min_datetime = datetime.datetime(date.year, date.month, date.day)
    max_datetime = min_datetime + datetime.timedelta(days=1)
    route_ids, stop_ids = set(), set()
    for route in session.query(Route).filter(sqlalchemy.or_(Route.min_date <= date, Route.max_date >= date)):
        route_ids.add(route.id)
        session.delete(route)
    for stop in session.query(Stop).filter(sqlalchemy.or_(Stop.min_date <= date, Stop.max_date >= date)):
        stop_ids.add(stop.id)
        session.delete(stop)
    session.query(VehicleLocation).filter(VehicleLocation.recorded_at_time >= min_datetime, VehicleLocation.recorded_at_time <= max_datetime).delete()
    session.query(RouteStop).filter(sqlalchemy.or_(
        RouteStop.stop_id.is_(None), RouteStop.route_id.is_(None),
        RouteStop.stop_id.in_(stop_ids), RouteStop.route_id.in_(route_ids)
    )).delete()
    for ride in session.query(Ride).filter(Ride.journey_ref.startswith(date.strftime('%Y-%m-%d-'))):
        session.delete(ride)
    for siri_snapshot in session.query(SiriSnapshot).filter(SiriSnapshot.snapshot_id.startswith(date.strftime('%Y/%m/%d/'))):
        session.delete(siri_snapshot)
    session.commit()
