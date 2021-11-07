from open_bus_stride_db.db import Session
from open_bus_stride_db.model import SiriSnapshot, SiriVehicleLocation, SiriRoute, SiriStop, SiriRideStop, SiriRide


def clear_siri_data(session: Session):
    session.execute('truncate {} cascade;'.format(SiriStop.__tablename__))
    session.execute('truncate {} cascade;'.format(SiriRide.__tablename__))
    session.execute('truncate {} cascade;'.format(SiriRoute.__tablename__))
    session.execute('truncate {} cascade;'.format(SiriRideStop.__tablename__))
    session.execute('truncate {} cascade;'.format(SiriVehicleLocation.__tablename__))
    session.execute('truncate {} cascade;'.format(SiriSnapshot.__tablename__))
    session.commit()
