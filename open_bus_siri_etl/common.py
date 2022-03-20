import datetime


def now():
    return datetime.datetime.now(datetime.timezone.utc)


def is_None(val):
    # due to a problem with airflow dag initialization, in some cases we get
    # the actual string 'None' which we need to handle as None
    return val is None or val == 'None'


def parse_date_str(date, num_days=None):
    """Parses a date string in format %Y-%m-%d with default of today if empty
    if num_days is not None - will use a default of today minus given num_days
    """
    if isinstance(date, datetime.date):
        return date
    elif not date or is_None(date):
        return datetime.date.today() if num_days is None else datetime.date.today() - datetime.timedelta(days=int(num_days))
    else:
        return datetime.datetime.strptime(date, '%Y-%m-%d').date()
