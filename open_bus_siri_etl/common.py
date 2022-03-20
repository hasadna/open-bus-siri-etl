import datetime


def now():
    return datetime.datetime.now(datetime.timezone.utc)


def parse_date_str(date):
    """Parses a date string in format %Y-%m-%d with default of today if empty"""
    if isinstance(date, datetime.date):
        return date
    if not date:
        return datetime.date.today()
    return datetime.datetime.strptime(date, '%Y-%m-%d').date()
