import json
import pytz
import datetime
from contextlib import contextmanager

from . import config


@contextmanager
def debug_time(title, msg=None, **kwargs):
    if config.DEBUG:
        start_time = datetime.datetime.now(pytz.timezone('Israel'))
        print('{} start {}{}{}'.format(
            start_time.strftime('%Y-%m-%d %H:%M:%S'),
            title,
            ': {}'.format(msg) if msg else '',
            ' ({})'.format(json.dumps(kwargs)) if kwargs else ''
        ))
        yield
        end_time = datetime.datetime.now(pytz.timezone('Israel'))
        duration_seconds = (end_time - start_time).total_seconds()
        print('{} ({}s) end {}'.format(end_time.strftime('%Y-%m-%d %H:%M:%S'), duration_seconds, title))
    else:
        yield


@contextmanager
def debug_time_stats(title, stats, log_if_more_then_seconds=None, **kwargs):
    start_time = datetime.datetime.now(pytz.timezone('Israel'))
    yield
    end_time = datetime.datetime.now(pytz.timezone('Israel'))
    duration_seconds = (end_time - start_time).total_seconds()
    if log_if_more_then_seconds and duration_seconds > log_if_more_then_seconds:
        print('{} {}: took {} seconds{}'.format(
            end_time.strftime('%Y-%m-%d %H:%M:%S'),
            title, duration_seconds,
            ' ({})'.format(json.dumps(kwargs)) if kwargs else ''
        ))
    stats['{}-total-seconds'.format(title)] += duration_seconds
    stats['{}-total-calls'.format(title)] += 1
