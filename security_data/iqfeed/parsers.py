import logging
from datetime import datetime
import pandas as pd

log = logging.getLogger(__name__)

DATE_INPUT_FORMAT = '%Y-%m-%d %H:%M:%S'


def parse_minute(data):
    """
    :param data: Iqfeed data
    :return: Array JSON objects representing minute history data
    """
    result = []

    if len(data) is 0:
        return result

    for line in data.split('\n'):
        try:
            (datetime_str, high, low, open_, close, volume, _, _) = line.split(',')
            line_data = {
                'datetime': datetime.strptime(datetime_str, DATE_INPUT_FORMAT),
                'high': float(high),
                'low': float(low),
                'open': float(open_),
                'close': float(close),
            }
            result.append(line_data)
        except ValueError:
            if 'NO_DATA' in line:
                log.info('No data received.')
            else:
                log.info(data)
            return []

    return result


def parse_day(data):
    return parse_minute(data)


def parse_tick():
    pass
