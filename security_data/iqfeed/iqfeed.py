# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import socket
from collections import namedtuple
from enum import Enum
import logging
from security_data.iqfeed import parsers as parser_table

from .tools import retry

log = logging.getLogger(__name__)

Bar = namedtuple('IQFeedBar', ['datetime', 'open', 'high', 'low', 'close', 'volume'])
TickBar = namedtuple('IQFeedTickBar', ['datetime', 'last', 'last_size', 'volume', 'bid', 'ask', 'ticket_id'])


class DataType(Enum):
    DAY = 0
    MINUTE = 1
    TICK = 2

PARSERS = {
    DataType.DAY: parser_table.parse_day,
    DataType.MINUTE: parser_table.parse_minute,
    DataType.TICK: parser_table.parse_tick,
}

BEGIN_TIME_FILTER = '093000'
END_TIME_FILTER = '160000'
BARS_PER_MINUTE = 60

HOST = 'localhost'
PORT = 9100
TIMEOUT = 10.0

DATETIME_FORMAT = "%Y%m%d %H%M%S"
DATE_FORMAT = "%Y%m%d"


def get_minute_bar_data(instrument, start_time, end_time):
    return get_data(instrument, start_time, end_time, DataType.MINUTE)


def get_day_bar_data(instrument, start_time, end_time):
    return get_data(instrument, start_time, end_time, DataType.DAY)


@retry(5, delay=2)
def get_data(instrument, start_date, end_date, data_type=DataType.MINUTE):
    if data_type is DataType.TICK:
        raise NotImplementedError('Not implemented')
    try:
        socket_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_.connect((HOST, PORT))

        socket_.settimeout(TIMEOUT)
        socket_.sendall(_get_request_string(data_type, instrument, start_date, end_date))

        data = _read_historical_data_socket(socket_)
    finally:
        socket_.close()

    return PARSERS[data_type](data)


def _read_historical_data_socket(sock, receive_buffer=4096):
    """
    Read the information from the socket, in a buffered
    fashion, receiving only 4096 bytes at a time.

    Parameters:
    sock - The socket object
    recv_buffer - Amount in bytes to receive per read
    """
    data_buffer = ""
    while True:
        data = sock.recv(receive_buffer).decode()
        data_buffer += data

        # Check if the end message string arrives
        if "!ENDMSG!" in data_buffer:
            break

    # Remove the end message string
    data_buffer = data_buffer[:-12]
    return data_buffer


def _get_request_string(data_type, instrument, start_date, end_date):
    start_date_str = start_date.strftime(DATETIME_FORMAT)
    end_date_str = end_date.strftime(DATETIME_FORMAT)

    result = ""

    if data_type is DataType.DAY:
        start_date_str = start_date.strftime(DATE_FORMAT)
        end_date_str = end_date.strftime(DATE_FORMAT)
        result = 'HDT,{0},{1},{2},,,,1\n'.format(instrument, start_date_str, end_date_str)
    elif data_type is DataType.MINUTE:

        result = 'HIT,{0},{1},{2},{3},,{4},{5},1\n'\
            .format(instrument, BARS_PER_MINUTE, start_date_str, end_date_str, BEGIN_TIME_FILTER, END_TIME_FILTER)
    elif data_type is DataType.TICK:
        result = 'HTT,{0},{1},{2},,{3},{4},1,,\n'\
            .format(instrument, start_date_str, end_date_str, BEGIN_TIME_FILTER, END_TIME_FILTER)

    return str.encode(result)
