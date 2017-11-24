import datetime
import dateutil.relativedelta
import logging
import pandas as pd
from security_data.iqfeed import iqfeed
from security_data.db import DataService
from security_data.indicators import Indicators

start_date = datetime.date(year=2017, month=11, day=1)
logger = logging.getLogger(__name__)


class SecurityService(object):
    def __init__(self):
        self.ds = DataService()

    def update_daily_data(self, tickers):
        min_date = self.ds.get_last_available_date(intraday=False)
        if min_date is not None:
            self.ds.remove_data(min_date, intraday=False)
        lookback_date = self.get_lookback_date(min_date, start_date)

        for ticker in tickers:
            logger.info('Loading {0}'.format(ticker))
            price_data = iqfeed.get_day_bar_data(ticker, lookback_date, datetime.datetime.now())
            df = pd.DataFrame.from_records(price_data)
            if df.empty:
                logger.info('Unable to load any data. {0}'.format(ticker))
                continue
            df['datetime'] = df['datetime'].dt.date
            data_df = Indicators(df.set_index('datetime').sort_index())\
                .linear_regression(10, 'rl10')\
                .linear_regression(30, 'rl30')\
                .df
            data_df['ticker'] = ticker
            if min_date is not None:
                data_df = data_df[data_df.index > min_date]
            self.ds.insert_daily_data(data_df)

    def update_intraday_data(self, tickers):
        min_date = self.ds.get_last_available_date(intraday=True)
        if min_date is not None:
            self.ds.remove_data(min_date, intraday=True)
        lookback_date = self.get_lookback_date(min_date, start_date)

        for ticker in tickers:
            price_data = iqfeed.get_minute_bar_data(ticker, lookback_date, datetime.datetime.now())
            data_df = Indicators(pd.DataFrame.from_records(price_data, index='datetime').sort_index()) \
                .linear_regression(10, 'rl10') \
                .linear_regression(30, 'rl30') \
                .df
            data_df['ticker'] = ticker
            if min_date is not None:
                data_df = data_df[data_df.index > min_date]
            self.ds.insert_intraday_data(data_df)

    def get_lookback_date(self, min_date, sdate):
        if min_date is None:
            return sdate
        return min_date - dateutil.relativedelta.relativedelta(months=2)
