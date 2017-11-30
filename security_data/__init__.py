import datetime
import dateutil.relativedelta
import logging
import pandas as pd
from dateutil.relativedelta import relativedelta
from security_data.iqfeed import iqfeed
from security_data.db import DataService
from security_data.indicators import Indicators

logger = logging.getLogger(__name__)


class SecurityService(object):
    def __init__(self):
        self.ds = DataService()

    def update_daily_data(self, tickers, start_date):
        lookback_date = start_date + relativedelta(years=-5)

        result_list = []
        for ticker in tickers:
            logger.info('Loading daily {0}'.format(ticker))
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
            data_df = data_df[data_df.index >= start_date.date()]
            result_list.append(data_df.reset_index())
        self.ds.insert_daily_data(pd.concat(result_list, axis=0, ignore_index=True))

    def update_intraday_data(self, tickers, start_date):
        lookback_date = start_date + relativedelta(days=-3)

        result_list = []
        for ticker in tickers:
            logger.info('Loading intraday {0}'.format(ticker))
            price_data = iqfeed.get_minute_bar_data(ticker, lookback_date, datetime.datetime.now())
            df = pd.DataFrame.from_records(price_data)
            if df.empty:
                logger.info('Unable to load any data. {0}'.format(ticker))
                continue
            data_df = Indicators(df.set_index('datetime').sort_index())\
                .linear_regression(10, 'rl10') \
                .linear_regression(30, 'rl30') \
                .df
            data_df['ticker'] = ticker
            data_df = data_df[data_df.index > start_date]
            result_list.append(data_df.reset_index())
        self.ds.insert_intraday_data(pd.concat(result_list, axis=0, ignore_index=True))
