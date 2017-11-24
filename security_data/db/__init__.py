import sqlalchemy as sa
import os.path
from sqlalchemy.dialects.mssql import SMALLDATETIME, DATE, VARCHAR, DECIMAL, MONEY
import urllib.parse
import pandas as pd

INTRADAY_QUERY_BASE = r'SELECT price_time as [datetime], ticker, price_open as [open], price_high as [high], price_low as [low], price_close as [close], rl10, rl30 FROM dbo.intraday_price'
DAILY_QUERY_BASE = r'SELECT price_time as [datetime], ticker, price_open as [open], price_high as [high], price_low as [low], price_close as [close], rl10, rl30 FROM dbo.daily_price'


class DataService(object):
    def __init__(self):
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 13 for SQL Server};SERVER=localhost;DATABASE=RLCO;UID=root;PWD=root")
        self.engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=' + params)

    def get_intraday_data_by_start_date(self, start_date):
        sql = INTRADAY_QUERY_BASE + 'WHERE price_time > {}'.format(start_date)
        with self.engine.connect() as con:
            return pd.read_sql(sql, con)

    def get_daily_data_by_start_date(self, start_date):
        sql = DAILY_QUERY_BASE + 'WHERE price_time > {}'.format(start_date)
        with self.engine.connect() as con:
            return pd.read_sql(sql, con)

    def get_last_available_date(self, intraday=True):
        table_name = 'intraday_price' if intraday else 'daily_price'
        sql = 'SELECT ticker, MAX(price_time) AS max_date FROM dbo.{0} GROUP BY ticker'.format(table_name)
        with self.engine.connect() as con:
            df = pd.read_sql(sql, con)
            if df.empty:
                return None
            return df['max_date'].min()

    def remove_data(self, date, intraday=True):
        table_name = 'intraday_price' if intraday else 'daily_price'
        sql = "DELETE FROM dbo.{0} WHERE price_time > '{1}'".format(table_name, date.strftime('%Y-%m-%d'))
        with self.engine.connect() as con:
            con.execute(sql)

    def insert_daily_data(self, df):
        df = df.rename(columns={
            'datetime': 'price_time',
            'open': 'price_open',
            'high': 'price_high',
            'low': 'price_low',
            'close': 'price_close'
        }).dropna()
        df['price_time'] = pd.to_datetime(df['price_time']).dt.strftime('%Y-%m-%d')
        df['rl10'] = df['rl10'].round(3)
        df['rl30'] = df['rl30'].round(3)
        file_path = 'daily.csv'
        df[['price_time', 'ticker', 'price_open', 'price_high', 'price_low', 'price_close', 'rl10', 'rl30']].to_csv(
            file_path, header=False, index=False)
        absolute_path = os.path.abspath(file_path)
        sql = """
            BULK INSERT RLCO.dbo.daily_price
            FROM '{0}' WITH (
            FIELDTERMINATOR=',',
            ROWTERMINATOR='\n'
            );
        """.format(absolute_path)
        with self.engine.connect() as con:
            con.execute(sql)

    def insert_intraday_data(self, df):
        df = df.rename(columns={
            'datetime': 'price_time',
            'open': 'price_open',
            'high': 'price_high',
            'low': 'price_low',
            'close': 'price_close'
        }).dropna()
        df['price_time'] = pd.to_datetime(df['price_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['rl10'] = df['rl10'].round(3)
        df['rl30'] = df['rl30'].round(3)
        file_path = 'intraday.csv'
        df[['price_time', 'ticker', 'price_open', 'price_high', 'price_low', 'price_close', 'rl10', 'rl30']].to_csv(file_path, header=False, index=False)
        absolute_path = os.path.abspath(file_path)
        sql = """
            BULK INSERT RLCO.dbo.intraday_price
            FROM '{0}' WITH (
            FIELDTERMINATOR=',',
            ROWTERMINATOR='\n'
            );
        """.format(absolute_path)
        with self.engine.connect() as con:
            con.execute(sql)