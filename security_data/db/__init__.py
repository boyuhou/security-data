import sqlalchemy as sa
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
        self.daily_table = sa.Table('daily_price', sa.MetaData(),
                                    sa.Column('price_time', DATE, primary_key=True),
                                    sa.Column('price_open', MONEY),
                                    sa.Column('price_high', MONEY),
                                    sa.Column('price_low', MONEY),
                                    sa.Column('price_close', MONEY),
                                    sa.Column('rl10', MONEY),
                                    sa.Column('rl30', MONEY),
                                    sa.Column('ticker', VARCHAR, primary_key=True),
                                    )
        self.intraday_table = sa.Table('intraday_price', sa.MetaData(),
                                       sa.Column('price_time', SMALLDATETIME, primary_key=True),
                                       sa.Column('price_open', MONEY),
                                       sa.Column('price_high', MONEY),
                                       sa.Column('price_low', MONEY),
                                       sa.Column('price_close', MONEY),
                                       sa.Column('rl10', MONEY),
                                       sa.Column('rl30', MONEY),
                                       sa.Column('ticker', VARCHAR, primary_key=True),
                                       )

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
        df = df.reset_index().rename(columns={
            'datetime': 'price_time',
            'open': 'price_open',
            'high': 'price_high',
            'low': 'price_low',
            'close': 'price_close'
        }).dropna()
        insert_statement = self.daily_table.insert().values(df.to_records(index=False).tolist())
        with self.engine.connect() as con:
            con.execute(insert_statement)

# https://stackoverflow.com/questions/29638136/how-to-speed-up-bulk-insert-to-ms-sql-server-from-csv-using-pyodbc
    def insert_intraday_data(self, df):
        df = df.reset_index().rename(columns={
            'datetime': 'price_time',
            'open': 'price_open',
            'high': 'price_high',
            'low': 'price_low',
            'close': 'price_close'
        }).dropna()
        df['price_time'] = df['price_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        insert_statement = self.intraday_table.insert().values(df.to_records(index=False).tolist())
        with self.engine.connect() as con:
            con.execute(insert_statement)