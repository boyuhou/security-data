import sqlalchemy as sa
import urllib.parse
import pandas as pd


class DataService(object):
    def __init__(self):
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 13 for SQL Server};SERVER=localhost;DATABASE=RLCO;UID=root;PWD=root")
        self.engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=' + params)

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