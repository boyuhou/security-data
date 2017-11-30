import click
import logging
import datetime
import pandas as pd
from security_data import SecurityService

DATE_FORMAT = '%Y%m%d'
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %I:%M:%S')
logger = logging.getLogger(__name__)


@click.command()
@click.option('--input_file', default=r'.\config\FrogBoxList.csv', help='Trade data input folder')
@click.option('--start_date', default=pd.datetime.today().strftime(DATE_FORMAT))
def main(input_file, start_date):
    logger.info('input file: {0}'.format(input_file))
    logger.info('start date: {0}'.format(start_date))
    start_date = datetime.datetime.strptime(start_date+'000000', '%Y%m%d%H%M%S')
    ticker_list = get_ticker_list(input_file).tolist()
    security_service = SecurityService()
    # security_service.update_daily_data(ticker_list, start_date)
    security_service.update_intraday_data(ticker_list, start_date)


def get_ticker_list(input_file):
    df = pd.read_csv(input_file)
    return df['ticker']


if __name__ == '__main__':
    main()