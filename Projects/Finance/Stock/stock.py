import pandas_datareader as web
import yfinance as yf
import Data.FileFormat.pgexcel as pgexcel
import Data.Connect.pgmysql as pgmysql
import csv
import pandas as pd
import yfinance as yf
import Data.Config.pgconfig as pgconfig
import Data.Logging.pglogging as pglogging
import sys

LOGGING_LEVEL = pglogging.logging.INFO
pglogging.logging.basicConfig(level=LOGGING_LEVEL)


class StockData:
    def __init__(self):
        self._project_name = self.__class__.__name__.lower()
        self._config = pgconfig.Config(project_name=self._project_name)
        try:
            self._logger = pglogging.Logging(self._config, logging_level=LOGGING_LEVEL,
                                             subject=f" {self._project_name} logger").getLogger(self._project_name)

        except Exception as err:
            self._logger.logging.critical(f"unable to instantiate Daemon logger {err}")
            sys.exit(99)

        self._logger.debug('Instantiate config and metadata objects')
        self._logger.info(f"The log for '{self._project_name}' is stored at {getattr(self._config, 'logger_file_filedir')}")

    @property
    def logger(self):
        return self._logger


def main():
    sd = StockData()
    mysql = pgmysql.PGMysql("/Users/jianhuang/opt/anaconda3/envs/stock_data/mysql.yml")
    for ss in mysql.simple_query("select stock_symbol from stock_queue where status = 'None'"):
        stock_symbol = ss[0]
        sd.logger.info(f" Start loading profile data for {stock_symbol}")
        mysql.simple_query(f"update stock_queue set status = 'WIP' where stock_symbol = '{stock_symbol}'")
        try:
            stock = yf.Ticker(stock_symbol).info
        except Exception as err:
            sd.logger.critical(f"Something wrong parsing web content for {stock_symbol}")
            continue
        if stock:
            if mysql.populate_data(table_name="stock_detail", mode="simple", data_in=stock):
                mysql.simple_query(f"update stock_queue set status = 'DONE' where stock_symbol = '{stock_symbol}'")
                sd.logger.info(f" Loading profile data for {stock_symbol} is successfully completed\n\n")
            else:
                print("\n")
        else:
            print("\n")


if __name__ == '__main__':
    stock_data = pgexcel.PGexcel()
    stock_data.load_file_pd(filename="nasdaq_screener.csv")
    data = list(map(lambda x: tuple([x["Symbol"], "None"]),pd.DataFrame(stock_data.data, columns=['Symbol']).to_dict('records')))
    print(data)

    a = [tuple([item.values()]) for item in pd.DataFrame(web.DataReader('AAPL', data_source="yahoo", start='2012-01-01', end='2019-12-17')).to_dict('records')]
    print(a[0:2])

    exit(0)

    main()


    #with open('nasdaq_screener.csv', 'r') as csv_file:
    #    data = pd.read_csv(csv_file)
    #    df = pd.DataFrame(data, columns=['Symbol'])
    #    print(df.to_dict('records'))



    #stock_data = pgexcel.PGexcel()
    #stock_data.load_file_pd(filename="nasdaq_screener.csv")

    #data = list(map(lambda x: tuple([x["Symbol"], "None"]), pd.DataFrame(stock_data.data, columns=['Symbol']).to_dict('records')))
    #mysql.insert_table("stock_queue", "bulk", data)


    #print (pd.DataFrame(stock_data.data).to_dict('records')[0])

    #a = pd.DataFrame.from_dict(msft.info)

    #print(pd.DataFrame(a, columns=['sector']).to_dict('records'))


    print(web.DataReader('AAPL', data_source="yahoo", start='2012-01-01', end='2019-12-17'))