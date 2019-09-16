import cx_Oracle
import pandas as pd
from makeTradeCalendar import getTradeCalendar
from makeDailyUniverse import makeDailyUniverse


class OracleSql(object):
    '''
    Oracle数据库数据访问

    '''

    def __init__(self):
        '''
        初始化数据库连接
        '''
        self.host, self.oracle_port = '18.210.64.72', '1521'
        self.db, self.current_schema = 'tdb', 'wind'
        self.user, self.pwd = 'reader', 'reader'

    def __enter__(self):
        self.conn = self.__connect_to_oracle()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def __connect_to_oracle(self):
        dsn = self.host + ':' + self.oracle_port + '/' + self.db
        try:
            connection = cx_Oracle.connect(self.user, self.pwd, dsn, encoding="UTF-8", nencoding="UTF-8")
            connection.current_schema = self.current_schema
            print('连接oracle数据库')
        except Exception:
            print('不能连接oracle数据库')
            connection = None
        return connection

    def query(self, sql):
        '''
        查询并返回数据

        '''
        return pd.read_sql(sql, self.conn)

    def execute(self, sql):
        '''
        对数据库执行插入、修改等数据上行操作

        '''
        self.conn.cursor().execute(sql)
        self.conn.commit()


def makeStkPrice(start_date: str, end_date: str, daily_universe=None) -> pd.DataFrame:
    '''
    查询某一日期区间内（包括查询初始日和结束日）每天的OHLC、交易量、交易金额、复权因子和复权OHLC。
    对于停牌股票和日期，这些列取空值。
    :param start_date: str, 初始日期，"YYYYMMDD"
    :param end_date: str, 结束日期，"YYYYMMDD"
    :return: pd.DataFrame, columns = [tradeday - str, ticker - str, open - numpy.float64,  high - numpy.float64,
            low - numpy.float64, close - numpy.float64, volume - numpy.float64, amount - numpy.float64,
            adjust_factor - numpy.float64, adjust_open - numpy.float64, adjust_high - numpy.float64,
            adjust_low - numpy.float64, adjust_close - numpy.float64]
    '''

    sql = \
        '''
        SELECT
            TRADE_DT,
            S_INFO_WINDCODE,
            S_DQ_OPEN,
            S_DQ_HIGH,
            S_DQ_LOW,
            S_DQ_CLOSE， 
            S_DQ_VOLUME,
            S_DQ_AMOUNT, 
            S_DQ_ADJFACTOR,
            S_DQ_ADJOPEN,
            S_DQ_ADJHIGH,
            S_DQ_ADJLOW， 
            S_DQ_ADJCLOSE
        FROM
            ASHAREEODPRICES 
        WHERE
            TRADE_DT >= {0}
            AND TRADE_DT <= {1}
        '''.format(start_date, end_date)
    with OracleSql() as oracle:
        price = oracle.query(sql)
    price.columns = ["tradeday", "ticker", "open", "high", "low", "close", "volume", "amount", "adjust_factor",
                     "adjust_open", "adjust_high", "adjust_low", "adjust_close"]

    if daily_universe is None:
        tradedays = getTradeCalendar(start_date, end_date)
        daily_universe = pd.DataFrame([])
        for tradeday in tradedays:
            daily_universe = daily_universe.append(makeDailyUniverse(tradeday))

    daily_universe = pd.merge(daily_universe, price, on=["tradeday", "ticker"], how="left")

    daily_universe.index = list(range(daily_universe.shape[0]))
    return daily_universe

