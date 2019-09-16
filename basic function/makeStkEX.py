import cx_Oracle
import numpy as np
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


def makeStkEX(start_date: str, end_date: str, daily_universe=None) -> pd.DataFrame:
    '''
    查询某一日期区间内（包括查询初始日和结束日）每天的股票的除息除权情况。
    除息除权类别有6种：分红、股改、增发、配股、缩股、Null。
    :param start_date: str, 初始日期，"YYYYMMDD"
    :param end_date: str, 结束日期，"YYYYMMDD"
    :return: pd.DataFrame, columns = [tradeday - str, ticker - str, ex_right_dividend - bool, ex_type - str]
    '''

    sql = \
        '''
        SELECT
            EX_DATE,
            S_INFO_WINDCODE,
            EX_TYPE 
        FROM
            AShareEXRightDividendRecord
        WHERE
            EX_DATE >= {0}
            AND EX_DATE <= {1}
        '''.format(start_date, end_date)
    with OracleSql() as oracle:
        EX = oracle.query(sql)
    EX.columns = ["tradeday", "ticker", "ex_type"]
    EX["ex_right_dividend"] = True
    EX = EX[["tradeday", "ticker", "ex_right_dividend", "ex_type"]]

    if daily_universe is None:
        tradedays = getTradeCalendar(start_date, end_date)
        daily_universe = pd.DataFrame([])
        for tradeday in tradedays:
            daily_universe = daily_universe.append(makeDailyUniverse(tradeday))

    daily_universe = pd.merge(daily_universe, EX, on=["tradeday", "ticker"], how="left")
    daily_universe["ex_right_dividend"].fillna(False, inplace=True)
    daily_universe.sort_values(by=["tradeday", "ticker"], inplace=True)
    daily_universe.index = list(range(daily_universe.shape[0]))
    return daily_universe


