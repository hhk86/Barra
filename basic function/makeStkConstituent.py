import cx_Oracle
import pandas as pd
import numpy as np
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


def makeStkConstituent(start_date: str, end_date: str, daily_universe=None) -> pd.DataFrame:
    '''
    查询某一日期区间内（包括查询初始日和结束日）每天股票的成分股信息。
    :param start_date: str, 初始日期，"YYYYMMDD"
    :param end_date: str, 结束日期，"YYYYMMDD"
    :return: pd.DataFrame, columns = [tradeday - str，ticker - str，name - str]
    '''

    sql1 = \
        '''
        SELECT
            TRADE_DT,
            S_CON_WINDCODE 
        FROM
            AINDEXCSI500WEIGHT
        WHERE
            TRADE_DT >= {0}
            AND TRADE_DT <= {1}
        '''.format(start_date, end_date)
    with OracleSql() as oracle:
        CSI500 = oracle.query(sql1)

    sql2 = \
        '''
        SELECT
            TRADE_DT,
            S_CON_WINDCODE 
        FROM
            AIndexHS300Weight
        WHERE
            TRADE_DT >= {0}
            AND TRADE_DT <= {1}
        '''.format(start_date, end_date)
    with OracleSql() as oracle:
        HS300 = oracle.query(sql2)

    CSI500.columns = ["tradeday", "ticker"]
    CSI500["CSI500"] = True
    HS300.columns = ["tradeday", "ticker"]
    HS300["HS300"] = True

    if daily_universe is None:
        tradedays = getTradeCalendar(start_date, end_date)
        daily_universe = pd.DataFrame([])
        for tradeday in tradedays:
            daily_universe = daily_universe.append(makeDailyUniverse(tradeday))

    daily_universe = pd.merge(daily_universe, CSI500, on=["tradeday", "ticker"], how="left")
    daily_universe = pd.merge(daily_universe, HS300, on=["tradeday", "ticker"], how="left")
    daily_universe["CSI500"].fillna(False, inplace=True)
    daily_universe["HS300"].fillna(False, inplace=True)
    daily_universe.sort_values(by=["tradeday", "ticker"], inplace=True)

    daily_universe.index = list(range(daily_universe.shape[0]))
    daily_universe.to_csv("debug.csv", encoding="gbk")
    return daily_universe
