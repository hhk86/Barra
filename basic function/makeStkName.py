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


def makeStkName(start_date: str, end_date: str, daily_universe=None) -> pd.DataFrame:
    '''
    查询某一日期区间内（包括查询初始日和结束日）每天的股票名称。
    :param start_date: str, 初始日期，"YYYYMMDD"
    :param end_date: str, 结束日期，"YYYYMMDD"
    :return: pd.DataFrame, columns = [tradeday - str，ticker - str，name - str]
    '''

    sql = \
        '''
        SELECT
            S_INFO_WINDCODE,
            S_INFO_NAME,
            BEGINDATE,
            ENDDATE 
        FROM
            ASHAREPREVIOUSNAME 
        WHERE
            BEGINDATE <= {1}
            AND (
            ENDDATE >= {0} 
            OR ENDDATE IS NULL)
        '''.format(start_date, end_date)
    with OracleSql() as oracle:
        name_range = oracle.query(sql)

    if daily_universe is None:
        tradedays = getTradeCalendar(start_date, end_date)
        daily_universe = pd.DataFrame([])
        for tradeday in tradedays:
            daily_universe = daily_universe.append(makeDailyUniverse(tradeday))

    name_range['ENDDATE'] = np.where(name_range['ENDDATE'].isna(), end_date, name_range['ENDDATE'])

    daily_universe['name'] = None

    for idx, record in name_range.iterrows():
        ticker = record['S_INFO_WINDCODE']
        start_dt = record['BEGINDATE']
        end_dt = record['ENDDATE']
        name = record['S_INFO_NAME']
        daily_universe['name'] = np.where((daily_universe['ticker'] == ticker) \
                                          & (daily_universe['tradeday'] >= start_dt) \
                                          & (daily_universe['tradeday'] <= end_dt),
                                          name, daily_universe['name'])
    daily_universe.index = list(range(daily_universe.shape[0]))
    return daily_universe
