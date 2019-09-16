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


def makeStkCiticsIndustry(start_date: str, end_date: str, daily_universe=None) -> pd.DataFrame:
    '''
    查询某一日期区间内（包括查询初始日和结束日）每天股票的中信行业一级分类和二级分类。
    AShareIndustriesClassCITIC表中[ENTRY_DT, REMOVE_DT]是闭区间，与AShareST中的左闭右开区间不同，因此使用makeStkName逻辑。
    :param start_date: str, 初始日期，"YYYYMMDD"
    :param end_date: str, 结束日期，"YYYYMMDD"
    :return: pd.DataFrame, columns = [tradeday - str，ticker - str，name - str, citic_industry_L1 - str, citic_industry_L2 - str]
    '''

    sql1 = \
        '''
        SELECT
            a.S_INFO_WINDCODE,
            b.INDUSTRIESNAME,
            a.ENTRY_DT,
            a.REMOVE_DT 
        FROM
            ASHAREINDUSTRIESCLASSCITICS a,
            ASHAREINDUSTRIESCODE b 
        WHERE
            substr( a.CITICS_IND_CODE, 1, 4 ) = substr( b.INDUSTRIESCODE, 1, 4 ) 
            AND b.LEVELNUM = '2' 
            AND ENTRY_DT <= {1}
            AND (REMOVE_DT >= {0} OR REMOVE_DT IS NULL)
        '''.format(start_date, end_date)
    with OracleSql() as oracle:
        industry_L1 = oracle.query(sql1)
    sql2 = \
        '''
        SELECT
            a.S_INFO_WINDCODE,
            b.INDUSTRIESNAME,
            a.ENTRY_DT,
            a.REMOVE_DT 
        FROM
            ASHAREINDUSTRIESCLASSCITICS a,
            ASHAREINDUSTRIESCODE b 
        WHERE
            substr( a.CITICS_IND_CODE, 1, 6 ) = substr( b.INDUSTRIESCODE, 1, 6 ) 
            AND b.LEVELNUM = '3' 
            AND ENTRY_DT <= {1}
            AND (REMOVE_DT >= {0} OR REMOVE_DT IS NULL)
        '''.format(start_date, end_date)
    with OracleSql() as oracle:
        industry_L2 = oracle.query(sql2)
    industry_L1['REMOVE_DT'] = np.where(industry_L1['REMOVE_DT'].isna(), end_date, industry_L1['REMOVE_DT'])
    industry_L2['REMOVE_DT'] = np.where(industry_L2['REMOVE_DT'].isna(), end_date, industry_L2['REMOVE_DT'])


    if daily_universe is None:
        tradedays = getTradeCalendar(start_date, end_date)
        daily_universe = pd.DataFrame([])
        for tradeday in tradedays:
            daily_universe = daily_universe.append(makeDailyUniverse(tradeday))
    daily_universe['citic_industry_L1'] = None
    daily_universe['citic_industry_L2'] = None

    for idx, record in industry_L1.iterrows():
        ticker = record['S_INFO_WINDCODE']
        start_dt = record['ENTRY_DT']
        end_dt = record['REMOVE_DT']
        industry_name = record['INDUSTRIESNAME']
        daily_universe['citic_industry_L1'] = np.where((daily_universe['ticker'] == ticker) \
                                          & (daily_universe['tradeday'] >= start_dt) \
                                          & (daily_universe['tradeday'] <= end_dt),
                                          industry_name, daily_universe['citic_industry_L1'])
    for idx, record in industry_L2.iterrows():
        ticker = record['S_INFO_WINDCODE']
        start_dt = record['ENTRY_DT']
        end_dt = record['REMOVE_DT']
        industry_name = record['INDUSTRIESNAME']
        daily_universe['citic_industry_L2'] = np.where((daily_universe['ticker'] == ticker) \
                                          & (daily_universe['tradeday'] >= start_dt) \
                                          & (daily_universe['tradeday'] <= end_dt),
                                          industry_name, daily_universe['citic_industry_L2'])
    daily_universe.index = list(range(daily_universe.shape[0]))
    return daily_universe
