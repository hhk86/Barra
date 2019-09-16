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


def parse_ST_type(s: str) -> str:
    '''
    将特别处理类型由字母解析成文字
    :param s: str
    :return: str, 共6种特别处理类型
    '''
    if s == 'S':
        return "特别处理(ST)"
    elif s == 'Z':
        return "暂停上市"
    elif s == 'P':
        return "特别转让服务(PT)"
    elif s == 'L':
        return "退市处理"
    elif s == 'X':
        return "创业板暂停上市风险警示"
    elif s == 'T':
        return "退市"
    else:
        raise ValueError("特别处理类型错误：" + s)



def makeStkST(start_date: str, end_date: str, daily_universe=None) -> pd.DataFrame:
    '''
    查询某一日期区间内（包括查询初始日和结束日）每天的股票ST状态和类别。
    :param start_date: str, 初始日期，"YYYYMMDD"
    :param end_date: str, 结束日期，"YYYYMMDD"
    :return: pd.DataFrame, columns = [tradeday - str，ticker - str，ST - bool, ST_type - str]
    '''

    sql = \
        '''
        SELECT
            S_INFO_WINDCODE,
            S_TYPE_ST,
            ENTRY_DT,
            REMOVE_DT 
        FROM
            ASHAREST
        WHERE
            ENTRY_DT <= {1}
            AND (
            REMOVE_DT > {0}   --由于[ENTRY_DT, REMOVE_DT）左闭右开, 此处不能取等号
            OR REMOVE_DT IS NULL)
        '''.format(start_date, end_date)
    with OracleSql() as oracle:
        st = oracle.query(sql)
    st["ST_type"] = st["S_TYPE_ST"].apply(parse_ST_type)
    if daily_universe is None:
        tradedays = getTradeCalendar(start_date, end_date)
        daily_universe = pd.DataFrame([])
        for tradeday in tradedays:
            daily_universe = daily_universe.append(makeDailyUniverse(tradeday))

    # 由于[ENTRY_DT, REMOVE_DT）左闭右开，不能用end_date来代替Null的REMOVE_DT, 用2099年12月31日代替右端点
    st['REMOVE_DT'] = np.where(st['REMOVE_DT'].isna(), "20991231", st['REMOVE_DT'])

    daily_universe['ST'] = False
    daily_universe['ST_type'] = None

    for idx, record in st.iterrows():
        ticker = record['S_INFO_WINDCODE']
        start_dt = record['ENTRY_DT']
        end_dt = record['REMOVE_DT']
        ST_type = record['ST_type']
        logic = (daily_universe['ticker'] == ticker) & (daily_universe['tradeday'] >= start_dt) \
                & (daily_universe['tradeday'] < end_dt) #第三个条件没有等号
        daily_universe['ST'] = np.where(logic, True, daily_universe['ST'])
        daily_universe['ST_type'] = np.where(logic, ST_type, daily_universe['ST_type'])
    daily_universe.index = list(range(daily_universe.shape[0]))
    return daily_universe


