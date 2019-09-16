import cx_Oracle
import pandas as pd
import numpy as np
import datetime as dt
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


def get_last_suspend_list(resump_series: pd.core.series.Series) -> list:
    '''

    根据复牌日系列，返回最后一个停牌日列表，最后一个停牌日定义为复牌日的上一个交易日。
    使用最后一个停牌日，使得停牌区间可以用[suspend_date, last_suspend_date]闭区间表示，与单停牌日[suspend_date, suspend_date]
    以及其他函数的闭区间保持一致。
    :param resump_date: str, "YYYYMMDD"
    :return: str, "YYYYMMDD"
    '''
    last_suspend_list = list()
    for resump_date in resump_series:
        if resump_date is None:
            last_suspend_list.append(None)
        else:
            last_suspend_list.append(
                dt.datetime.strftime(dt.datetime.strptime(resump_date, "%Y%m%d") - dt.timedelta(1), "%Y%m%d"))
    return last_suspend_list


def parse_suspend_type(code: np.int64) -> str:
    '''
    将停牌类型代码解析成文字。
    :param code: int, 9位数字
    :return: str, 共六种停牌类型
    '''

    if code == 444001000:
        return "上午停牌"
    elif code == 444002000:
        return "下午停牌"
    elif code == 444003000:
        return "今起停牌"
    elif code == 444004000:
        return "盘中停牌"
    elif code == 444007000:
        return "停牌1小时"
    elif code == 444016000:
        return "停牌一天"
    else:
        raise ValueError("停牌类型代码错误: " + str(code))


def makeStkSuspend(start_date: str, end_date: str, daily_universe=None) -> pd.DataFrame:
    '''
    查询某一日期区间内（包括查询初始日和结束日）每天的股票的停牌状态。是否停牌用布尔值表示（停牌是True），
    停牌类型、停牌原因、停牌原因代码缺省值是None。
    :param start_date: str, 初始日期，"YYYYMMDD"
    :param end_date: str, 结束日期，"YYYYMMDD"
    :return: pd.DataFrame, columns = [tradeday - str，ticker - str，name - str, suspend - bool,
                    suspend_type - str, suspend_reason_code - object, suspend_reason - str]
    '''

    sql = \
        '''
        SELECT
            S_INFO_WINDCODE,
            S_DQ_SUSPENDDATE,
            S_DQ_RESUMPDATE,
            S_DQ_SUSPENDTYPE,
            S_DQ_CHANGEREASONTYPE,
            S_DQ_CHANGEREASON 
        FROM
            ASHARETRADINGSUSPENSION 
        WHERE
                ( S_DQ_RESUMPDATE IS NULL 
                AND S_DQ_SUSPENDDATE >= {0}
                AND S_DQ_SUSPENDDATE <= {1} ) 
            OR 
                (S_DQ_RESUMPDATE IS NOT NULL 
                AND S_DQ_SUSPENDDATE <= {1}
                AND S_DQ_RESUMPDATE >= {0} )
        '''.format(start_date, end_date)
    with OracleSql() as oracle:
        name_range = oracle.query(sql)
    name_range['last_suspend_date'] = np.where(name_range['S_DQ_RESUMPDATE'].isna(), name_range["S_DQ_SUSPENDDATE"],
                                               get_last_suspend_list(name_range['S_DQ_RESUMPDATE']))
    name_range["S_DQ_SUSPENDTYPE"] = name_range["S_DQ_SUSPENDTYPE"].apply(parse_suspend_type)

    if daily_universe is None:
        tradedays = getTradeCalendar(start_date, end_date)
        daily_universe = pd.DataFrame([])
        for tradeday in tradedays:
            daily_universe = daily_universe.append(makeDailyUniverse(tradeday))
    daily_universe['suspend'] = False
    daily_universe['suspend_type'] = None
    daily_universe['suspend_reason_code'] = None
    daily_universe['suspend_reason'] = None

    for idx, record in name_range.iterrows():
        ticker = record['S_INFO_WINDCODE']
        start_dt = record['S_DQ_SUSPENDDATE']
        end_dt = record['last_suspend_date']
        suspend_type = record['S_DQ_SUSPENDTYPE']
        suspend_reason = record["S_DQ_CHANGEREASON"]
        suspend_reason_code = record["S_DQ_CHANGEREASONTYPE"]
        logic = (daily_universe['ticker'] == ticker) & (daily_universe['tradeday'] >= start_dt) & \
                (daily_universe['tradeday'] <= end_dt)
        daily_universe['suspend'] = np.where(logic, True, daily_universe['suspend'])
        daily_universe['suspend_type'] = np.where(logic, suspend_type, daily_universe['suspend_type'])
        daily_universe['suspend_reason'] = np.where(logic, suspend_reason, daily_universe['suspend_reason'])
        daily_universe['suspend_reason_code'] = np.where(logic, suspend_reason_code, daily_universe['suspend_reason_code'])

    daily_universe.index = list(range(daily_universe.shape[0]))
    return daily_universe


