import cx_Oracle
import pandas as pd
from makeTradeCalendar import getTradeCalendar


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


def makeStkName(start_date: str, end_date: str) -> pd.DataFrame:
    '''
    查询某一日期区间内（包括查询初始日和结束日）每天的股票名称。
    :param start_date: str, 初始日期，"YYYYMMDD"
    :param end_date: str, 结束日期，"YYYYMMDD"
    :return: pd.DataFrame, columns = [日期，股票代码，股票名称]
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
    name_range.ENDDATE = name_range.ENDDATE.apply(lambda s: end_date if s is None else s)
    ticker_list = name_range.groupby("S_INFO_WINDCODE").S_INFO_WINDCODE.count().index.tolist()
    tradedate_list = getTradeCalendar(start_date, end_date)
    stock_name_df = pd.DataFrame(columns=["tradeday", "ticker", "name"])
    i = 0
    N = len(tradedate_list) * len(ticker_list)
    for date in tradedate_list:
        for ticker in ticker_list:
            name = name_range[(name_range.S_INFO_WINDCODE == ticker) & (name_range.BEGINDATE <= date)
                       & (name_range.ENDDATE >= date)].squeeze().S_INFO_NAME
            stock_name_df.loc[i] = [date, ticker, name]
            i += 1
        print("\r完成" + str(round(100 * i / N, 1)) + '%', end='')
    return stock_name_df



