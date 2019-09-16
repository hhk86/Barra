import pandas as pd
import cx_Oracle


class OracleSql(object):
    '''
    Query data from database
    '''

    def __init__(self, pt=False):
        '''
        Initialize database
        '''
        self.host, self.oracle_port = '18.210.64.72', '1521'
        self.db, self.current_schema = 'tdb', 'wind'
        self.user, self.pwd = 'reader', 'reader'
        self.pt = pt

    def __enter__(self):
        '''
        Connect to database
        :return: self
        '''
        self.conn = self.__connect_to_oracle()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def __connect_to_oracle(self):
        '''
        Connect to database
        :return: connection
        '''
        dsn = self.host + ':' + self.oracle_port + '/' + self.db
        try:
            connection = cx_Oracle.connect(self.user, self.pwd, dsn, encoding="UTF-8", nencoding="UTF-8")
            connection.current_schema = self.current_schema
            if self.pt is True:
                print('Connected to Oracle database successful!')
        except Exception:
            print('Failed on connecting to Oracle database!')
            connection = None
        return connection

    def query(self, sql: str) -> pd.DataFrame:
        '''
        Query data
        '''
        return pd.read_sql(sql, self.conn)

    def execute(self, sql: str):
        '''
        Execute SQL scripts, including inserting and updating

        '''
        self.conn.cursor().execute(sql)
        self.conn.commit()


def getTradeCalendar(start_date: str, end_date: str) -> list:
    '''
    获取某一日期区间内的所有交易日（包括起始日期和终止日期）。
    :param start_date: str, 起始日期, "YYYMMDD"
    :param end_date:str, 终止日期, "YYYMMDD"
    :return: list, 交易日列表
    '''
    sql = \
        '''
        SELECT
            TRADE_DAYS 
        FROM
            asharecalendar 
        WHERE
            S_INFO_EXCHMARKET = 'SSE' 
            AND trade_days BETWEEN {} AND {}
    '''.format(start_date, end_date)
    with OracleSql() as oracle:
        tradingDays = oracle.query(sql)
    return sorted(tradingDays.TRADE_DAYS.tolist())
