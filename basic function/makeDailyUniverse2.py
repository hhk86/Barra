import cx_Oracle
import pandas as pd
import datetime as dt


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




def makeDailyUniverse2(tradeday: str)-> pd.DataFrame:
    '''
    返回指定交易日的上市股票代码, 使用AShareEODPrices
    :param tradeday: str, "YYYYMMDD"
    :return: pd.DataFrame, columns = [tradeday - str, ticker - obj]
    '''
    if dt.datetime.strptime(tradeday,"%Y%m%d") > dt.datetime.now():
        raise ValueError("输入日期晚于系统日期:" + str(tradeday))
    sql = \
    '''
    SELECT
        '{0}' AS tradedate,
        S_INFO_WINDCODE AS ticker
    FROM
        ASHAREEODPRICES
    WHERE
        TRADE_DT = {0}
    '''.format(tradeday)
    with OracleSql() as oracle:
        df = oracle.query(sql)
    df.columns = ["tradeday", "ticker"]
    return df




