import cx_Oracle
import numpy as np
import pandas as pd
import datetime as dt
import sys


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


def makeDailyUniverse(tradeday: str)-> pd.DataFrame:
    '''
    返回指定交易日的上市股票代码
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
        ASHAREDESCRIPTION 
    WHERE
        S_INFO_LISTDATE <= {0}
        AND (
            S_INFO_DELISTDATE > {0}
        OR S_INFO_DELISTDATE IS NULL 
        )
    '''.format(tradeday)
    with OracleSql() as oracle:
        df = oracle.query(sql)
    df.columns = ["tradeday", "ticker"]
    return df


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


def makeStkSWIndustry(start_date: str, end_date: str, daily_universe=None) -> pd.DataFrame:
    '''
    查询某一日期区间内（包括查询初始日和结束日）每天股票的申万行业一级分类和二级分类。
    AShareSWIndustriesClass表中[ENTRY_DT, REMOVE_DT]是闭区间，与AShareST中的左闭右开区间不同，因此使用makeStkName逻辑。
    :param start_date: str, 初始日期，"YYYYMMDD"
    :param end_date: str, 结束日期，"YYYYMMDD"
    :return: pd.DataFrame, columns = [tradeday - str，ticker - str，name - str, sw_industry_L1 - str, sw_industry_L2 - str]
    '''

    sql1 = \
        '''
        SELECT
            a.S_INFO_WINDCODE,
            b.INDUSTRIESNAME,
            a.ENTRY_DT,
            a.REMOVE_DT 
        FROM
            ASHARESWINDUSTRIESCLASS a,
            ASHAREINDUSTRIESCODE b 
        WHERE
            substr( a.SW_IND_CODE, 1, 4 ) = substr( b.INDUSTRIESCODE, 1, 4 ) 
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
            ASHARESWINDUSTRIESCLASS a,
            ASHAREINDUSTRIESCODE b 
        WHERE
            substr( a.SW_IND_CODE, 1, 6 ) = substr( b.INDUSTRIESCODE, 1, 6 ) 
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
    daily_universe['sw_industry_L1'] = None
    daily_universe['sw_industry_L2'] = None

    for idx, record in industry_L1.iterrows():
        ticker = record['S_INFO_WINDCODE']
        start_dt = record['ENTRY_DT']
        end_dt = record['REMOVE_DT']
        industry_name = record['INDUSTRIESNAME']
        daily_universe['sw_industry_L1'] = np.where((daily_universe['ticker'] == ticker) \
                                          & (daily_universe['tradeday'] >= start_dt) \
                                          & (daily_universe['tradeday'] <= end_dt),
                                          industry_name, daily_universe['sw_industry_L1'])
    for idx, record in industry_L2.iterrows():
        ticker = record['S_INFO_WINDCODE']
        start_dt = record['ENTRY_DT']
        end_dt = record['REMOVE_DT']
        industry_name = record['INDUSTRIESNAME']
        daily_universe['sw_industry_L2'] = np.where((daily_universe['ticker'] == ticker) \
                                          & (daily_universe['tradeday'] >= start_dt) \
                                          & (daily_universe['tradeday'] <= end_dt),
                                          industry_name, daily_universe['sw_industry_L2'])
    daily_universe.index = list(range(daily_universe.shape[0]))
    return daily_universe


def makeStkListDate(start_date: str, end_date: str, daily_universe=None) -> pd.DataFrame:
    '''
    查询某一日期区间内（包括查询初始日和结束日）每天的股票的上市日期。
    :param start_date: str, 初始日期，"YYYYMMDD"
    :param end_date: str, 结束日期，"YYYYMMDD"
    :return: pd.DataFrame, columns = [tradeday - str，ticker - str，list_date - str]
    '''

    sql = \
        '''
        SELECT
            S_INFO_WINDCODE,
            S_INFO_LISTDATE,
            S_INFO_DELISTDATE
        FROM
            ASHAREDESCRIPTION 
        WHERE
            S_INFO_LISTDATE <= {1} 
            AND (
            S_INFO_DELISTDATE >= {0}
            OR S_INFO_DELISTDATE IS NULL)
        '''.format(start_date, end_date)
    with OracleSql() as oracle:
        list_date_df = oracle.query(sql)


    if daily_universe is None:
        tradedays = getTradeCalendar(start_date, end_date)
        daily_universe = pd.DataFrame([])
        for tradeday in tradedays:
            daily_universe = daily_universe.append(makeDailyUniverse(tradeday))

    list_date_df['S_INFO_DELISTDATE'] = np.where(list_date_df['S_INFO_DELISTDATE'].isna(), "20991231",
                                                 list_date_df['S_INFO_DELISTDATE'])
    daily_universe['list_date'] = None

    for idx, record in list_date_df.iterrows():
        ticker = record['S_INFO_WINDCODE']
        start_dt = record['S_INFO_LISTDATE']
        end_dt = record['S_INFO_DELISTDATE']
        list_date = record['S_INFO_LISTDATE']
        daily_universe['list_date'] = np.where((daily_universe['ticker'] == ticker) \
                                          & (daily_universe['tradeday'] >= start_dt) \
                                          & (daily_universe['tradeday'] < end_dt),
                                          list_date, daily_universe['list_date'])
    daily_universe.index = list(range(daily_universe.shape[0]))
    return daily_universe


def makeStkPriceLimitStatus(start_date: str, end_date: str, daily_universe=None) -> pd.DataFrame:
    '''
    查询某一日期区间内（包括查询初始日和结束日）每天的股票的涨跌幅状态，1代表涨停，-1代表跌停，0代表没有涨停和跌停。
    :param start_date: str, 初始日期，"YYYYMMDD"
    :param end_date: str, 结束日期，"YYYYMMDD"
    :return: pd.DataFrame, columns = [tradeday - str，ticker - str，list_date - str]
    '''

    sql = \
        '''
        SELECT
            TRADE_DT,
            S_INFO_WINDCODE,
            UP_DOWN_LIMIT_STATUS 
        FROM
            ASHAREEODDERIVATIVEINDICATOR 
        WHERE
            TRADE_DT > {0} 
            AND TRADE_DT < {1}
        '''.format(start_date, end_date)
    with OracleSql() as oracle:
        price_limit = oracle.query(sql)

    if daily_universe is None:
        tradedays = getTradeCalendar(start_date, end_date)
        daily_universe = pd.DataFrame([])
        for tradeday in tradedays:
            daily_universe = daily_universe.append(makeDailyUniverse(tradeday))

    price_limit.columns = ["tradeday", "ticker", "price_limit_status"]
    daily_universe = pd.merge(daily_universe, price_limit, on=["tradeday", "ticker"])

    daily_universe.index = list(range(daily_universe.shape[0]))
    return daily_universe


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

def confirm() -> None:
    '''
    Confirm before inserting or updating.
    :return: None
    '''
    confirm = input('''Please input the word "confirm" to confirm the inserting or updating operation.\n>>>''')
    if confirm != "confirm":
        sys.exit()







