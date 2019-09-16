from basicFunction import *

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


if __name__ == "__main__":
    print(makeStkPriceLimitStatus("20190701", "20190830"))