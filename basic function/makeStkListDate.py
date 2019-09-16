from basicFunction import *

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


if __name__ == "__main__":
    print(makeStkListDate("20190701", "20190830"))