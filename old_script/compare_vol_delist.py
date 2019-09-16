from basicFunction import *



if __name__ == "__main__":
    start_date = "20180101"
    end_date = "20190820"
    sql = \
        '''
        select TRADE_DT, S_INFO_WINDCODE from ASHAREEODPRICES
        where S_DQ_VOLUME = 0
        AND TRADE_DT BETWEEN {0} AND {1}
        '''.format(start_date, end_date)
    with OracleSql() as oracle:
        universe = oracle.query(sql)
    print(universe)
    universe.columns = ["tradeday", "ticker"]
    df1 = makeStkSuspend(start_date, end_date,daily_universe=universe)
    print(df1)
    df1.to_csv("debug2.csv", encoding="gbk")
    df2 = makeStkPrice(start_date, end_date, daily_universe=universe)
    df3 = makeStkName(start_date, end_date,daily_universe=universe)
    df = pd.merge(df1, df2, on=["tradeday", "ticker"])
    df = pd.merge(df, df3, on=["tradeday", "ticker"], how="left")
    df.to_csv("debug.csv",  encoding="gbk")
    print(df)


