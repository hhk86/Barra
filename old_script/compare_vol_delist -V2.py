from basicFunction import *



if __name__ == "__main__":
    start_date = "20190101"
    end_date = "20190820"
    df1 = makeStkSuspend(start_date, end_date)
    df1 = df1[df1["suspend"] == True]
    universe = df1[["tradeday", "ticker"]]
    print(universe)


    # sql = \
    #     '''
    #     select TRADE_DT, S_INFO_WINDCODE from ASHAREEODPRICES
    #     where S_DQ_VOLUME = 0
    #     AND TRADE_DT BETWEEN {0} AND {1}
    #     '''.format(start_date, end_date)
    # with OracleSql() as oracle:
    #     universe = oracle.query(sql)
    # print(universe)


    # df1.to_csv("debug2.csv", encoding="gbk")
    df2 = makeStkPrice(start_date, end_date, daily_universe=universe)
    # df3 = makeStkName(start_date, end_date,daily_universe=universe)
    # df = pd.merge(df1, df2, on=["tradeday", "ticker"])
    # df = pd.merge(df, df3, on=["tradeday", "ticker"], how="left")
    df2.to_csv("debug.csv",  encoding="gbk")
    print(df2)


