from makeStkName import *


def get_ticker_whose_name_changed(start_date, end_date):
    sql =\
    '''
    SELECT
        S_INFO_WINDCODE
    FROM
        ASHAREPREVIOUSNAME 
    WHERE
        ( BEGINDATE BETWEEN {0} AND {1} ) 
        OR ( ENDDATE BETWEEN {0} AND {1} )
    '''.format(start_date, end_date)
    with OracleSql() as oracle:
        df = oracle.query(sql)
    return df.squeeze().tolist()


if __name__ == "__main__":
    results = makeStkName("20151001", "20151130")
    ticker_list = get_ticker_whose_name_changed("20151001", "20151130")
    for ticker in ticker_list:
        print(results[results["ticker"] == ticker ])
        print('\n' * 3)