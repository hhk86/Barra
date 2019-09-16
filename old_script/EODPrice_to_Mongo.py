import sys
from pymongo import MongoClient
from basicFunction import *


class MongoDB():
    '''
    Connect to local MongoDB
    '''
    def __init__(self):
        self.host = "18.210.68.192"
        self.port = 27017
        self.db = "basicdb"
        self.username = "user"
        self.password = "user"

    def __enter__(self):
        self.conn = MongoClient(self.host, self.port, username=self.username, password=self.password)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def connect(self):
        return self

if __name__ == "__main__":
    confirm = input("please input: confirm\n>>>")
    if confirm  != "confirm":
        sys.exit()
    # sql = \
    #     '''
    #     SELECT
    #         *
    #     FROM
    #         ASHAREEODPRICES
    #     WHERE
    #         TRADE_DT >= '20190901'
    #     '''
    # with OracleSql() as oracle:
    #     df = oracle.query(sql)
    # df.drop(["OBJECT_ID", "OPDATE", "OPMODE"], axis=1, inplace=True)
    # print(df.columns)
    # df.to_csv("price_data.csv", index=None, encoding="gbk")

    df = pd.read_csv("price_data.csv", encoding="gbk")
    df.columns = ['code', 'trade_date', 'currency_code', 'preclose',
       'open', 'high', 'low', 'close', 'change',
       'pctchange', 'volume', 'amount', 'adjpreclose',
       'adjopen', 'adjhigh', 'adjlow', 'adjclose',
       'adjfactor', 'avgprice', 'trade_status']
    df["trade_date"] = df["trade_date"].astype(str)
    sql2 = \
        '''
        SELECT
            TRADE_DT,
            S_INFO_WINDCODE,
            UP_DOWN_LIMIT_STATUS 
        FROM
            ASHAREEODDERIVATIVEINDICATOR 
        WHERE
            TRADE_DT > 20090101
        '''
    with OracleSql() as oracle:
        df2 = oracle.query(sql2)
    df2.columns = ["trade_date", "code", "price_limit_status"]
    print(df2)
    df = pd.merge(df, df2, on=["trade_date", "code"], how="left")
    print(df)
    print(df.columns)
    df.to_csv("debug.csv", encoding="gbk")

    data = df.to_dict("records")
    with MongoDB() as mongo:
        connection = mongo.connect()
        db = connection.conn["basicdb"]
        collection = db["price"]
        collection.insert_many(data)
