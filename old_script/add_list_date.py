from pymongo import MongoClient
from make_financial_factor import *
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

# class MongoDB():
#     '''
#     Connect to local MongoDB
#     '''
#     def __init__(self):
#         self.host = "localhost"
#         self.port = 27017
#         self.db = "huang"
#
#     def __enter__(self):
#         self.conn = MongoClient(self.host, self.port)
#         return self
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         self.conn.close()
#
#     def connect(self):
#         return self

if __name__ == "__main__":

    # with MongoDB() as mongo:
#     #     connection = mongo.connect()
#     #     db = connection.conn["universedb"]
#     #     collection = db["universe_test"]
#     #     cursor = collection.find()
#     # df = pd.DataFrame(list(cursor))
#     # print(df)
#     # df.to_csv("just_test.csv", encoding='gbk', index=None)
    confirm  = input("please input: confirm\n>>>")
    if confirm != "confirm":
        sys.exit()

    df = pd.read_csv("backupdata.csv", encoding="gbk")
    # df = df[["_id", "code", "name"]]
    df = df[['trade_date', 'code', 'name',  'citic_industry_L1', 'citic_industry_L2',
            'sw_industry_L1', 'sw_industry_L2', 'suspend', 'suspend_type','suspend_reason_code', 'suspend_reason',
            'ST', 'ST_type' ]]
    sql = \
        '''
        SELECT
            S_INFO_WINDCODE, S_INFO_LISTDATE
        FROM
            ASHAREDESCRIPTION
        '''
    with OracleSql() as oracle:
        list_df = oracle.query(sql)
    list_df.columns = ["code", "list_date"]
    print(list_df)
    df = pd.merge(df, list_df, on = "code")
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
            TRADE_DT >= 20090101
        '''
    with OracleSql() as oracle:
        df2 = oracle.query(sql2)
    df2.columns = ["trade_date", "code", "price_limit_status"]
    print(df2)
    df = pd.merge(df, df2, on=["trade_date", "code"], how="left")
    print(df)
    print(df.columns)
    df.to_csv("debug2.csv", encoding="gbk")

    print("process....")
    df.suspend_type = df.suspend_type.apply(lambda s: None if pd.isnull(s) else s)
    df.suspend_reason_code = df.suspend_reason_code.apply(lambda s: None if pd.isnull(s) else s)
    df.suspend_reason = df.suspend_reason.apply(lambda s: None if pd.isnull(s) else s)
    df["suspend_reason_code"] = df.suspend_reason.apply(lambda s: None if pd.isnull(s) else s)
    df.ST_type = df.ST_type.apply(lambda s: None if pd.isnull(s) else s)
    print("replaced!")
    print(df.suspend_reason_code)


    data = df.to_dict("records")
    with MongoDB() as mongo:
        connection = mongo.connect()
        db = connection.conn["universedb"]
        collection = db["universe_new2"]
        collection.insert_many(data)






# with mongo as mongo:
#     connection = mongo.connect()
#     db = connection.conn["basicdb"]
#     collection = db["basic_balance_new"]
#     collection.rename('basic_balance')