from pymongo import MongoClient
from make_financial_factor import *


class MongoDB():
    '''
    Connect to local MongoDB
    '''
    def __init__(self):
        self.host = "18.210.68.192"
        self.port = 27017
        self.db = "universedb"
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
    start_date = "20090101"
    end_date = "20190831"
    confirm = input("please input: confirm\n>>>")
    if confirm  != "confirm":
        sys.exit()
    test_mode = False
    df = make_financial_factor(start_date, end_date, "net_income", test_mode=test_mode)
    data = df.to_dict("records")
    N = len(data)
    i = 0
    with MongoDB() as mongo:
        connection = mongo.connect()
        db = connection.conn["basicdb"]
        collection = db["basic_income"]
        collection.insert_many(data)
    temp_df = make_financial_factor(start_date, end_date, "total_revenue", test_mode=test_mode)
    df = pd.merge(df, temp_df, on=["tradeday", "ticker"], how="left")
    data = df.to_dict("records")
    with MongoDB() as mongo:
        connection = mongo.connect()
        db = connection.conn["basicdb"]
        collection = db["basic_income_new"]
        collection.insert_many(data)


    df = make_financial_factor(start_date, end_date, "operating_cashflow", test_mode=test_mode)
    data = df.to_dict("records")
    with MongoDB() as mongo:
        connection = mongo.connect()
        db = connection.conn["basicdb"]
        collection = db["basic_cashflow"]
        collection.insert_many(data)


    df = make_financial_factor(start_date, end_date, "total_asset", test_mode=test_mode)
    temp_df = make_financial_factor(start_date, end_date, "total_equity", test_mode=test_mode)
    df = pd.merge(df, temp_df,on=["tradeday", "ticker"], how="left")
    temp_df = make_financial_factor(start_date, end_date, "total_debt", test_mode=test_mode)
    df = pd.merge(df, temp_df, on=["tradeday", "ticker"], how="left")
    temp_df = make_financial_factor(start_date, end_date, "total_share", test_mode=test_mode)
    df = pd.merge(df, temp_df, on=["tradeday", "ticker"], how="left")
    data = df.to_dict("records")
    with MongoDB() as mongo:
        connection = mongo.connect()
        db = connection.conn["basicdb"]
        collection = db["basic_balance"]
        collection.insert_many(data)
