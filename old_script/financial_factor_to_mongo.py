from pymongo import MongoClient
from make_financial_factor import *


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
    start_date = "20090101"
    end_date = "20190831"
    confirm = input("please input: confirm\n>>>")
    if confirm  != "confirm":
        sys.exit()
    test_mode = False

    df1 = make_financial_factor(start_date, end_date, "total_assets", test_mode=test_mode)
    df2 = make_financial_factor(start_date, end_date, "total_equities_exc_min", test_mode=test_mode)
    df3 = make_financial_factor(start_date, end_date, "total_equities_inc_min", test_mode=test_mode)
    df4 = make_financial_factor(start_date, end_date, "noncur_liabilities", test_mode=test_mode)
    df5 = make_financial_factor(start_date, end_date, "total_liabilities", test_mode=test_mode)
    df6 = make_financial_factor(start_date, end_date, "longterm_loan", test_mode=test_mode)
    df7 = make_financial_factor(start_date, end_date, "bonds_payable", test_mode=test_mode)
    df8 = make_financial_factor(start_date, end_date, "longterm_payable", test_mode=test_mode)
    df9 = make_financial_factor(start_date, end_date, "preferred_stock", test_mode=test_mode)


    df = df1
    df = pd.merge(df, df2, on=["tradeday", "ticker"], how="outer")
    df = pd.merge(df, df3, on=["tradeday", "ticker"], how="outer")
    df = pd.merge(df, df4, on=["tradeday", "ticker"], how="outer")
    df = pd.merge(df, df5, on=["tradeday", "ticker"], how="outer")
    df = pd.merge(df, df6, on=["tradeday", "ticker"], how="outer")
    df = pd.merge(df, df7, on=["tradeday", "ticker"], how="outer")
    df = pd.merge(df, df8, on=["tradeday", "ticker"], how="outer")
    df = pd.merge(df, df9, on=["tradeday", "ticker"], how="outer")

    df.rename(columns={'tradeday': 'trade_date', 'ticker': 'code'}, inplace=True)

    print(df.head())


    data = df.to_dict("records")
    with MongoDB() as mongo:
        connection = mongo.connect()
        db = connection.conn["basicdb"]
        collection = db["basic_balance_new"]
        collection.insert_many(data)


    # df = make_financial_factor(start_date, end_date, "net_income", test_mode=test_mode)
    # data = df.to_dict("records")
    # N = len(data)
    # i = 0
    # with MongoDB() as mongo:
    #     connection = mongo.connect()
    #     db = connection.conn["basicdb"]
    #     collection = db["basic_income"]
    #     collection.insert_many(data)
    #
    # temp_df = make_financial_factor(start_date, end_date, "total_revenue", test_mode=test_mode)
    # df = pd.merge(df, temp_df, on=["tradeday", "ticker"], how="left")
    # data = df.to_dict("records")
    # with MongoDB() as mongo:
    #     connection = mongo.connect()
    #     db = connection.conn["basicdb"]
    #     collection = db["basic_income_new"]
    #     collection.insert_many(data)


    # df = make_financial_factor(start_date, end_date, "operating_cashflow", test_mode=test_mode)
    # data = df.to_dict("records")
    # with MongoDB() as mongo:
    #     connection = mongo.connect()
    #     db = connection.conn["basicdb"]
    #     collection = db["basic_cashflow"]
    #     collection.insert_many(data)
    #
    #
    # df = make_financial_factor(start_date, end_date, "total_asset", test_mode=test_mode)
    # temp_df = make_financial_factor(start_date, end_date, "total_equity", test_mode=test_mode)
    # df = pd.merge(df, temp_df,on=["tradeday", "ticker"], how="left")
    # temp_df = make_financial_factor(start_date, end_date, "total_debt", test_mode=test_mode)
    # df = pd.merge(df, temp_df, on=["tradeday", "ticker"], how="left")
    # temp_df = make_financial_factor(start_date, end_date, "total_share", test_mode=test_mode)
    # df = pd.merge(df, temp_df, on=["tradeday", "ticker"], how="left")
    # data = df.to_dict("records")
    # with MongoDB() as mongo:
    #     connection = mongo.connect()
    #     db = connection.conn["basicdb"]
    #     collection = db["basic_balance"]
    #     collection.insert_many(data)


# with mongo as mongo:
#     connection = mongo.connect()
#     db = connection.conn["basicdb"]
#     collection = db["basic_balance_new"]
#     collection.rename('basic_balance')