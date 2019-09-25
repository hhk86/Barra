from pymongo import MongoClient
from basicFunction import *
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
        print("connect to Mongodb")
        self.conn = MongoClient(self.host, self.port, username=self.username, password=self.password)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def connect(self):
        return self

def add_one_factor(factor, collection, start_date, end_date, test_mode) -> None:
    df = make_financial_factor(start_date, end_date, factor, test_mode=test_mode)
    df.rename(columns={'tradeday': 'trade_date', 'ticker': 'code'}, inplace=True)
    factor = df.columns[2]
    snapshots = df.columns[3]
    N = df.shape[0]
    i = 0
    with MongoDB() as mongo:
        connection = mongo.connect()
        db = connection.conn["basicdb"]
        collection = db[collection]
        for key, record in df.iterrows():
            if i % 2000 == 0:
                print("\rUpdate " + factor  + ": "+ str(round(i/ N * 100, 3)) + '%', end=" ")
            i += 1
            collection.update_one({"trade_date" : record["trade_date"], "code": record["code"]},
                                  {"$set" : {factor: record[factor], snapshots: record[snapshots]}})



if __name__ == "__main__":

    # Update test 1 record
    confirm()
    start_date = "20090101"
    end_date = "20190831"
    test_mode = False
    for factor in ["cash", "tradable_financialasset", "notes_receiveable", "accounts_receivable",
                  "inventory", "fixed_asset", "construction_inprogress", "intangible_asset",
                  "development_expenditure", "goodwill", "notes_payable", "accounts_payable"]:
        add_one_factor(factor, "basic_balance", start_date, end_date, test_mode=test_mode)
