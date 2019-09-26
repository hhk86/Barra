from pymongo import MongoClient
from pymongo import UpdateOne
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
    request = list()
    for key, record in df.iterrows():
        request.append(UpdateOne({"trade_date" : record["trade_date"], "code": record["code"]},
                                  {"$set" : {factor: record[factor], snapshots: record[snapshots]}}))
    with MongoDB() as mongo:
        connection = mongo.connect()
        db = connection.conn["basicdb"]
        collection = db[collection]
        collection.bulk_write(request)



if __name__ == "__main__":

    # Update test 1 record
    confirm()
    start_date = "20090101"
    end_date = "20190831"
    test_mode = False
    # for factor in ["cash", "tradable_financialasset", "notes_receiveable", "accounts_receivable",
    #               "inventory", "fixed_asset", "construction_inprogress", "intangible_asset",
    #               "development_expenditure", "goodwill", "notes_payable", "accounts_payable"]:
    for factor in ["accounts_receivable",
                   "inventory", "fixed_asset", "construction_inprogress", "intangible_asset",
                   "development_expenditure", "goodwill", "notes_payable", "accounts_payable"]:
        tic = dt.datetime.now()
        add_one_factor(factor, "basic_balance", start_date, end_date, test_mode=test_mode)
        toc = dt.datetime.now()
        print(factor + "cost time: ", toc - tic)




    for factor in ["revenue", "total_opcost", "operating_cost", "sale_expense",
                  "management_expense", "research_expense", "financial_expense", "operating_profit"]:
        tic = dt.datetime.now()
        add_one_factor(factor, "basic_income", start_date, end_date, test_mode=test_mode)
        toc = dt.datetime.now()
        print(factor + "cost time: ", toc - tic)




    for factor in ["operating_cashinflow", "operating_cashoutflow", "investment_cashinflow", "investment_cashoutflow",
                 "investment_cashflow", "finance_cashinflow", "finance_cashoutflow", "finance_cashflow"]:
        tic = dt.datetime.now()
        add_one_factor(factor, "basic_cashflow", start_date, end_date, test_mode=test_mode)
        toc = dt.datetime.now()
        print(factor + "cost time: ", toc - tic)

