from math import *
from pymongo import MongoClient
import matplotlib.pyplot as plt
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
        print("connect to Mongodb")
        self.conn = MongoClient(self.host, self.port, username=self.username, password=self.password)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def connect(self):
        return self



if __name__ == "__main__":
    # with MongoDB() as mongo:
    #     connection = mongo.connect()
    #     db = connection.conn["basicdb"]
    #     collection = db["basic_capital"]
    #     cursor = collection.find({}, {"trade_date":1, "code": 1, "float_mv":1})
    # capital_df = pd.DataFrame(list(cursor))
    # capital_df.drop("_id", axis=1, inplace=True)
    # capital_df.to_csv("capital_data_all.csv", index=None)

    # with MongoDB() as mongo:
    #     connection = mongo.connect()
    #     db = connection.conn["factordb"]
    #     collection = db["fac_barra_beta"]
    #     cursor = collection.find({}, {"trade_date":1, "code": 1, "beta":1})
    # beta_df = pd.DataFrame(list(cursor))
    # beta_df.drop("_id", axis=1, inplace=True)
    # beta_df.to_csv("beta_data_all.csv", index=None)



    beta_df = pd.read_csv("beta_data_all.csv", dtype={"trade_date": str})
    capital_df = pd.read_csv("capital_data_all.csv", dtype={"trade_date": str})
    whole_df = pd.merge(beta_df, capital_df, on=["trade_date", "code"])
    whole_df["sqrt_cap"] = whole_df["float_mv"].apply(sqrt)
    date_list = sorted(list(set(whole_df["trade_date"].tolist())))
    data = pd.DataFrame()
    # for date in date_list:
    #     df = whole_df[whole_df["trade_date"] == date]
    #     df["wsum"] = df["beta"].mul(df["sqrt_cap"])
    #     pd.set_option("display.max_columns", None)
    #     wavg = df["wsum"].sum()/df["sqrt_cap"].sum()
    #     print(date, "weighted average Beta:" ,wavg)
    #     data = data.append([[date, wavg],])
    # print(data)
    # data.to_csv("data.csv", index=None)
    # plt.plot(data["beta"].tolist())
    # plt.savefig("beta.png")
    # plt.close()
    for date in date_list:
        df = whole_df[whole_df["trade_date"] == date]
        df["wsum"] = df["beta"].mul(df["float_mv"])
        pd.set_option("display.max_columns", None)
        wavg = df["wsum"].sum()/df["float_mv"].sum()
        print(date, "weighted average Beta:" ,wavg)
        data = data.append([[date, wavg],])
    print(data)
    data.to_csv("data2.csv", index=None)
    plt.plot(data["beta"].tolist())
    plt.savefig("beta.png")
    plt.close()