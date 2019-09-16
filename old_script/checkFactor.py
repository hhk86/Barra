import sys
import weakref
import pandas as pd
from jinja2 import Template
from dateutil.parser import parse as dateparse
sys.path.append("D:\\Program Files\\Tinysoft\\Analyse.NET")
import TSLPy3 as ts
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


class TsTickData(object):


    def __enter__(self):
        if ts.Logined() is False:
            print('天软未登陆或客户端未打开，将执行登陆操作')
            self.__tsLogin()
            return self


    def __tsLogin(self):
        ts.ConnectServer("tsl.tinysoft.com.cn", 443)
        dl = ts.LoginServer("fzzqjyb", "fz123456")
        print('天软登陆成功')

    def __exit__(self, *arg):
        ts.Disconnect()
        print('天软连接断开')

    def ticks(self, ticker, date, func):
        ts_sql = '''    
                setsysparam(pn_stock(),'{0}');
                v:={2}(inttodate({1}));
                return v;
                '''.format(ticker, date, func)
        fail, value, _ = ts.RemoteExecute(ts_sql, {})
        return value

    
def compareFactor(factor):
    if factor.startswith("StockPNA"):
        collection_name = "fac_barra_book_to_price"
    elif factor.startswith("StockPE") or factor.startswith("StockPCF"):
        collection_name = "fac_barra_earning_yield"
    with MongoDB() as mongo:
        connection = mongo.connect()
        db = connection.conn["factordb"]
        collection = db[collection_name]
        cursor = collection.find({"trade_date": {"$gt": "20180101"}})
    df = pd.DataFrame(list(cursor))
    if factor.startswith("StockPNA"):
        new_columns = df.columns.tolist()[1:]
        new_columns.reverse()
        df = df[new_columns]
        new_col_name = df.columns[2] + '_tsl'
        df[new_col_name] = None
    elif factor.startswith("StockPE"):
        df = df[["trade_date", "code", "etop"]]
        df["etop_tsl"] = None
    elif factor.startswith("StockPCF"):
        df = df[["trade_date", "code", "cetop"]]
        df["cetop_tsl"] = None

    df = df[(df["code"] < "000005.SZ") | ((df["code"] >= "600000.SH") & (df["code"] < "600005.SH"))]

    with TsTickData() as obj:
        N = df.shape[0]
        value = False
        for i in range(N):
            if i % 10 == 0:
                print(factor, round(i / N * 100, 4), '%', value)
            date = df.iloc[i, 0]
            ticker = df.iloc[i, 1][-2: ] + df.iloc[i, 1][ : 6]
            value = obj.ticks(ticker, date, factor)
            df.iloc[i, 3] = value
    if not factor.startswith("StockPNA"):
        df.iloc[:, 3] = df.iloc[:, 3].apply(lambda x: 1 / x)
    df["diff"] = df.iloc[:, 3] - df.iloc[:, 2]
    df["round_df"] = df["diff"].apply(lambda x: round(x, 4))
    print(df)
    df.to_csv(factor + ".csv")

if __name__ == '__main__':
    pd.set_option("display.max_columns", None)
    # for factor in ["StockPNA_II", "StockPNA", "StockPNA_III", "StockPE", "StockPE_II", "StockPE_III", "StockPE_IV",\
    #                "StockPCF", "StockPCF_III", "StockPCF_IV", "StockPCF_V"]:
    for factor in ["StockPE", "StockPE_II", "StockPE_IV", "StockPCF", "StockPCF_V", "StockPCF_VI"]:
        compareFactor(factor)






    # with TsTickData() as obj:
    #
    #     data = obj.ticks("SZ000002", "20120830", "StockPNA_II")
    # print(data)
