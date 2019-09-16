from pymongo import MongoClient
from make_financial_factor import *
from basicFunction import *


class MongoDB():
    '''
    Connect to local MongoDB
    '''
    def __init__(self):
        self.host = "localhost"
        self.port = 27017
        self.db = "huang"

    def __enter__(self):
        self.conn = MongoClient(self.host, self.port)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def connect(self):
        return self


if __name__ =="__main__":
    df = pd.read_csv("debug2.csv", encoding="gbk", nrows=1000)
    df.suspend_type = df.suspend_type.apply(lambda s: None if pd.isnull(s) else s)
    df.suspend_reason_code = df.suspend_reason_code.apply(lambda s: None if pd.isnull(s) else s)
    df.suspend_reason = df.suspend_reason.apply(lambda s: None if pd.isnull(s) else s)
    df.ST_type = df.ST_type.apply(lambda s: None if pd.isnull(s) else s)
    pd.set_option("display.max_columns", None)
    print(df)
    data = df.to_dict("record")
    with MongoDB() as mongo:
        connection = mongo.connect()
        db = connection.conn["huang"]
        collection = db["null2"]
        collection.insert_many(data)
