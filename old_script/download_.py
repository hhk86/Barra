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

    with MongoDB() as mongo:
        connection = mongo.connect()
        db = connection.conn["universedb"]
        collection = db["universe"]
        cursor = collection.find()
    df = pd.DataFrame(list(cursor))
    print(df)
    df.to_csv("backupdata.csv", encoding='gbk', index=None)





# with mongo as mongo:
#     connection = mongo.connect()
#     db = connection.conn["basicdb"]
#     collection = db["basic_balance_new"]
#     collection.rename('basic_balance')