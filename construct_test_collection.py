from pymongo import MongoClient
from basicFunction import *
import sys


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
    # # Get data slice from mongo and save it in local.
    # with MongoDB() as mongo:
    #     connection = mongo.connect()
    #     db = connection.conn["basicdb"]
    #     collection = db["basic_balance"]
    #     cursor = collection.find({"trade_date": {"$lt":"20090201"}}, )
    # test_balance_data = pd.DataFrame(list(cursor))
    # test_balance_data.drop("_id", axis=1, inplace=True)
    # test_balance_data.to_csv("test_balance_data.csv", index=None)
    #
    # # Write local data slice to MongoDB.
    # data = test_balance_data.to_dict("records")
    # confirm = input("Please input confirm\n>>>")
    # if confirm != "confirm":
    #     sys.exit()
    # with MongoDB() as mongo:
    #     connection = mongo.connect()
    #     db = connection.conn["testdb"]
    #     collection = db["basic_balance_test"]
    #     collection.insert_many(data)





    with MongoDB() as mongo:
        connection = mongo.connect()
        db = connection.conn["basicdb"]
        collection = db["basic_balance"]
        cursor = collection.find({"trade_date": {"$lt":"20090201"}}, )
    test_balance_data = pd.DataFrame(list(cursor))
    test_balance_data.drop("_id", axis=1, inplace=True)
    test_balance_data.to_csv("test_balance_data.csv", index=None)

    # Write local data slice to MongoDB.
    data = test_balance_data.to_dict("records")
    confirm = input("Please input confirm\n>>>")
    if confirm != "confirm":
        sys.exit()
    with MongoDB() as mongo:
        connection = mongo.connect()
        db = connection.conn["testdb"]
        collection = db["basic_balance_test"]
        collection.insert_many(data)

