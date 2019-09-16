import sys
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
        self.conn = MongoClient(self.host, self.port, username=self.username, password=self.password)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def connect(self):
        return self


if __name__ == "__main__":
    confirm = input("Please input confirm:\n>>>")
    if confirm != "confirm":
        sys.exit()
    with MongoDB() as mongo:
        connection = mongo.connect()
        db = connection.conn["basicdb"]
        collection = db["basic_balance_new"]
        db.basic_balance_new.rename("basic_balance")
        # a = db.universe_new2.find()
        # for b in a:
        #     print(b)