from pymongo import MongoClient
import parameters.Globals as G


class Mongo:

    def __init__(self):
        self.user = G.MDB_USER_1
        self.password = G.MDB_PASS_USER_1

    def newUser(self, user_, password_):
        self.user = user_
        self.password = password_

    def connectMongoClient(self, host_, database_):
        client = MongoClient(host_,
                             username=self.user,
                             password=self.password,
                             authSource=database_)

        return client
