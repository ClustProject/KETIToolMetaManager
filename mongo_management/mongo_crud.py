"""This module defines an customized MongoDB CRUD object.

CRUD
: Connect, Read, Update, Delete

  Typical usage example:

  mydb = MongoCRUD(db_info)
  data = mydb.getCollList()
"""
import pymongo

class MongoCRUD:
    
    def __init__(self, infoDict=None):
        if(infoDict is None):
            self.userId = "test"
            self.userPwd = "test"
            self.host = "localhost"
            self.port = 27017
            self.dbName = "test"
            self.db = self.connectDB()
        else:
            self.userId = infoDict['USER_ID']
            self.userPwd = infoDict['USER_PWD']
            self.host = infoDict['HOST_ADDR']
            self.port = infoDict['HOST_PORT']
            self.dbName = infoDict['DB_NAME']
            self.db = self.connectDB()

    # Connect
    def connectDB(self):
        # self.conn = pymongo.MongoClient("mongodb://"+self.userId+\
        #     ":"+self.userPwd+"@"+self.host+\
        #         ":"+str(self.port)+"/"+self.dbName)
        self.conn= pymongo.MongoClient(host=self.host,
                         port=int(self.port),
                         username=self.userId,
                         password=self.userPwd,
                        authSource="admin")
        return self.conn.get_database(self.dbName)

    # Disconnect
    def close(self):
        self.conn.close()

    # Switch Database
    def switchDB(self,dbName):
        self.dbName = dbName
        self.db = self.conn.get_database(self.dbName)
    
    def create_unique_index(self,collection, unique_col_name):
        self.db[collection].create_index(unique_col_name, unique=True)

    # Get current's DB name
    def getDBName(self):
        return self.db
    
    # Read
    def getDBList(self):
        return self.conn.list_database_names()

    def getCollList(self):
        return self.db.list_collection_names()

    def getOneData(self, collection, condition=None):
        return self.db[collection].find_one(condition)

    def getManyData(self, collection, condition=None):
        return self.db[collection].find(condition)

    # Update
    def insertOne(self, collection, data, unique_col_name=None):
        try:
            if unique_col_name==None:
                return self.db[collection].insert_one(data)
            else :
                self.create_unique_index(collection,unique_col_name)
                return self.db[collection].insert_one(data)
        except Exception as e :
            return e
    
    def insertMany(self, collection, data, unique_col_name=None):
        try:
            if unique_col_name==None:
                return self.db[collection].insert_many(data)
            else:
                self.create_unique_index(collection,unique_col_name)
                return self.db[collection].insert_many(data)
        except Exception as e :
            return e
    
    def updateOne(self,collection,ori_data,new_data):
        return self.db[collection].update_one(ori_data, new_data)
    
    def updateMany(self,collection,ori_data,new_data):
        return self.db[collection].update_many(ori_data, new_data)

    # Delete
    def deleteOne(self,collection,condition):
        return self.db[collection].delete_one(condition)
    
    def deleteMany(self,collection,condition):
        return self.db[collection].delete_many(condition)

    def deleteDB(self, db_name):
        return self.conn.drop_database(db_name)
    
    def deleteCollection(self, collection):
        return self.db[collection].drop()


if __name__=="__main__":
    import json
    with open('./config.json', 'r') as f:
        config = json.load(f)
    
    db_info = config['MONGO_DB_INFO']
    mydb = MongoCRUD(db_info)
    
    dbs = mydb.getDBList()
    print(dbs)

    colls = mydb.getCollList()
    print(colls)
    collection_name = colls[0]
    
    items = mydb.getManyData(collection_name)
    for item in items:
        print(item)
    
    #mydb.deleteDB("air")
    dbs = mydb.getDBList()
    print(dbs)