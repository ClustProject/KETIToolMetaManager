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

    def checkField(self, collection,table_name, field):
        #{"field_to_check_for": {"$exists": True}}
        condition = {"table_name":table_name,field:{"$exists":True}} 
        res = list(self.db[collection].find(condition))
        return len(res)

    # Insert
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
    
    # Update
    def updateKey(self,collection,select_condition,update_data):
        # 조건에 해당하는 첫번째 Document만 변경 
        # $set:{'midExamDetails.$.Marks': 97}
        return self.db[collection].update_one(select_condition, { '$set': update_data })
    
    def updateManyKey(self,collection,select_condition,update_data):
        # 조건에 해당하는 모두를 변경 
        return self.db[collection].update_many(select_condition, { '$set': update_data })
    
    def updateKeyToArray(self,collection,select_condition,update_data):
        # { $addToSet: { tags: "accessories" }
        return self.db[collection].update_many(select_condition, { '$addToSet': update_data })

    def updateManyKeyToArray(self,collection,select_condition,update_data):
        # { "tags": [ "camera", "electronics", "accessories" ] }
        # { $addToSet: { tags: { $each: [ "camera", "electronics", "accessories" ] } } }
        for key in update_data :
            val = update_data[key]
            if(type(val) is list):
                new_val = {"$each":val}
                update_data[key] = new_val
        print(update_data)
        return self.db[collection].update_many(select_condition, { '$addToSet': update_data })

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

    # print
    def printData(self, db_name, collection_name):
        mydb.switchDB(db_name)
        items = mydb.getManyData(collection_name)
        for item in items:
            print(item)
    
if __name__=="__main__":
    import json
    with open('./config.json', 'r') as f:
        config = json.load(f)
    
    db_info = config['MONGO_DB_INFO']
    mydb = MongoCRUD(db_info)
    
    dbs = mydb.getDBList()
    #print(dbs)

    data = {
        "name" : "ION",
        "age" : 45,
        "favorite" : "ON",
        "tag" : ["love","outgoing"],
        "loca":{
            "syn":"test",
            "lat":32
        },
        "table_name":"test7",
        "domain":"test"
    }
    #mydb.deleteDB("test")
    mydb.switchDB("test2")
    #mydb.insertOne("test2",data)
    # colls = mydb.getCollList()
    # print(colls)
    #mydb.printData("test2","test2")
    check = mydb.checkField("test2", "test2","name")
    print(check)
    '''
    mydb.printData("test","test")
    # #mydb.insertOne("test",data)
    # {'midExamDetails.$.Marks': 97}
    mydb.updateKey("test",{ 'name': 'ION','loca.syn':'test'},{"loca.syn":"USA" })
    #mydb.updateKey("test",{ 'name': 'Donghee'},{ 'tag.1' : "Hot"})
    #mydb.updateKey("test",{ 'name': 'Donghee'},{ 'tag.6' : "Hot2"})
    #mydb.updateKeyToArray("test",{ 'name': 'Donghee'},{ 'tag' : "Cool2"})
    #mydb.updateKeyToArray("test",{ 'name': 'Donghee'},{ 'tag' : { "$each": [ "camera", "electronics", "accessories" ] }})

    mydb.updateManyKeyToArray("test",{ 'name': 'ION'},{ 'tag' : [ "camera", "electronics", "accessories" ] })
    # { "$each": [ "camera", "electronics", "accessories" ] }
    # { 'tag.1.content' : "Hot"}
    # mydb.updateManyKey("test",{ 'name': 'test'},{ 'age': 12,"favorite" : "milk","hobby":"basketball"} )
    print("after update")
    mydb.printData("test","test")'''

    
    
    