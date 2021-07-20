from flask import Flask, jsonify
from mongo_management import mongo_crud as mg
import json
import logging

with open('./meta_manager/config.json', 'r') as f:
    config = json.load(f)
    
db_info = config['DB_INFO']
mydb = mg.MongoCRUD(db_info['USER_ID']\
        ,db_info["USER_PWD"],db_info["HOST_ADDR"]\
        ,db_info["HOST_PORT"],db_info["DB_NAME"])

app = Flask(__name__)
app.config['SECRET_KEY']='myProject'
app.config['exp']='exploration'
app.config['int']='integration'
app.config['DB_KEY_LIST']=['exp','int']

@app.route("/")
def home():
    items = mydb.getManyData("test")
    return jsonify(ItemstoJson(items))

@app.route("/<dbKey>/get_data/<table>")
def getDataOne(dbKey,table):
    res = switchDB(dbKey)
    if not res is None:
        return res
    item = mydb.getOneData(table) #,{'x':int(name)})
    return jsonify(ItemstoJson([item]))

@app.route("/<dbKey>/get_all_data/<table>")
def getDataAll(dbKey,table):
    res = switchDB(dbKey)
    if not res is None:
        return res
    items = mydb.getManyData(table)
    return jsonify(ItemstoJson(items))

@app.route("/<dbKey>/all_tables")
def getAllExpTables(dbKey):
    res = switchDB(dbKey)
    if not res is None:
        return res
    return jsonify(mydb.getCollList())

def switchDB(dbKey):
    if dbKey not in app.config['DB_KEY_LIST']:
        return jsonify({"error":"bad DB name"})
    try:
        if not mydb.getDBName==app.config[dbKey]:
            mydb.switchDB(app.config[dbKey])
    except KeyError:
        return jsonify({"error":"bad DB name"})
    return None

def ItemstoJson(items):
    allData   = []
    for item in items:       
        item['_id']=str(item['_id'])
        allData.append(item)
    return allData

def log_setup():
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    termlog_handler = logging.StreamHandler()
    termlog_handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.addHandler(termlog_handler)
    logger.setLevel(logging.INFO)

if __name__=="__main__":
    log_setup()
    app.run()