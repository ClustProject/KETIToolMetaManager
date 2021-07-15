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

@app.route("/")
def home():
    items = mydb.getManyData("test")
    return jsonify(ItemstoJson(items))

@app.route("/get_data/<name>")
def getDataOne(name):
    item = mydb.getOneData("test") #,{'x':int(name)})
    return jsonify(ItemstoJson([item]))

@app.route("/get_all_data/<table>")
def getDataAll(table):
    items = mydb.getManyData(table)
    return jsonify(ItemstoJson(items))

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