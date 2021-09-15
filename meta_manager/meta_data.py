from enum import unique
import functools
from time import monotonic
import json
from flask import(
    Blueprint, flash, g, redirect, render_template, request, session, url_for, jsonify
)
from flask.globals import current_app
from KETIToolMetaManager.meta_generator.generator import MetaGenerator
from .db import get_influx_db, get_mongo_db

bp = Blueprint('meta_data',__name__,url_prefix='/meta')
#generator = MetaGenerator(current_app.config["GENERATOR_INFO"])

include_db = ["air", "farm", "factory", "bio", "life", "energy", \
                "weather", "city", "traffic", "culture", "economy", "INNER","OUTDOOR"]
unique_col_name = "table_name"

@bp.route('/get_all_dbs', methods=('GET','POST'))
def get_dbs():
    mongodb = get_mongo_db()
    error = None
    #print(db.getDBName())
    return str(mongodb.getDBList())

@bp.route('/metadata', methods=('GET','POST'))
def register():
    '''
    {
        "domain" : "air",
        "sub_domain" : "indoor_경로당",
        "table_name" : "ICL1L2000234",
        "location" :{
        "lat" : "",
        "lng" : "",
        "syntax" : ""
        },
        "description" : "This is weather data from kweather ",
        "source_agency" : "kweather",
        "source" : "None",
        "source_type" : "csv",
        "tag" : [
            "wheather",
            "경로당",
            "indoor",
            "air"
        ]
    }
    '''
    mongodb = get_mongo_db()
    if request.method == 'POST':
        new_data = request.get_json()
        if new_data["domain"] not in include_db:
            return "Domain name is false"
        generator = MetaGenerator(current_app.config["GENERATOR_INFO"])
        
        collection_name = new_data["sub_domain"]
        metadata = generator.generate(new_data.copy(),get_influx_db())
        
        mongodb.switchDB(metadata["domain"])
        mongodb.insertOne(collection_name, metadata, unique_col_name)
        return "OK"
       
    elif request.method == 'GET':
        db_name = request.args["domain"]
        if db_name not in include_db:
            return "Domain name is false"
        collection_name = request.args["sub_domain"]
        mongodb.switchDB(db_name)
        items = mongodb.getManyData(collection_name)
        return jsonify(ItemstoJson(items))

    return "AA"

@bp.route('/metadatas', methods=('GET','POST'))
def register_all():
    mongodb = get_mongo_db()
    if request.method == 'POST':
        generator = MetaGenerator(current_app.config["GENERATOR_INFO"])
        new_datas = request.get_json()["data"]
        elements = {}
        for new_data in new_datas:
            collection_name = new_data["sub_domain"]
            metadata = generator.generate(new_data.copy(),get_influx_db())
            if(collection_name in elements):
                elements[collection_name].append(metadata)
            else:
                elements[collection_name]=[metadata]
            #elements.append(metadata)
        for collection_name in elements.keys():
            #print(collection_name)
            mongodb.insertMany(collection_name,elements[collection_name],unique_col_name)
        
        return "OK"

    elif request.method == 'GET':
        db_name = request.args["domain"]
        mongodb.switchDB(db_name)
        return str(mongodb.getCollList())

def ItemstoJson(items):
    allData   = []
    for item in items:       
        item['_id']=str(item['_id'])
        allData.append(item)
    return allData



