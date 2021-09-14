import functools
from time import monotonic

from flask import(
    Blueprint, flash, g, redirect, render_template, request, session, url_for, jsonify
)
from flask.globals import current_app
from KETIToolMetaManager.meta_generator.generator import MetaGenerator
from .db import get_influx_db, get_mongo_db

bp = Blueprint('meta_data',__name__,url_prefix='/meta')
#generator = MetaGenerator(current_app.config["GENERATOR_INFO"])

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
        generator = MetaGenerator(current_app.config["GENERATOR_INFO"])
        new_data = request.get_json()
        collection_name = new_data["sub_domain"]
        metadata = generator.generate(new_data.copy(),get_influx_db())
        return metadata
        #mongodb.insertOne(collection_name,metadata)
    elif request.method == 'GET':
        db_name = request.args["domain"]
        collection_name = request.args["sub_domain"]
        mongodb.switchDB(db_name)
        items = mongodb.getManyData(collection_name)
        return jsonify(ItemstoJson(items))

    return "AA"

def ItemstoJson(items):
    allData   = []
    for item in items:       
        item['_id']=str(item['_id'])
        allData.append(item)
    return allData



