
import json
from meta_generator.generator import MetaGenerator
from meta_generator.inte_generator import IntegrationGenerator
from influxdb_management.influx_crud import InfluxCRUD
from influxdb_management import influx_setting as ifs
from mongo_management.mongo_crud import MongoCRUD

if __name__=="__main__":
    from pprint import pprint

    with open('./meta_manager/config.json', 'r') as f:
        config = json.load(f)
    
    db_info = config['DB_INFO']
    mydb = MongoCRUD(db_info['USER_ID']\
        ,db_info["USER_PWD"],db_info["HOST_ADDR"]\
        ,db_info["HOST_PORT"],db_info["DB_NAME"])

    with open('./meta_generator/config.json', 'r') as f:
        gener_config = json.load(f)
    
    print("=== exploration metadata ===")
    gener = MetaGenerator(gener_config)
    
    data = {
    "domain" : "Environment",
    "sub_domain" : "Air",
    "have_location" : "True",
    "location" : {
      "syntax" : "경북 상주시 북천로 63 북문동주민센터"
    },
    "description" : "it is the open data ..",
    "source_agency":"Air Korea",
    "collector":"korea government",
    "source_type":"server",
    "method":"csv",
    "number_of_columns":6
    }
    metadata = gener.generate(data)
    collection_name = metadata["domain"]+"_"+metadata["sub_domain"]
    #mydb.insertOne(collection_name,metadata)

    colls = mydb.getCollList()
    print(colls)

    items = mydb.getManyData(collection_name)
    for item in items:
        pprint(item)
        print()
    
    print()
    # === integration metadata ===
    print("=== integration metadata ===")
    import pandas as pd
    import numpy as np

    # === get sample data ===
    db_name = data['domain']+"_"+data['sub_domain']
    test = InfluxCRUD(ifs.host_, ifs.port_, ifs.user_,
                      ifs.pass_, db_name, ifs.protocol)
    measurements = test.get_all_db_measurements()
    mname = measurements[0]['name']
    sample = test.get_df_all(mname)
    gener = IntegrationGenerator(sample)

    mydb.switchDB("integration")
    colls = mydb.getCollList()
    collection_name = db_name
    meta = gener.get_column_meta()
    meta["_id"]=mname
    #mydb.insertOne(collection_name,gener.get_column_meta())

    print("=====Data=====")
    from bson.objectid import ObjectId
    #mydb.deleteOne(collection_name,{'_id':ObjectId('60f63badf229c8ef41279f62')})
    items = mydb.getManyData(collection_name)
    for item in items:
        pprint(item)
        print()