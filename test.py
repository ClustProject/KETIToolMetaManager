
import sys, json
from meta_generator.generator import MetaGenerator
from mongo_management.mongo_crud import MongoCRUD

if __name__=="__main__":
    with open('./meta_manager/config.json', 'r') as f:
        config = json.load(f)
    
    db_info = config['DB_INFO']
    mydb = MongoCRUD(db_info['USER_ID']\
        ,db_info["USER_PWD"],db_info["HOST_ADDR"]\
        ,db_info["HOST_PORT"],db_info["DB_NAME"])

    with open('./meta_generator/config.json', 'r') as f:
        gener_config = json.load(f)
    
    gener = MetaGenerator(gener_config)
    
    data = {
        "domain" : "Environment",
        "sub_domain" : "Weather",
        "have_location" : "True",
        "location" : {
            "lat" : 36.40837,
            "lng" : 128.15741,
            "syntax" : "경상북도 상주시 남산2길 322 상주지역기상서비스센터",
        },
        "description" : "it is the open data ..",
        "source_agency":"Air Korea",
        "collector":"기상청 대구지방기상청 관측과",
        "source_type":"server",
        "method":"api",
        "number_of_columns":6,
        }
    metadata = gener.generate(data)
    collection_name = metadata["domain"]+"_"+metadata["sub_domain"]
    #mydb.insertOne(collection_name,metadata)

    colls = mydb.getCollList()
    print(colls)

    items = mydb.getManyData(collection_name)
    for item in items:
        print(item)
    
    