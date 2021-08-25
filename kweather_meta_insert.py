import json
from meta_generator.generator import MetaGenerator
from meta_generator.inte_generator import IntegrationGenerator
from influxdb_management.influx_crud import InfluxCRUD
from influxdb_management import influx_setting as ins
from mongo_management.mongo_crud import MongoCRUD
from influxdb import InfluxDBClient, DataFrameClient

if __name__=="__main__":
    import pprint
    with open('./config.json', 'r') as f:
        config = json.load(f)

    gener = MetaGenerator(config['GENERATOR_INFO'])
    ins = config['INFLUX_DB_INFO']
    db = InfluxDBClient(host=ins["host_"], port=ins["port_"], username=ins["user_"], password=ins["pass_"])
    common_name = "ICL1L20002"
    new_data = {
        "domain" : "air",
        "sub_domain" : "indoor_경로당",
        "table_name" : "ICL1L20002",
        "location" :{
        "lat" : "None",
        "lng" : "None",
        "syntax" : ""
        },
        "description" : "This is weather data, ",
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
    db_info = config['MONGO_DB_INFO']
    db_info['DB_NAME'] = new_data['domain']
    mydb = MongoCRUD(db_info)

    collection_name = new_data["sub_domain"]
    # locations = [
        
    # ]
    locations = [
        "원주시 소초면 황골로 172",
        "원주시 신림황둔로 1214",
        "원주시 호저로 200-1",
        "원주시 유문1길 5-2",	
        "원주시 봉화로 67",	
        "원주시 우산공단길 23",	
        "원주시 북원상가길 13",
        "원주시 판부면 한여마을길 30-2",
        "원주시 섭재삼보길 40-15",	
        "원주시 호저로 933",
        "원주시 가매기길 17-2",	
        "원주시 행구로 279",	
        "원주시 시청로 64",
        "원주시 동진골3길 16",
        "원주시 예술관길 31",#(명륜동, 명륜2단지)	
        "원주시 번재길 217"	,
        "원주시 무실로55번길 13-3",#(원동)	
        "원주시 치악로 1803",	
        "원주시 옛시청길 11",	
        "원주시 충정길 5"
    ]
    count = 0
    
    elements=[]
    print(len(locations))
    for i in range(34,34+len(locations)):
        new_data["table_name"]=common_name+str(i)
        #mydb.deleteOne(collection_name,{"table_name":new_data["table_name"]})
        print(new_data["table_name"]+" Start")
        new_data["location"]["syntax"]=locations[i-34]
        metadata = gener.generate(new_data.copy(),db)
        pprint.pprint(metadata)
        elements.append(metadata.copy())
    print(elements)
    #mydb.insertMany(collection_name,elements)
        
    colls = mydb.getCollList()
    print(colls)
        
    #mydb.insertOne(collection_name,metadata)
    #mydb.deleteOne(collection_name,{"table_name":"ICL1L2000234"})
    #mydb.insertOne("test",{"test":"test"})
    #print(type(metadata))

    