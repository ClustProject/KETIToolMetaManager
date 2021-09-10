import json
from meta_generator.generator import MetaGenerator
from mongo_management.mongo_crud import MongoCRUD
from influxdb import InfluxDBClient

if __name__=="__main__":
    import pprint
    with open('KETIToolMetaManager/config.json', 'r') as f:
        config = json.load(f)

    gener = MetaGenerator(config['GENERATOR_INFO'])
    ins = config['INFLUX_DB_INFO']
    db = InfluxDBClient(host=ins["host_"], port=ins["port_"], username=ins["user_"], password=ins["pass_"])
    
    new_data = {
        "domain" : "air",
        "sub_domain" : "indoor_어린이집",
        "table_name" : "ICW0W2000638",
        "location" :{
        "lat" : "35.21388",
        "lng" : "129.08397",
        "syntax" : ""
        },
        "description" : "This is weather data from kweather ",
        "source_agency" : "kweather",
        "source" : "None",
        "source_type" : "csv",
        "tag" : [
            "wheather",
            "어린이집",
            "indoor",
            "air"
        ]
    }
    db_info = config['MONGO_DB_INFO']
    db_info['DB_NAME'] = new_data['domain']
    mydb = MongoCRUD(db_info)

    collection_name = new_data["sub_domain"]
    locations = config["locations"]
    
    count = 0
    elements=[]
    print(len(locations))
    common_name = "ICL1L20002"
    for i in range(34,34+1):
        #new_data["table_name"]=common_name+str(i)
        #mydb.deleteOne(collection_name,{"table_name":new_data["table_name"]})
        print(new_data["table_name"]+" Start")
        #new_data["location"]["syntax"]=locations[i-34]
        metadata = gener.generate(new_data.copy(),db)
        pprint.pprint(metadata)
        elements.append(metadata.copy())
    #print(elements)
    #mydb.insertMany(collection_name,elements)
        
    colls = mydb.getCollList()
    print(colls)
        
    #mydb.insertOne(collection_name,metadata)
    #mydb.deleteOne(collection_name,{"table_name":"ICL1L2000234"})
    #mydb.insertOne("test",{"test":"test"})
    #print(type(metadata))

    