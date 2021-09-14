import json
from influxdb import InfluxDBClient
import sys,os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from meta_generator.generator import MetaGenerator
from mongo_management.mongo_crud import MongoCRUD

with open(os.path.dirname(os.path.realpath(__file__))+'/config.json', 'r') as f:
        config = json.load(f)

gener = MetaGenerator(config['GENERATOR_INFO'])
ins = config['INFLUX_DB_INFO']
db = InfluxDBClient(host=ins["host_"], port=ins["port_"], username=ins["user_"], password=ins["pass_"])
mydb = MongoCRUD(config['MONGO_DB_INFO'])

def location_with_table(data):
    data["location"]["syntax"] = data["table_name"]
    return data

def loacation_with_suffix_table(data):
    idx = data["table_name"].find("_")
    data["location"]["syntax"] = data["table_name"][idx+1:]
    return data

def make(data, type=0):
    db_name = data["domain"]+"_"+data["sub_domain"]
    elements = []
    if {"name": db_name} in db.get_list_database():
        db.switch_database(db_name)
        ms_list = db.get_list_measurements()
        print(ms_list)
        
        for ms in ms_list:
            print("=================")
            data["table_name"]=ms["name"]
            if(type==1): data = location_with_table(data.copy())
            if(type==2): data = loacation_with_suffix_table(data.copy())
            
            metadata = gener.generate(data.copy(),db)
            print(metadata)
            elements.append(metadata)
    return elements

def make_one(data):
    db_name = data["domain"]+"_"+data["sub_domain"]
    metadata = gener.generate(data.copy(),db)
    return [metadata]

if __name__=="__main__":
    
    data = {
     "domain": "OUTDOOR", 
    "sub_domain": "AIR_CLEAN", 
    "table_name": "seoul", 
    "location": {
      "lat": "", 
      "lng": "", 
      "syntax": "서울 마포구 포은로 6길 10 망원1동주민센터 옥상"
    },
    "description": "This is outdoor air data ", 
    "source_agency": "AirKorea", 
    "source": "Server", 
    "source_type": "XLS", 
    "tag": ["outdoor", "air", "seoul"], 
    "start_time": "", 
    "end_time": "", 
    "frequency": "", 
    "number_of_columns": ""
    }  

    # manual location (covid, kweather, INNER_AIR)
    #elements = make(data)
    # manual location for only one table (OUTDOOR_WEATHER, OUTDOOR_AIR)
    elements = make_one(data)
    # location_with_table (energy_solar, energy_windpower)
    #elements = make(data,1)
    # location_with_suffix_table (traffic_subway)
    # elements = make(data,2)

    #print(elements)
    mydb.switchDB(data["domain"])
    #mydb.deleteDB("bio")
    mydb.create_unique_index(data["sub_domain"])
    #mydb.insertMany(data["sub_domain"],elements)