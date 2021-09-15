"""
meta_read_write
===
the modules 
- generating metadata using CLUST rules 
- writing and reading metadata on mongodb

"""
import sys,os
# add a directory path of this file in python's path
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

import json
from influxdb import InfluxDBClient
from meta_generator.generator import MetaGenerator
from mongo_management.mongo_crud import MongoCRUD

with open(os.path.dirname(os.path.realpath(__file__))+'/config.json', 'r') as f:
        config = json.load(f)

# initialize MetaGenerator, InfluxDB, MongoDB as global values using config
gener = MetaGenerator(config['GENERATOR_INFO'])
ins = config['INFLUX_DB_INFO']
influxdb = InfluxDBClient(host=ins["host_"], port=ins["port_"], username=ins["user_"], password=ins["pass_"])
mydb = MongoCRUD(config['MONGO_DB_INFO'])

exclude_db = ["config","local","admin"]
include_db = ["air", "farm", "factory", "bio", "life", "energy", \
            "weather", "city", "traffic", "culture", "economy", "INNER","OUTDOOR"]

def location_with_table(data):
    data["location"]["syntax"] = data["table_name"]
    return data

def loacation_with_suffix_table(data):
    idx = data["table_name"].find("_")
    data["location"]["syntax"] = data["table_name"][idx+1:]
    return data

def make(data, type=0, locations=None):
    """
    this function makes a list that have all metadata that have common info with a argument "data"
    1. get all measurement names from a db named data["domain"]+"_"+data["sub_domain"] from influxdb
    2. for each measurement, get the table from the influxdb and make a metadata and save it in a list
    3. return that list

    Args:
        data: dictionary
                ex) {
                        "domain": "db_name", (required)
                        "sub_domain": "collection_name", (required)
                        "table_name": "influxdb measurement name", 
                        "location": {
                            "lat": "", 
                            "lng": "", 
                            "syntax": "경상북도"
                        },
                        "description": "description", (required)
                        "source_agency": "agency", (required)
                        "source": "source", (required)
                        "source_type": "source type", (required)
                        "tag": [tags], 
                    } 
        type: int
                0 - use manual input for location syntax or lat,lng (detault) 
                1 - use data["table_name"] (=influxdb measurement name) for location syntax
                2 - use the suffix of data["table_name"] for location syntax

    Returns:
        elements : a list that have all metadata we will saves
    """

    db_name = data["domain"]+"_"+data["sub_domain"]
    elements = []
    if {"name": db_name} in influxdb.get_list_database():
        influxdb.switch_database(db_name)
        ms_list = influxdb.get_list_measurements()
        print(ms_list)
        
        for ms in ms_list:
            print("=================")
            data["table_name"]=ms["name"]
            if(type==0 and locations is not None):
                data["location"] = locations[data["table_name"]]
            if(type==1): data = location_with_table(data.copy())
            if(type==2): data = loacation_with_suffix_table(data.copy())
            
            metadata = gener.generate(data.copy(),influxdb)
            print(metadata)
            elements.append(metadata)
    return elements

def make_one(data):
    """
    this function make a list that only have a metadata generated using a argument "data"
    it uses a manual input for location syntax or lat,lng (detault) 

    Args:
        data: dictionary

    Returns:
        elements : a list that have a metadata we will saves
    """
    db_name = data["domain"]+"_"+data["sub_domain"]
    metadata = gener.generate(data.copy(),influxdb)
    return [metadata]

def read_all_db_coll_list():
    """
    print all db_names and measurements names and return it
    
    Returns:
        result : a dictionary which key is a db name and value is a list of measurement names
    """
    result = {}
    db_list = mydb.getDBList()
    for db_name in db_list:
        if db_name not in exclude_db and db_name in include_db:
            result[db_name]=[]
            mydb.switchDB(db_name)
            
            for coll_name in mydb.getCollList():
                result[db_name].append(coll_name)
    import pprint
    pprint.pprint(result)
    return result

def read_coll_list(db_name):
    """
    retrun all measurement list on a db
    
    Returns:
        result : a list
    """
    if db_name not in exclude_db and db_name in include_db:
        mydb.switchDB(db_name)
        return mydb.getCollList()
    return "Wrong DB name"

def read_db_coll(db_name,collection_name):
    """
    retrun a list of all metadatas in a collection on a db
    
    Returns:
        result : a list of all metadatas
    """
    if db_name not in exclude_db and db_name in include_db:
        mydb.switchDB(db_name)
        for coll in mydb.getCollList():
            if(coll==collection_name):
                return ItemstoJson(mydb.getManyData(collection_name))
        return "There is no collection named "+collection_name+" in "+db_name
    return "Wrong DB name"

def ItemstoJson(items):
    allData   = []
    for item in items:       
        item['_id']=str(item['_id'])
        allData.append(item)
    return allData

'''
def make_all_unique_index(unique_index_col):
    db_list = mydb.getDBList()
    for db_name in db_list:
        if db_name not in exclude_db and db_name in include_db:
            print()
            print(db_name)
            mydb.switchDB(db_name)
            print("=========ms=========")
            for coll_name in mydb.getCollList():
                print(coll_name)
                try:
                    mydb.create_unique_index(coll_name,unique_index_col)
                except Exception as e:
                    print(e)
    return "OK"
'''

if __name__=="__main__":
    
    data = {
    "domain": "OUTDOOR", 
    "sub_domain": "WEATHER_CLEAN", 
    "table_name": "sangju", 
    "location": {
      "lat": "", 
      "lng": "", 
      "syntax": "경상북도 상주시 남산2길 322 상주지역기상서비스센터"
    },
    "description": "This is weater data ", 
    "source_agency": "AirKorea", 
    "source": "Server", 
    "source_type": "CSV", 
    "tag": ["weather", "outdoor", "sangju"], 
    "start_time": "", 
    "end_time": "", 
    "frequency": "", 
    "number_of_columns": ""
    }  

    # manual location (covid, kweather, INNER_AIR)
    #elements = make(data)
    # manual location for only one table (OUTDOOR_WEATHER, OUTDOOR_AIR)
    #elements = make_one(data)
    # location_with_table (energy_solar, energy_windpower)
    #elements = make(data,1)
    # location_with_suffix_table (traffic_subway)
    # elements = make(data,2)

    #print(elements)
    #mydb.switchDB(data["domain"])
    #mydb.deleteDB("OUTDOOR")
    #mydb.create_unique_index(data["sub_domain"],"table_name")
    #mydb.deleteCollection(data["sub_domain"])
    #mydb.insertMany(data["sub_domain"],elements,"table_name")

    #print(make_all_unique_index("table_name"))
    print("===all db and collections===")
    res = read_all_db_coll_list()
    print("===all collection list on a db")
    res = read_coll_list("bio")
    print(res)
    res = read_db_coll("bios","covid")
    print(res)
    