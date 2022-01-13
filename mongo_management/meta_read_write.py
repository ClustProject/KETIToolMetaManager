"""
meta_read_write , Test Code by Miseon
===
the modules 
- generating metadata using CLUST rules 
- writing, updating and reading metadata on mongodb

function names
- location_* : make location syntax using table_name
- make* : make metadata 
- write* : wrtie data to mongodb
- update* : modify or append infomation of some metadata
- read* : read data from mongodb
"""
import sys,os
# add a directory path of this file in python's path
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(".")

import json
from influxdb import InfluxDBClient
from mongo_management.meta_generator import MetaGenerator
from mongo_management.mongo_crud import MongoCRUD
from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins

# with open(os.path.dirname(os.path.realpath(__file__))+'/config.json', 'r') as f:
#         config = json.load(f)

# initialize MetaGenerator, InfluxDB, MongoDB as global values using config
#gener = MetaGenerator(config['GENERATOR_INFO'])
try:
    gener = MetaGenerator(ins.GENERATOR_INFO)
except AttributeError:
    gener = MetaGenerator()


CLUSTDataServer= ins.CLUSTDataServer
influxdb = InfluxDBClient(host=CLUSTDataServer.host, port=CLUSTDataServer.port, username=CLUSTDataServer.user, password=CLUSTDataServer.password)
mydb = MongoCRUD(ins.CLUSTMetaInfo)

unique_index_name = "table_name"
exclude_db = ["config","local","admin"]
include_db = ["air", "farm", "factory", "bio", "life", "energy", \
            "weather", "city", "traffic", "culture", "economy", "INNER","OUTDOOR", "test"]

def location_with_table(data):
    data["location"]["syntax"] = data[unique_index_name]
    return data

def loacation_with_suffix_table(data):
    idx = data[unique_index_name].find("_")
    data["location"]["syntax"] = data[unique_index_name][idx+1:]
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
        
        locations : dictionary (option)
                for mapping measurement with location information
                ex) {
                        "table1":{
                            "lat":127,"lng":36
                        },
                        "table2":{
                            "syntax":"seoul"
                        }
                    }

    Returns:
        elements : a list that have all metadata we will saves
    """
    db_name = data["domain"]+"_"+data["sub_domain"]
    elements = []
    if {"name": db_name} in influxdb.get_list_database():
        influxdb.switch_database(db_name)
        ms_list = influxdb.get_list_measurements()
        
        for ms in ms_list:
            data["table_name"]=ms["name"]
            if(type==0 and locations is not None):
                data["location"] = locations[data["table_name"]]
            if(type==1): data = location_with_table(data.copy())
            if(type==2): data = loacation_with_suffix_table(data.copy())
            
            metadata = gener.generate(data.copy(),influxdb)
            elements.append(metadata)
    return elements

def make_one(data):
    """
    this function make a list that only have a metadata generated 
    using a argument "data" without modifying table_name
    it uses a manual input for location syntax or lat,lng (detault) 

    Args:
        data: dictionary

    Returns:
        elements : a list that have a metadata we will save if the measurement exists
        if not, it returns None
    """
    metadata = gener.generate(data.copy(),influxdb)
    if(metadata is None) : return None
    return [metadata]

def write_metadata_to_mongo(db_name,collection_name,elements,unique_col_name=unique_index_name):
    """
    this function writes all metadata in mongodb

    Args:
        db_name: string
        collection_name: string
        elements: list
        unique_col_name: string (unique index column name)

    Returns:
        status message 
        return 'errmsg': 'E11000 duplicate key error collection: .. ' 
                if the table name is already exists
    """
    try:
        mydb.switchDB(db_name)
        return mydb.insertMany(collection_name,elements,unique_col_name)
    except Exception as e:
        return e

def run_and_save(data,type=0,locations=None):
    """
    make a list of metadata and save it to mongo db

    Args:
        data : dictionary
        type : int (-1,0,1,2)
        location : dictionary (option for type=0)

    Returns:
        status message 
        return 'errmsg': 'E11000 duplicate key error collection: .. ' 
                if the table name is already exists
    """
    if(type==-1): elements = make_one(data)
    else : elements = make(data,type,locations)
    return write_metadata_to_mongo(data["domain"],data["sub_domain"],elements,unique_col_name=unique_index_name)

def check_field(db_name,collection,table_name,field):
    """
    check if a field exists in a specific document

    Args:
        db_name : string
        collection : string
        table_name : string
        filed : string

    Returns:
        Boolean
    """
    mydb.switchDB(db_name)
    res = mydb.checkField(collection,table_name, field)
    return True if res>0 else False

def update_metadata(db_name,collection, table_name, update_data):
    """
    update metadata where table_name is same with the parameter "table_name"
    modify value if key already exists,
    append value if key not already exists.
    if key is "tag" : it will be updated using update_tag function

    Args:
        db_name : string
        collection : string
        table_name : string
        update_data : dictionary ex) {"frequency":30,"location.syntax":"sangju", "tag":["hot"]}
    """
    mydb.switchDB(db_name)
    select_condition = {unique_index_name:table_name}
    if "tag" in update_data:
        update_tag(db_name,collection,table_name,update_data["tag"])
        del update_data["tag"]
    return mydb.updateKey(collection,select_condition,update_data)

def update_tag(db_name,collection,table_name,update_data):
    """
    update only array type key(tag) where table_name is same with the parameter "table_name"
    append value if key not already exists in the array.

    Args:
        db_name : string
        collection : string
        table_name : string
        update_data : string or list ex) "food" or ["food","medicine"] 
    """
    mydb.switchDB(db_name)
    select_condition = {unique_index_name:table_name}
    if(type(update_data) is list):
        return mydb.updateManyKeyToArray(collection,select_condition,{"tag":update_data})
    else:
        return mydb.updateKeyToArray(collection,select_condition,{"tag":update_data})

def update_many_metadata(db_name,collection,select_condition,update_data):
    """
    update all metadata where is matching with select_condition

    Args:
        db_name : string
        collection : string
        select_condition : dictionary ex) {"location.syntax":"sangju", "source_type":"csv"}
        update_data : dictionary ex) {"number_of_columns":30,"location.syntax":"sangju", "tag":["hot"]}
    """
    mydb.switchDB(db_name)
    if "tag" in update_data:
        mydb.updateManyKeyToArray(collection,select_condition,{"tag":update_data["tag"]})
        del update_data["tag"]
    return mydb.updateManyKey(collection,select_condition,update_data)

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
    return result

def read_coll_list(db_name):
    """
    retrun all measurement list on a db
    
    Returns:
        result : a list
        if db name is not in a clust catecory return wrong message
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
        if db name is not in a clust catecory return wrong message
    """
    if db_name not in exclude_db and db_name in include_db:
        mydb.switchDB(db_name)
        for coll in mydb.getCollList():
            if(coll==collection_name):
                return ItemstoJson(mydb.getManyData(collection_name))
        return "There is no collection named "+collection_name+" in "+db_name
    return "Wrong DB name"

def read_db_coll_table(db_name,collection_name,table_name):
    """
    retrun a metadata in a collection on a db
    
    Returns:
        result : a dictionary
        if db name is not in a clust catecory return wrong message
    """
    if db_name not in exclude_db and db_name in include_db:
        mydb.switchDB(db_name)
        for coll in mydb.getCollList():
            if(coll==collection_name):
                return mydb.getOneData(collection_name,{"table_name":table_name})
    return "Wrong DB name"

def ItemstoJson(items):
    allData   = []
    for item in items:       
        item['_id']=str(item['_id'])
        allData.append(item)
    return allData

def delete_db_coll_table(db_name,collection_name,table_name):
    """
    delete a metadata in a collection on a db
    
    """
    if db_name not in exclude_db and db_name in include_db:
        mydb.switchDB(db_name)
        for coll in mydb.getCollList():
            if(coll==collection_name):
                mydb.deleteOne(collection_name,{"table_name":table_name})
                return "OK"
    else: return "Wrong DB name"

def delete_one_key(db_name,collection_name,table_name,key):
    """
    delete a key(feature) in a table on a collection
    
    """
    if db_name not in exclude_db and db_name in include_db:
        mydb.switchDB(db_name)
        for coll in mydb.getCollList():
            if(coll==collection_name):
                mydb.deleteOneKey(collection_name,table_name,key)
                return "OK"
    else : return "Wrong DB name"

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
    
    import pprint
    # case -1 : each measurement has a manual location (OUTDOOR_WEATHER, OUTDOOR_AIR, kweather,)
    data = {
                "domain": "OUTDOOR", 
                "sub_domain": "AIR", 
                "table_name": "sangju", 
                "location": {
                    "lat": "", 
                    "lng": "", 
                    "syntax": "경북 상주시 북천로 63 북문동주민센터"
                },
                "description": "This is outdoor air data ", 
                "source_agency": "AirKorea", 
                "source": "Server", 
                "source_type": "API", 
                "tag": ["outdoor", "air", "sangju"], 
            } 

    print("===========case -1===========")
    elements = make_one(data)
    pprint.pprint(elements)
    # with save
    #print(run_and_save(data,-1))

    # case 0 : same location info for all measurements (covid, INNER_AIR)
    data = {
                "domain": "bio", 
                "sub_domain": "covid", 
                "table_name": "seoul_infected_person", 
                "location": {
                "lat": "", 
                "lng": "",
                "syntax": "서울특별시"
                },
                "description": "This is data from people infected with Covid-19 in Seoul.", 
                "source_agency": "서울 열린데이터 광장", 
                "source": "", 
                "source_type": "csv", 
                "tag": ["covid", "seoul", "bio", "health", "life", "infected"], 
            }  

    print("===========case 0===========")
    elements = make(data) # default type=0
    pprint.pprint(elements)
    # with save
    # print(run_and_save(data))
    
    # case 1 : table_name is location syntax (energy_solar, energy_windpower)
    data = {
        "domain": "energy", 
        "sub_domain": "solar", 
        "table_name": "jeju", 
        "location": {
        "lat": "", 
        "lng": "",
        "syntax": "제주도"
        },
        "description": "This is solar energy data in jeju.", 
        "source_agency": "한국전력거래소", 
        "source": "", 
        "source_type": "csv", 
        "tag": ["energy", "solar", "jeju"]
    }

    print("===========case 1===========")
    elements = make(data,1)
    pprint.pprint(elements)
    # with save
    # print(run_and_save(data,1))

    # case 2 : table_name's suffix is location syntax (traffic_subway)
    data = {
        "domain": "traffic", 
        "sub_domain": "seoul_subway", 
        "table_name": "line2_euljiro3ga", 
        "location": {
        "lat": "", 
        "lng": "", 
        "syntax": ""
        },
        "description": "This is subway data in seoul.", 
        "source_agency": "서울 열린데이터 광장", 
        "source": "", 
        "source_type": "csv", 
        "tag": ["traffic", "life", "subway", "seoul"]
    }

    print("===========case 2===========")
    elements = make(data,2)
    pprint.pprint(elements)
    # with save
    # print(run_and_save(data,2))

    # read functions
    print("===========read===========")
    print("===all db and collections===")
    res = read_all_db_coll_list()
    pprint.pprint(res)
    print("===all collection list on a db===")
    res = read_coll_list("bio")
    pprint.pprint(res)
    print("===all metadata list on a collection===")
    res = read_db_coll("test","test")
    pprint.pprint(res)
    print("===one metadata on a collection===")
    res = read_db_coll_table("traffic","seoul_subway","line2_euljiro3ga")
    pprint.pprint(res)
    
    # update functions
    print("===========update===========")
    # res = update_metadata("bio","covid","seoul_infected_person",{"source_type":"xls","tag":["sad"]})
    # res = update_metadata("bio","covid","seoul_infected_person",{"tag":"sad2"})
    # res = update_many_metadata("bio","covid",{"source_type":"csv"},{"source_type":"xls","tag":"pendemic"})
    # res = read_db_coll("bio","covid")
    # pprint.pprint(res)

    # check functions
    print("===========check===========")
    print(check_field("bio","covid","seoul_infected_person","domain"))


    # delete functions
    print("===========delete===========")
    res = delete_db_coll_table("test","test","test")
    print(res)

    