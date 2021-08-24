import hashlib
import googlemaps, json
from exploration_db import exploration_data_list
import influx_setting as ins

class MetaGenerator():
    def __init__(self,config) -> None:
        self.api_key = config['MAP_API_KEY']
        self.gmaps = googlemaps.Client(key=self.api_key)
        
    def createId(self,content):
        return hashlib.sha224(content).hexdigest()
    
    def geocoding(self,address):
        geocode_result = self.gmaps.geocode((address), language='ko')
        return geocode_result[0]['geometry']['location']
    
    def reverse_geocoding(self,pos):
        reverse_geocode_result = self.gmaps.reverse_geocode((pos['lat'], pos['lng']),language='ko')
        return reverse_geocode_result[0]["formatted_address"]
    
    def get_info(self,db_name,table_name):
        test = exploration_data_list(ins,db_name,table_name)
        #print(test)
        return test
    
    def generate(self, data):
        '''
        {
            "domain" : "traffic",
            "sub_domain" : "seoul_subway",
            "table_name" : "line3_dongdae",
            "location" :{
            "lat" : "None",
            "lng" : "None",
            "syntax" : "서울특별시 중구 장충동2가 189-2"
            },
            "description" : "This is public data on the Seoul Metro, \
                which has been divided monthly since 20210501",
            "source_agency" : "서울열린데이터광장",
            "source" : "None",
            "source_type" : "csv",
            "tag" : [
            "Traffic",
            "Seoul",
            "Subway",
            "Metro"
            ],
            "collectionFrequency":"day",
            #"column_characteristics":
        },'''
        #if(data["have_location"]=="True"):
        # if("lat" in data["location"]):
        #     pass
        db_name = data["domain"]+"_"+data["sub_domain"]
        table_name = data["table_name"]
        info = self.get_info(db_name,table_name)
        if(data["location"]["syntax"] is not None and data["location"]["syntax"]!=""):
            pos=self.geocoding(data["location"]["syntax"])
            pos["syntax"]=data["location"]["syntax"] #["syntax"]
            pos["lat"]=float(pos["lat"])
            pos["lng"]=float(pos["lng"])
            data["location"]=pos
        
        info = info.drop(["db_name",'measurement_name'],axis=1)
        for col in info.columns:
            print(info[col])
            if col == "number_of_columns":
                data[col]=int(info[col][0])
            else:
                data[col]=str(info[col][0])
        #data["_id"]=self.createId(str(data).encode('utf-8'))
        
        return data

if __name__=="__main__":

    with open('./meta_generator/config.json', 'r') as f:
        config = json.load(f)

    gener = MetaGenerator(config)

    '''
    {
        "domain" : "air",
        "sub_domain" : "indoor_경로당",
        "table_name" : "line3_dongdae",
        "location" :{
        "lat" : "None",
        "lng" : "None",
        "syntax" : ""
        },
        "description" : "This is weather data",
        "source_agency" : "kweather",
        "source" : "None",
        "source_type" : "csv",
        "tag" : [
        "wheather",
        "경로당",
        "indoor",
        "air"
        ],
        "collectionFrequency":"",
        #"column_characteristics":
    },'''
    new_data = {
        "domain" : "air",
        "sub_domain" : "indoor_경로당",
        "table_name" : "ICL1L2000234",
        "location" :{
        "lat" : "None",
        "lng" : "None",
        "syntax" : "원주시 소초면 황골로 172"
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
        ],
        #"collectionFrequency":"day",
        #"column_characteristics":
    }
    metadata = gener.generate(new_data)
    #print(metadata)
    #import json
    import pprint
    pprint.pprint(metadata)
    import sys
    #sys.path.append('../')
    sys.path.append('/Users/yumiseon/KETI/CLUST/CLUSTPROJECT/KETIToolMetaManager')
    #print(sys.path)
    from mongo_management.mongo_crud import MongoCRUD
    with open('/Users/yumiseon/KETI/CLUST/CLUSTPROJECT/KETIToolMetaManager/mongo_management/config.json', 'r') as f:
        config = json.load(f)
    
    db_info = config['DB_INFO']
    db_info['DB_NAME'] = metadata['domain']
    #print(db_info)
    mydb = MongoCRUD(db_info)
    
    collection_name = metadata["sub_domain"]
    
    mydb.insertOne(collection_name,metadata)

    #mydb.insertOne("test",{"test":"test"})
    print(type(metadata))

    colls = mydb.getCollList()
    print(colls)