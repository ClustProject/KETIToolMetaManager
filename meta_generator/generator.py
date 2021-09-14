import hashlib
import googlemaps, json
from requests.api import head
import pandas as pd
from datetime import datetime
from pandas.tseries.frequencies import to_offset


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
        reverse_geocode_result = self.gmaps.reverse_geocode((float(pos["lat"]), float(pos["lng"])),language='ko')
        return reverse_geocode_result[0]["formatted_address"]
    
    def get_table_freqeuncy(self,data,freq_check_length=5):
        #freq = self.get_df_freq_timedelta(data)
        freq = to_offset(pd.infer_freq(data[:freq_check_length]["time"]))
        freq_timedelta = pd.to_timedelta(freq, errors='coerce')
        return str(freq_timedelta)
        #return str(freq)

    def get_table_info(self, influxDB, db_name, measurement_name):
        exploration_df = pd.DataFrame()
        influxDB.switch_database(db_name)
        ms_list = influxDB.get_list_measurements()
        if len(ms_list) > 0:
            query_string = "SHOW FIELD KEYS"
            fieldkeys = list(influxDB.query(query_string).get_points(measurement=measurement_name))
            number_of_columns = len(fieldkeys)
            fieldkey = fieldkeys[0]['fieldKey']
            
            query_string = 'SELECT FIRST("'+fieldkey+'") FROM "'+measurement_name+'"'
            start_time = list(influxDB.query(query_string).get_points())[0]['time']
            query_string = 'SELECT LAST("'+fieldkey+'") FROM "'+measurement_name+'"'
            end_time = list(influxDB.query(query_string).get_points())[0]['time']
            query_string = 'SELECT "'+fieldkey+'" from "'+measurement_name +'" LIMIT 20' 
            df = pd.DataFrame(influxDB.query(query_string).get_points())
                    
            df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M:%SZ")
            # freq = df.time[1] - df.time[0]
                    
            frequency = self.get_table_freqeuncy(df)
            exploration_df = exploration_df.append([[db_name, measurement_name, start_time, end_time, frequency, number_of_columns]])
        
        exploration_df.columns = ['db_name', 'measurement_name', 'start_time', 'end_time', 'frequency', 'number_of_columns']
        exploration_df.reset_index(drop=True, inplace = True)
        return exploration_df

    def generate(self, data, influxDB):
        db_name = data["domain"]+"_"+data["sub_domain"]
        table_name = data["table_name"]
        info = self.get_table_info(influxDB, db_name, table_name)
        if(data["location"]["syntax"] is not None and data["location"]["syntax"]!=""):
            pos=self.geocoding(data["location"]["syntax"])
            pos["syntax"]=data["location"]["syntax"] #["syntax"]
            pos["lat"]=float(pos["lat"])
            pos["lng"]=float(pos["lng"])
            data["location"]=pos
        elif(data["location"]["lat"] is not None and data["location"]["lat"]!="" and data["location"]["lng"] is not None and data["location"]["lng"]!=""):
            pos=self.reverse_geocoding(data["location"])
            data["location"]['syntax']=pos
        else:
            del data["location"]
        
        info = info.drop(["db_name",'measurement_name'],axis=1)
        for col in info.columns:
            #print(info[col])
            if col == "number_of_columns":
                data[col]=int(info[col][0])
            else:
                data[col]=str(info[col][0])
        
        return data

if __name__=="__main__":

    with open('./meta_generator/config.json', 'r') as f:
        config = json.load(f)
    import influx_setting as ins
    gener = MetaGenerator(config)
    from influxdb import InfluxDBClient, DataFrameClient
    db = InfluxDBClient(host=ins.host_, port=ins.port_, username=ins.user_, password=ins.pass_)
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
        ]
    }
    metadata = gener.generate(new_data,db)
    
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
    
    #mydb.insertOne(collection_name,metadata)
    #mydb.deleteOne(collection_name,{"table_name":"ICL1L2000234"})
    #mydb.insertOne("test",{"test":"test"})
    #print(type(metadata))

    colls = mydb.getCollList()
    print(colls)