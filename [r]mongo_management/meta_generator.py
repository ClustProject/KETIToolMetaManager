"""
generator (Test Code by Miseon)
===
the modules 
- generate metadata 

"""
import hashlib
import googlemaps, json
from requests.api import head
import pandas as pd
from datetime import datetime
from pandas.tseries.frequencies import to_offset


class MetaGenerator():
    """
    make metadata using influxDB and googlemaps

    '__init__' : get googlemap api key for geocoding or reverse_geocoding
    """
    def __init__(self,config=None) -> None:
        if config is not None:
            self.api_key = config['MAP_API_KEY']
            self.gmaps = googlemaps.Client(key=self.api_key)
            
    def geocoding(self,address):
        """
        convert address syntex into (latitude , longitude) 
        """
        geocode_result = self.gmaps.geocode((address), language='ko')
        return geocode_result[0]['geometry']['location']
    
    def reverse_geocoding(self,pos):
        """
        convert (latitude , longitude) into address syntex
        """
        reverse_geocode_result = self.gmaps.reverse_geocode((float(pos["lat"]), float(pos["lng"])),language='ko')
        return reverse_geocode_result[0]["formatted_address"]
    
    def get_table_freqeuncy(self,data,freq_check_length=5):
        """
        return time frequency from the data
        """
        freq = to_offset(pd.infer_freq(data[:freq_check_length]["time"]))
        if(freq==None): 
            freq1 = data["time"][1]-data["time"][0]
            freq2 = data["time"][len(data["time"])-1]-data["time"][len(data["time"])-2]
            freq = freq2 if(freq1>freq2) else freq1
        freq_timedelta = pd.to_timedelta(freq, errors='coerce')
        return str(freq_timedelta)

    def get_table_info(self, influxDB, db_name, measurement_name):
        """
        get table information
        """
        exploration_df = pd.DataFrame()
        influxDB.switch_database(db_name)
        ms_list = influxDB.get_list_measurements()
        if len(ms_list) > 0 :
            query_string = "SHOW FIELD KEYS"
            fieldkeys = list(influxDB.query(query_string).get_points(measurement=measurement_name))
            
            # if the measurement doesn't exist 
            if(len(fieldkeys)<=0) : return None
            number_of_columns = len(fieldkeys)
            fieldkey = fieldkeys[0]['fieldKey']
            
            query_string = 'SELECT FIRST("'+fieldkey+'") FROM "'+measurement_name+'"'
            start_time = list(influxDB.query(query_string).get_points())[0]['time']
            query_string = 'SELECT LAST("'+fieldkey+'") FROM "'+measurement_name+'"'
            end_time = list(influxDB.query(query_string).get_points())[0]['time']
            query_string = 'SELECT "'+fieldkey+'" from "'+measurement_name +'" LIMIT 20' 
            df = pd.DataFrame(influxDB.query(query_string).get_points())
                    
            df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M:%SZ")
            frequency = self.get_table_freqeuncy(df)
            exploration_df = exploration_df.append([[start_time, end_time, frequency, number_of_columns]])
        
            exploration_df.columns = ['start_time', 'end_time', 'frequency', 'number_of_columns']
            exploration_df.reset_index(drop=True, inplace = True)
            return exploration_df
        else:
            # if the measurement doesn't exist 
            return None

    def generate(self, data, influxDB):
        """
        make a metadata using parameters

        Args:
            data : dictionary
            influxDB : Object(InfluxDBClient)

        Returns:
            dictionary that have a location info (both syntax and lat&lng)
        """
        db_name = data["domain"]+"_"+data["sub_domain"]
        table_name = data["table_name"]
        info = self.get_table_info(influxDB, db_name, table_name)
        if(info is None): return None
        if("location" in data):
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
    