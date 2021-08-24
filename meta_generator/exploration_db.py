# set modules path
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
from requests.api import head
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
from datetime import datetime


def exploration_data_list(influx_parameter, db_name, measurement):
    #global exploration_df
    exploration_df = pd.DataFrame()
    db = InfluxDBClient(host=influx_parameter.host_, port=influx_parameter.port_, username=influx_parameter.user_, password=influx_parameter.pass_, database=db_name)
    #db_list = db.get_list_database()
    ms_list = db.get_list_measurements()
    if len(ms_list) > 0:
        measurement_name = measurement
            
        query_string = "SHOW FIELD KEYS"
        fieldkeys = list(db.query(query_string).get_points(measurement=measurement))
        number_of_columns = len(fieldkeys)
        fieldkey = fieldkeys[0]['fieldKey']
        print(fieldkey)
        
        query_string = 'SELECT FIRST("'+fieldkey+'") FROM "'+measurement_name+'"'
        start_time = list(db.query(query_string).get_points())[0]['time']
        query_string = 'SELECT LAST("'+fieldkey+'") FROM "'+measurement_name+'"'
        end_time = list(db.query(query_string).get_points())[0]['time']
        query_string = 'SELECT "'+fieldkey+'" from "'+measurement_name +'" LIMIT 2' 
        print(query_string)
        df = pd.DataFrame(db.query(query_string).get_points())
                
        df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%dT%H:%M:%SZ")
        freq = df.time[1] - df.time[0]
                
        if freq.days == 0:
            if freq.seconds == 60:
                frequency = 'Minute'
            elif freq.seconds == 3600:
                frequency = 'Hour'
            elif freq.seconds < 60:
                frequency = 'Second'
            else:
                frequency = '{} Second'.format(str(freq.seconds))
        else:
            if freq.days == 1:
                frequency = '{} Day'.format(str(freq.days))
            elif freq.days == 7:
                frequency = 'Weekend'
            elif freq.days == 31:
                frequency = 'Month'
            elif freq.days == 365:
                frequency = 'Year'
            else:
                frequency = '{} Day'.format(str(freq.days))
        exploration_df = exploration_df.append([[db_name, measurement_name, start_time, end_time, frequency, number_of_columns]])
    
    exploration_df.columns = ['db_name', 'measurement_name', 'start_time', 'end_time', 'frequency', 'number_of_columns']
    exploration_df.reset_index(drop=True, inplace = True)
    return exploration_df


if __name__ == "__main__":
    import influx_setting as ins
    test = exploration_data_list(ins,"air_indoor_경로당","ICL1L2000234")
    print(test)
    #test.to_csv('test.csv')
