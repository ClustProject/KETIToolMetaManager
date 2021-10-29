import pandas
import json
import os
from manual_data_insert import data_feature_meta_insert  as dfmi
from KETIPreDataIngestion.data_influx import influx_Client
from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins

def kweather_data_read(sub_domain):
    count = 0
    dirname = "C:\\Users\\wuk34\\바탕 화면\\케이웨더 데이터 2차\\{}".format(sub_domain)
    #dirname = "C:\\Users\\82102\\Desktop\\케이웨더 데이터 2차\\{}".format(sub_domain)
    domain = "air"
    subdomain = "indoor_{}".format(sub_domain)
    filenames = os.listdir(dirname)

    # DB Meta Insert
    db_meta_insert = dfmi.MetaDataUpdate(domain, subdomain, "db_information")
    db_meta_insert.kw_database_info_meta_insert_save("save")

    for filename in filenames:
        table_name = filename.split(".")[0]
        data_by_influxdb = influx_Client.influxClient(ins)
        data = data_by_influxdb.get_data(domain+"_"+subdomain, table_name)

        meta_insert = dfmi.MetaDataUpdate(domain, subdomain, table_name, data)

        # Ms Meta Insert
        meta_json_file = {
                        "domain": "air", 
                        "sub_domain": "-", 
                        "table_name": "-", 
                        "description": "This is weather data", 
                        "source_agency": "air korea", 
                        "source": "None", 
                        "source_type": "csv", 
                        "tag": ["wheather", "indoor", "air"], 
                        "frequency": "0 days 00:01:00"
                        }
        meta_json_file["sub_domain"] = subdomain
        meta_json_file["table_name"] = table_name
        meta_json_file["tag"].append(subdomain.split("_")[1])

        meta_insert.data_label_information_meta_insert(meta_json_file, "save") # insert 로 입력 시 mode = "insert", save 입력시 mode = "save"


        print(count)
        count+=1




if __name__ == "__main__":

    kweather_data_read("초등학교")