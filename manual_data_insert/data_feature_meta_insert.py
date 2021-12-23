import pandas as pd
import numpy as np
import json
from pytimekr import pytimekr
import sys
import os
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
from KETIToolMetaManager.manual_data_insert import wiz_mongo_meta_api as wiz
from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
from KETIPreDataIngestion.data_influx import influx_Client
from pprint import pprint

class MetaDataUpdate():
    '''
    데이터의 column 별로 분석한 결과를 생성하고 이를 Meta로 구축하여 저장
    
    - 데이터의 통계적
    - 시간에 따른
    - label information
    '''
    def __init__(self, domain="all", sub_domain="all", measurement="all" , data=None):
        self.data_nopreprocessing = data
        self.domain = domain
        self.subdomain = sub_domain
        #self.tablename = measurement
        self.data_cut = pd.DataFrame()
        self.data_client = influx_Client.influxClient(ins)
        self.ms_list = self.data_client.measurement_list(self.domain+"_"+self.subdomain)
            
    def get_preprocessing_data(self, tablename):
        """ 지정 tablename에 해당하는 데이터를 읽어와 preprocessing 작업하는 함수

        :param tablename: MongoDB에서는 table_name 이자 InfluxDB에서는 Measurment Name 으로 사용되는 것.
        
        example
            >>> get_preprocessing_data(tablename)
        """        
        #if self.data_nopreprocessing != "all":
        self.data_nopreprocessing = self.data_client.get_data(self.domain+"_"+self.subdomain, tablename)
        # preprocessing
        from KETIPrePartialDataPreprocessing import data_preprocessing
        refine_param = {
            "removeDuplication":{"flag":True},
            "staticFrequency":{"flag":True, "frequency":None}
        }
        outlier_param  = {
            "certainOutlierToNaN":{"flag":True},
            "uncertainOutlierToNaN":{
                "flag":False,
                "param":{"neighbor":[0.5,0.6]}
            },
            "data_type":"air"
        }
        imputation_param = {
            "serialImputation":{
                "flag":True,
                "imputation_method":[{"min":0,"max":20,"method":"linear" , "parameter":{}}],
                "totalNonNanRatio":80
            }
        }
        process_param = {'refine_param':refine_param, 'outlier_param':outlier_param, 'imputation_param':imputation_param}
        partialP = data_preprocessing.packagedPartialProcessing(process_param)
        self.data = partialP.allPartialProcessing(self.data_nopreprocessing)["imputed_data"]

        self.columns = list(self.data.columns)

if __name__ == "__main__":

    from pprint import pprint
    from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
    from KETIPreDataIngestion.data_influx import influx_Client
   
    domain="air"
    subdomain="indoor_초등학교"
    meta_test = MetaDataUpdate(domain, subdomain)
    #meta_test.data_statistics_result_holiday_working_timestep_meta_insert("save", "statistics_all")
    
    ## flag 를 활용해 상황별 입력 파라미터를 다르게 받아 편히 쓰게 하기 / 아래와 같이 주석으로 나열해서 사용하지 않고!!!
    
    ## ----------------------Kweather DataBase Info Save----------------------
    # domain = "air"  
    # sub_domain = "indoor_경로당"
    # measurement = "db_information"

    # rud_features = MetaDataUpdate(domain = domain, sub_domain=sub_domain, measurement=measurement)
    # json = rud_features.kw_database_info_meta_save("insert")

    ## ----------------------Kweather Label Count Create----------------------
    # domain = "air" 
    # sub_domain = "indoor_경로당"
    # measurement = "ICL1L2000234"
    
    # data_by_influxdb = ibd.BasicDatasetRead(ins, domain +"_"+sub_domain, measurement) # DataServer 에서 Meta 추가 코드에 넣어서 사용
    # data = data_by_influxdb.get_data()
    # rud_features = MetaDataUpdate(domain = domain, sub_domain=sub_domain, measurement=measurement, data=data)
    # feature_info = rud_features.data_label_information_meta()
    # pprint(feature_info)

    ## ----------------------Kweather Label Count - Document Save----------------------
    # domain = "air"  # DataServer 에서 버튼 클릭으로 받는 값
    # sub_domain = "indoor_경로당"
    # measurement = "ICL1L2000236"
    
    # data_by_influxdb = influx_Client.influxClient(ins) # DataServer 에서 Meta 추가 코드에 넣어서 사용
    # data = data_by_influxdb.get_data(domain +"_"+sub_domain, measurement)
    # meta = MetaDataUpdate(domain = domain, sub_domain=sub_domain, measurement=measurement, data=data)
    # meta.data_label_information_meta_save("save")

    ## ----------------------Meta Json - Document Save----------------------
    # domain = "farm"
    # sub_domain = "outdoor_weather"
    # measurement = "seoul" # "HS2", "KDS1", "KDS2"

    # meta_json =  {
    #             "table_name": "seoul",
    #             "location": {
    #             "lat": 37.5711068,
    #             "lng": 126.966437,
    #             "syntax": "서울특별시 종로구 송월길 52 서울기상관측소"
    #             },
    #             "description": "This is weater data ",
    #             "source_agency": "AirKorea",
    #             "source": "Server",
    #             "source_type": "API",
    #             "tag": [
    #             "weather",
    #             "outdoor",
    #             "seoul"
    #             ],
    #             "frequency": "0 days 01:00:00"
    #         }

    # meta = MetaDataUpdate(domain = domain, sub_domain=sub_domain, measurement=measurement)
    # meta.data_meta_basic_save_update_insert("update", meta_json)

    ## ----------------------Statistics_result Dict Create----------------------
#     domain="air"
#     subdomain="indoor_초등학교"
#  #   ms = "ICL1L2000283"
#     dirname = "/home/hwangjisoo/바탕화면/케이웨더 데이터 2차/{}/{}".format(subdomain.split("_")[0], subdomain.split("_")[1])
#     mss = os.listdir(dirname)
#     count = 0
#     for ms in mss:
#         ms = ms.split(".")[0]
#         print(ms)
#         statistics = MetaDataUpdate(domain, subdomain, ms)
#         statistics_result = statistics.data_statistics_result_meta()
#         count+=1
#         print(statistics_result)

    ## ----------------------Statistics_result Dict&Holiday&WorkinTime Meta Create&Insert----------------------
#     domain="air"
#     subdomain="indoor_초등학교"
#  #   ms = "ICL1L2000283"
#     dirname = "/home/hwangjisoo/바탕화면/케이웨더 데이터 2차/{}/{}".format(subdomain.split("_")[0], subdomain.split("_")[1])
#     mss = os.listdir(dirname)
#     count = 0
#     for ms in mss:
#         ms = ms.split(".")[0]
#         print(ms)
#         total_meta = MetaDataUpdate(domain, subdomain, ms)
#         total_meta.data_statistics_result_holiday_working_meta_insert("save")
#         count+=1
#         print(count)

    ## ----------------------TimeStep Meta Create&Insert----------------------
#     domain="air"
#     subdomain="indoor_초등학교"
#  #   ms = "ICL1L2000283"
#  #   dirname = "/home/hwangjisoo/바탕화면/케이웨더 데이터 2차/{}/{}".format(subdomain.split("_")[0], subdomain.split("_")[1])
#     dirname = "C:\\Users\\82102\Desktop\\케이웨더 데이터 2차\\{}\\{}".format(subdomain.split("_")[0], subdomain.split("_")[1])
#     mss = os.listdir(dirname)
#     count = 0
#     for ms in mss:
#         ms = ms.split(".")[0]
#         print(ms)
#         only_timestep_meta = MetaDataUpdate(domain, subdomain, ms)
#         timestep_meta_dict = only_timestep_meta.data_time_step_meta()
#         only_timestep_meta.data_meta_basic_save_update_insert("save", timestep_meta_dict, "only_timestep")
#         count+=1
#         print(count)
## -----------------------Statistics_result Dict&Holiday&WorkinTime&TimeStep Meta Create&Insert----------------------
#     domain="air"
#     subdomain="outdoor"
#  #   ms = "ICL1L2000283"
#  #   dirname = "/home/hwangjisoo/바탕화면/케이웨더 데이터 2차/{}/{}".format(subdomain.split("_")[0], subdomain.split("_")[1])
#  #   dirname = "C:\\Users\\82102\Desktop\\케이웨더 데이터 2차\\{}\\{}".format(subdomain.split("_")[0], subdomain.split("_")[1])
#     dirname = "C:\\Users\\82102\\Desktop\\케이웨더 데이터 2차\\outdoor\\SDOT\\4"
#     mss = os.listdir(dirname)
#     count = 0 

#     for ms in mss:
#         ms = ms.split(".")[0]
#         print(ms)
#         total04_meta = MetaDataUpdate(domain, subdomain, ms)
#         total04_meta.data_statistics_result_holiday_working_timestep_meta_insert("save", "statistics_all")
#         count+=1
#         print(count)
        