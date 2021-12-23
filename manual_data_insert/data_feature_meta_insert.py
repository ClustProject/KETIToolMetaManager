import pandas as pd
import numpy as np
import json
from pytimekr import pytimekr
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
                "totalNanLimit":70
            }
        }
        process_param = {'refine_param':refine_param, 'outlier_param':outlier_param, 'imputation_param':imputation_param}
        partialP = data_preprocessing.packagedPartialProcessing(process_param)
        self.data = partialP.allPartialProcessing(self.data_nopreprocessing)["imputed_data"]

        self.columns = list(self.data.columns)
    
    def data_statistics_result_holiday_working_timestep_meta_insert(self, mode, meta_name):
        """
        데이터의 통계적 값과 휴일, 일하는 시간, Time Step 에 따른 분석 결과를 한번에 Meta 데이터로 생성하는 함수

        - 데이터 시간 정보의 주기가 Hour, Minute, Second 일때 사용
        """
        count = 0
        for tablename in self.ms_list:
            self.get_preprocessing_data(tablename)
            self.tablename = tablename
            statistics_result_meta = self.data_statistics_result_meta()
            holiday_meta = self.data_holiday_notholiday_meta(statistics_result_meta)
            working_meta = self.data_working_notworking_meta(holiday_meta)
            timestep_meta = self.data_time_step_meta(meta_timestep=working_meta)

            self.data_meta_basic_save_update_insert(mode, timestep_meta, meta_name)
            
            count+=1
            print(count)
    
    def data_statistics_result_holiday_working_meta_insert(self, mode, meta_name):
        """ 
        데이터의 통계적 값과 휴일, 일하는 시간에 따른 분석 결과를 한번에 Meta 데이터로 생성하는 함수

        - 데이터 시간 정보의 주기가 1시간 이하일 때 사용
        """
        statistics_result_meta = self.data_statistics_result_meta()
        holiday_meta = self.data_holiday_notholiday_meta(statistics_result_meta)
        working_meta = self.data_working_notworking_meta(holiday_meta)

        self.data_meta_basic_save_update_insert(mode, working_meta, meta_name)

    def data_statistics_result_holiday_meta_insert(self, mode, meta_name):
        """
        데이터의 통계적 값과 휴일에 따른 분석 결과를 한번에 Meta 데이터로 생성하는 함수

        - 데이터 시간 정보의 주기가 일별일 때 사용
        """
        statistics_result_meta = self.data_statistics_result_meta()
        holiday_meta = self.data_holiday_notholiday_meta(statistics_result_meta)

        self.data_meta_basic_save_update_insert(mode, holiday_meta, meta_name)


    # Data Feature statistics result Create
    def data_feature_statistics_result_create(self):
        """
        데이터의 통계적 분포 정보를 Dictionary로 생성하는 함수

        Returns:
            데이터의 통계적 분포 정보를 담고 있는 Dictionary
        """
        statistics_result_dict = self.data.describe().to_dict()
        return statistics_result_dict

    # Data Feature statistics_result Insert
    def data_statistics_result_meta(self):
        """
        데이터의 통계적 분포 분석을 위해 데이터 통계 Meta 정해진 Meta 구조에 맞춰 생성하는 함수

        - data_feature_statistics_result_create 함수를 활용해 데이터의 통계적 정보를 추출

        Returns: 
            데이터의 통계 분포 결과인 Dictionary Meta
        """
        des_dict = self.data_feature_statistics_result_create()
        des_feature_dict = {}
        for n in des_dict:
            for key in des_dict[n]:
                if np.isnan(des_dict[n][key]): # nan -> "None"
                    des_dict[n][key] = "None"
            des_feature_dict[n] = {"statistics":{"average":des_dict[n]}}
        des_final_dict = {"table_name":self.tablename, "feature_information":des_feature_dict}
        return des_final_dict
        
    # Data Day Create    
    def transform_day_create(self):
        """
        데이터의 시간에 따라 "Day" column에 요일 정보를 추가하는 함수

        Returns:
            요일 정보를 포함한 데이터
        """
        days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        day_list = [days[x] for x in self.data.index.weekday]
        self.data["Day"] = day_list
        return self.data
    
    # Data Public Holiday Create
    def transform_holiday_create(self):
        """
        데이터의 시간에 따라 "HoliDay" column에 휴일 정보를 추가하는 함수
        요일 정보를 생성해주는 함수를 활용해서 주말 정보를 휴일로 구분
        휴일은 주말과 공휴일을 의미

        Returns: 
            요일, 휴일 정보를 포함한 데이터
        """
        self.data = self.transform_day_create()
        years = range(self.data.index.min().year, self.data.index.max().year+1)
        holi_list = []
        for year in years:
            holi_list += [x.strftime("%Y-%m-%d") for x in pytimekr.holidays(year)]
        
        weekend_list = set([x.strftime("%Y-%m-%d") for x in self.data[(self.data.Day == "Sat") | (self.data.Day == "Sun")].index.tz_convert(None)])
        final_holi_list = holi_list + [x for x in weekend_list]

        holidays = ["holiday" if x.strftime("%Y-%m-%d") in final_holi_list else "notHoliday" for x in self.data.index]
        self.data["HoliDay"] = holidays

        return self.data
       
    def data_holiday_notholiday_meta(self, meta_holi = None):
        """
        Holiday &Not Holiday 에 따른 데이터의 평균 값을 Meta 로 생성하는 함수

        - 휴일과 휴일이 아닌 날에 따른 분석을 위해 transform_holiday_create 함수로 추출한 "HoliDay" column을 활용
        - Holiday Meta 를 생성한 후 병합하고 싶은 다른 Meta 가 있을시 meta_holi를 입력 받아 병합할 수 있으며 Holiday Meta 만 단독으로 생성하고 싶을 시 Meta 구조에 맞춰 단독 생성 가능

        Args:
            meta_holi: Holiday Meta 와 병합하고 싶은 다른 Meta
        Returns:
            Holiday, Not Holiday 의 평균 값을 포함한 Dictionary Meta
        """
        self.data = self.transform_holiday_create()
        holi_dict = self.data.groupby("HoliDay").mean().to_dict()
        
        holi_feature_dict = {}
        for n in holi_dict: # 먼저 holiday info dictionary를 만들어 둔다.
            if len(holi_dict[n].keys()) == 2:
                holi_x = list(holi_dict[n].keys())[0]
                holi_y = list(holi_dict[n].keys())[1]
                holi_average_x = holi_dict[n][holi_x]
                holi_average_y = holi_dict[n][holi_y]
                
                
                holi_feature_dict[n] ={"statistics":{
                "day_related_statistics":{
                    "holiday":{
                        "label":[holi_x, holi_y],
                        "average":[holi_average_x, holi_average_y]}
                    }}}
                holi_feature_dict[n]["statistics"]["day_related_statistics"]["holiday"]["average"] = ["None" if np.isnan(x) else x for x in holi_feature_dict[n]["statistics"]["day_related_statistics"]["holiday"]["average"]]    
            else:
                holi_x = list(holi_dict[n].keys())[0]
                holi_average_x = holi_dict[n][holi_x]
                if holi_x == "notHoliday":
                    holi_y = "holiday"
                else:
                    holi_y = "notHoliday"
                if np.isnan(holi_average_x):
                    holi_average_x = "None"
                holi_feature_dict[n] ={"statistics":{
                    "day_related_statistics":{
                        "holiday":{
                            "label":[holi_x, holi_y],
                            "average":[holi_average_x, "None"]}
                        }}}

        if meta_holi != None:
            for n in holi_feature_dict:
                meta_holi["feature_information"][n]["statistics"]["day_related_statistics"] = holi_feature_dict[n]["statistics"]["day_related_statistics"]
            return meta_holi
        else:
            holi_final_dict = {"table_name":self.tablename, "feature_information":holi_feature_dict}
            return holi_final_dict # 독립적으로 함수를 쓸땐 사용
    
    # Data WorkingTime Create 
    def data_working_notworking_create(self, working_start=9, working_end=18):
        """
        데이터 시간에 따라 일하는 시간과 일하지 않는 시간의 정보를 "Working" column에 추가하는 함수

        - 데이터에 휴일 정보가 없다면 휴일을 생성하는 transform_holiday_create 함수를 활용하여 휴일정보를 생성 # 데이터에 휴일정보가 있는지 없는지도 파라미터로 받기 or 스스로 알아내서 진행하기
        - 입력 working_start, working_end 범위 외의 시간과 휴일을 일하지 않는 시간으로 정의
        - 데이터 시간 정보의 주기가 1시간 이하일때 사용

        Args:
            working_start: 일을 시작하는 시간
            working_end: 일이 끝나는 시간
        Returns:
            Working Feature 정보를 포함한 데이터
        """
        if "HoliDay" not in self.data.columns: # 휴일 정보가 담긴 column은 이름을 HoliDay로 해야함 -> 사용자가 바꿔줘서 입력해야하낭? 어떻게 처리할것인지 고민하기
            self.data = self.transform_holiday_create()

        working_row = pd.Series("working", 
                                index = self.data.index[(self.data.index.hour >= working_start) & (self.data.index.hour <= (working_end-1))])
        self.data["Working"] = working_row
        self.data.loc[self.data[(self.data.index.hour < working_start) | (self.data.index.hour >= working_end)].index, "Working"] = "notWorking"
        self.data.loc[self.data[self.data.HoliDay == "holiday"].index, "Working"] = "notWorking"
        
        return self.data
    
    # Data WorkingTime Meta Create
    def data_working_notworking_meta(self, meta_work = None, working_start=9, working_end=18):
        """
        Working Time&Not Working Time 에 따른 데이터의 평균 값을 Meta 로 생성하는 함수

        - 일하는 시간과 일하지 않는 시간에 따른 분석을 위해 data_working_notworking_create 함수로 추출한 "Working" column을 활용
        - Working Meta 를 생성한 후 병합하고 싶은 다른 Meta 가 있을시 meta_work를 입력 받아 병합하고 Working Meta 만 단독으로 생성하고 싶을 시 Meta 구조에 맞춰 단독 생성 가능

        Args:
            meta_work: Working Meta 와 병합하고 싶은 다른 Meta
        Returns:
            Working Time, Not Working Time 의 평균 값을 포함한 Dictionary Meta
        """
        self.data = self.data_working_notworking_create(working_start, working_end)
        work_dict = self.data.groupby("Working").mean().to_dict()
        
        work_feature_dict = {}
        for n in work_dict:
            if len(work_dict[n].keys()) == 2:
                work_x = list(work_dict[n].keys())[0]
                work_y = list(work_dict[n].keys())[1]
                work_average_x = work_dict[n][work_x]
                work_average_y = work_dict[n][work_y]

                work_feature_dict[n] ={"statistics":{
                    "time_related_statistics":{
                        "work":{
                            "label":[work_x, work_y],
                            "average":[work_average_x, work_average_y]}
                    }}}
                work_feature_dict[n]["statistics"]["time_related_statistics"]["work"]["average"] = ["None" if np.isnan(x) else x for x in work_feature_dict[n]["statistics"]["time_related_statistics"]["work"]["average"]]
            else:
                work_x = list(work_dict[n].keys())[0]
                work_average_x = work_dict[n][work_x]
                if work_x == "notWorking":
                    work_y = "working"
                else:
                    work_y = "notWorking"
                if np.isnan(work_average_x):
                    work_average_x = "None"
                work_feature_dict[n] ={"statistics":{
                    "time_related_statistics":{
                        "work":{
                            "label":[work_x, work_y],
                            "average":[work_average_x, "None"]}
                    }}}

        if meta_work != None:
            for n in work_feature_dict:
                meta_work["feature_information"][n]["statistics"]["time_related_statistics"] = work_feature_dict[n]["statistics"]["time_related_statistics"]
            return meta_work
        else:
            work_final_dict = {"table_name":self.tablename, "feature_information":work_feature_dict}
            return work_final_dict

    # Data Time Step Create
    def data_time_step_cut(self, timestep=[0, 6, 12, 17, 20, 24], timelabel=["dawn", "morning", "afternoon", "evening", "night"]):
        """
        입력 Time Step에 따라 구분된 Time Label 정보를 "TimeStep" column 에 추가하는 함수
        
        - Hour의 흐름에 따라 구분을 하는 함수로 데이터 시간 정보의 주기가 Hour, Minute, Second 일때 사용

        Args:
            timestep: 나누고 싶은 시간 범위를 입력
            timelabel: 나눈 시간 범위에 따른 명칭 입력
        Returns:
            Time Step 에 따른 Time Label 정보를 포함한 데이터
        """
        self.data["TimeStep"] = np.array(None)
        for n in range(len(timestep)-1):
            self.data.loc[self.data[(self.data.index.hour >= timestep[n])&(self.data.index.hour < timestep[n+1])].index, "TimeStep"] = timelabel[n]
        return self.data

    # Data Time Step Meta Create 
    def data_time_step_meta(self, timestep=[0, 6, 12, 17, 20, 24], timelabel=["dawn", "morning", "afternoon", "evening", "night"], meta_timestep=None):
        """
        입력 Time Step, Time Label 에 따른 데이터의 평균 값을 Meta 로 생성하는 함수

        - Time Step 에 따른 Time Label 정보를 분석하기 위해 data_time_step_cut 함수로 추출한 "TimeStep" column 을 활용
        - Timestep Meta 를 생성한 후 병합하고 싶은 다른 Meta 가 있을시 meta_timestep 입력 받아 병합하고 Timestep Meta 만 단독으로 생성하고 싶을 시 Meta 구조에 맞춰 단독 생성 가능

        Args:
            timestep: 나누고 싶은 시간 범위를 입력
            timelabel: 나눈 시간 범위에 따른 명칭 입력
            meta_timestep: Timestep Meta 와 병합하고 싶은 다른 Meta
        Returns:
            Time Label 에 따른 평균 값을 포함한 Dictionary Meta
        """
        self.data = self.data_time_step_cut(timestep, timelabel)
        timestep_dict = self.data.groupby("TimeStep").mean().to_dict()
        
        timestep_feature_dict = {}
        for n in timestep_dict:
            label = []
            average = []

            for x in timestep_dict[n].keys():
                label.append(x)
                average.append(timestep_dict[n][x])
            
            average = ["None" if np.isnan(x) else x for x in average]
                
            if len(label) != len(timelabel):
                count = len(label)
                [label.append(x) for x in timelabel if x not in label]
                for c in range(len(timelabel)-count):
                    average.append("None")

            timestep_feature_dict[n] ={"statistics":{"time_related_statistics":
                                                     {"time_step":{
                                                         "label":label,
                                                         "average":average}}}}
            
        if meta_timestep != None:
            for n in timestep_feature_dict:
                meta_timestep["feature_information"][n]["statistics"]["time_related_statistics"]["time_step"] = timestep_feature_dict[n]["statistics"]["time_related_statistics"]["time_step"]
            return meta_timestep
        else:
            timestep_final_dict = {"table_name":self.tablename, "feature_information":timestep_feature_dict}
            return timestep_final_dict

    # KWeather DataBase Info Meta Insert or Save
    def kw_database_info_meta_insert_save(self, mode):
        """
        케이웨더 데이터베이스의 정보를 읽은 후 Meta 데이터로 저장하는 함수
        """
        if "indoor" in self.subdomain:
            with open(os.path.dirname(os.path.realpath(__file__))+"/[20211008] indoor_kweather.json", "r", encoding="utf-8") as f:
                feature_json_file = json.load(f)
        else:
            with open(os.path.dirname(os.path.realpath(__file__))+"/[20211008] outdoor_kweather.json", "r", encoding="utf-8") as f:
                feature_json_file = json.load(f)
        
        feature_json_file["table_name"] = "db_information"
        feature_json_file["domain"] = self.domain
        feature_json_file["sub_domain"] = self.subdomain

        wizapi = wiz.WizApiMongoMeta(self.domain, self.subdomain, "db_information") # table name = db_information
        wizapi.post_database_collection_document(mode, feature_json_file)

    # Data Label Information Meta Create
    def data_label_information_meta(self):
        """
        데이터의 Label Information Meta 를 생성하는 함수

        - 데이터베이스에 Label Information 정보가 있어야 함

        """
        db_doc = wiz.WizApiMongoMeta(self.domain, self.subdomain, "db_information")
        db_info_doc = db_doc.get_database_collection_document()
    
        feature_information = {}
        for feature in db_info_doc["db_feature_information"]:
            if "label_information" not in db_info_doc["db_feature_information"][feature].keys():
                feature_information[feature] = {"label_information":{"labelcount":0}}
            else:
                self.data_cut[feature] = pd.cut(x=self.data[feature], 
                                     bins=db_info_doc["db_feature_information"][feature]["label_information"]["level"],
                                    labels=db_info_doc["db_feature_information"][feature]["label_information"]["label"])

                labelcount = dict(self.data_cut.groupby(feature).count())
                label_dict = {}
                label_ls = []
                
                for n in range(len(labelcount)):
                    label_dict["value"] = int(labelcount[list(labelcount.keys())[n]])
                    label_dict["name"] = list(labelcount.keys())[n]
                    label_ls.append(label_dict.copy())

                feature_information[feature] = {"label_information":{"labelcount":label_ls}}

        return feature_information

    # Data Label Information Meta Save or Update (by data_label_information_meta)
    def data_label_information_meta_save_update(self, mode):
        feature_information = self.data_label_information_meta()
        table_doc = wiz.WizApiMongoMeta(self.domain, self.subdomain, self.tablename)
        
        if mode == "save":
            table_info_doc = table_doc.get_database_collection_document()
            table_info_doc["feature_information"] = feature_information
            table_doc.post_database_collection_document(mode, table_info_doc)

        elif mode == "update":
            pass # 위즈온텍 분당 mongodb에 label information 추가할때 쓰기

        else:
            print("The mode is incorrect.")
            
    # Data Label Information Meta Insert (by data_label_information_meta & 새롭게 Data Meta Document 를 생성할때 사용)
    def data_label_information_meta_insert(self, label_meta, mode = "insert"):
        feature_information = self.data_label_information_meta()
        table_doc = wiz.WizApiMongoMeta(self.domain, self.subdomain, self.tablename)

        label_meta["feature_information"] = feature_information
        table_doc.post_database_collection_document(mode, label_meta)

    # Meta Save or Update - Basic Method
    def data_meta_basic_save_update_insert(self, mode, meta_basic, meta_name=None):
        table_doc = wiz.WizApiMongoMeta(self.domain, self.subdomain, self.tablename)
        
        if mode == "save":
            table_info_doc = table_doc.get_database_collection_document()
            if (meta_name != None)&(meta_name != "statistics_all")&(meta_name != "only_timestep"):
                table_info_doc[meta_name] = meta_basic
            elif meta_name == "statistics_all":
                for n in self.columns:
                    table_info_doc["feature_information"][n]["statistics"] = meta_basic["feature_information"][n]["statistics"]
            elif meta_name == "only_timestep":
                for n in self.columns:
                    table_info_doc["feature_information"][n]["statistics"]["time_related_statistics"]["time_step"] = meta_basic["feature_information"][n]["statistics"]["time_related_statistics"]["time_step"]

            print(table_info_doc["feature_information"][self.columns[0]].keys())
            print(table_info_doc["feature_information"][self.columns[0]]["statistics"].keys())
            print(table_info_doc["feature_information"][self.columns[0]]["statistics"]["time_related_statistics"].keys())
            
            #pprint(table_info_doc)
            #table_doc.post_database_collection_document(mode, table_info_doc)

        elif (mode == "update") | (mode == "insert"):
            #table_doc.post_database_collection_document(mode, meta_basic)
            pass
        else:
            print("The mode is incorrect.")

if __name__ == "__main__":

    from pprint import pprint
    from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
    from KETIPreDataIngestion.data_influx import influx_Client
   
    domain="air"
    subdomain="indoor_초등학교"
    meta_test = MetaDataUpdate(domain, subdomain)
    meta_test.data_statistics_result_holiday_working_timestep_meta_insert("save", "statistics_all")
    
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
        