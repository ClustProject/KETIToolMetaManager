import pandas as pd
import datetime
from pytimekr import pytimekr
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
from KETIToolMetaManager.manual_data_insert import meta_read_write as mrw
import json
import os
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
        self.tablename = measurement
        self.data_cut = pd.DataFrame()
        
        if self.data_nopreprocessing != "all":
            data_client = influx_Client.influxClient(ins)
            self.data_nopreprocessing = data_client.get_data(self.domain+"_"+self.subdomain, self.tablename)

            # preprocessing
            from KETIPrePartialDataPreprocessing import data_preprocessing
            refine_param = {'removeDuplication':True, 'staticFrequency':True} 
            outlier_param= {'certainOutlierToNaN':True, 'uncertainOutlierToNaN':True, 'data_type':'air'}
            imputation_param ={ "imputation_method":[{"min":0,"max":5,"method":"linear"}],"totalNanLimit":30}
            self.data = data_preprocessing.ByAllMethod(self.data_nopreprocessing, refine_param, outlier_param, imputation_param)["imputed_data"]

            self.columns = list(self.data.columns)

    def data_describe_holiday_working_meta_insert(self, mode):
        """
        데이터의 통계적 값과 휴일, 일하는 시간에 따른 분석 결과를 한번에 Meta 데이터로 생성하는 함수
        """
        describe_meta = self.data_describe_meta()
        holiday_meta = self.data_holiday_notholiday_meta(describe_meta)
        working_meta = self.data_working_notworking_meta(holiday_meta)

        self.data_meta_basic_save_update_insert(mode, working_meta)
        #return working_meta

    # Data Feature Describe Create
    def data_feature_describe_create(self):
        """
        데이터의 통계적 분포 정보를 Dictionary로 생성하는 함수

        Returns:
            데이터의 통계적 분포 정보를 담고 있는 Dictionary
        """
        describe_dict = self.data.describe().to_dict()
        return describe_dict

    # Data Feature Describe Insert
    def data_describe_meta(self):
        """
        데이터의 통계적 분포 분석을 위해 데이터 통계 Meta 정해진 Meta 구조에 맞춰 생성하는 함수

        - data_feature_describe_create 함수를 활용해 데이터의 통계적 정보를 추출

        Returns: 
            데이터의 통계 분포 결과인 Dictionary Meta
        """
        des_dict = self.data_feature_describe_create()
        des_feature_dict = {}
        for n in des_dict: 
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
        - Holiday Meta 를 생성한 후 병합하고 싶은 다른 Meta 가 있을시 Meta를 입력 받아 병합할 수 있으며 Holiday Meta 만 단독으로 생성하고 싶을 시 Meta 구조에 맞춰 단독 생성 가능

        Args:
            meta_holi: Holiday Meta 와 병합하고 싶은 다른 Meta
        Returns:
            Holiday, Not Holiday 의 평균 값을 포함한 Dictionary Meta
        """
        self.data = self.transform_holiday_create()
        holi_dict = self.data.groupby("HoliDay").mean().to_dict()
        
        holi_feature_dict = {}
        for n in holi_dict: # 먼저 holiday info dictionary를 만들어 둔다.
            x = list(holi_dict[n].keys())[0]
            y = list(holi_dict[n].keys())[1]
            holi_feature_dict[n] ={"statistics":{
                "day_related_statistics":{
                    "holiday":{
                        "label":[x, y],
                        "average":[holi_dict[n][x], holi_dict[n][y]]}
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

        - 데이터에 휴일 정보가 없다면 휴일을 생성하는 transform_holiday_create 함수를 활용하여 휴일정보를 생성
        - 입력 working_start, working_end 범위 외의 시간과 휴일을 일하지 않는 시간으로 정의

        Args:
            working_start: 일을 시작하는 시간
            working_end: 일이 끝나는 시간
        Returns:
            Working Feature 정보를 포함한 데이터
        """
        if "HoliDay" not in self.data.columns:
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
        - Working Meta 를 생성한 후 병합하고 싶은 다른 Meta 가 있을시 Meta를 입력 받아 병합하고 Working Meta 만 단독으로 생성하고 싶을 시 Meta 구조에 맞춰 단독 생성 가능

        Args:
            meta_work: Working Meta 와 병합하고 싶은 다른 Meta
        Returns:
            Working Time, Not Working Time 의 평균 값을 포함한 Dictionary Meta
        """
        self.data = self.data_working_notworking_create(working_start, working_end)
        work_dict = self.data.groupby("Working").mean().to_dict()
        
        work_feature_dict = {}
        for n in work_dict:
                x = list(work_dict[n].keys())[0]
                y = list(work_dict[n].keys())[1]
                work_feature_dict[n] ={"statistics":{
                    "time_related_statistics":{
                        "work":{
                            "label":[x, y],
                            "average":[work_dict[n][x], work_dict[n][y]]}
                    }}}

        if meta_work != None:
            for n in work_feature_dict:
                meta_work["feature_information"][n]["statistics"]["time_related_statistics"] = work_feature_dict[n]["statistics"]["time_related_statistics"]
            return meta_work
        else:
            work_final_dict = {"table_name":self.tablename, "feature_information":work_feature_dict}
            return work_final_dict

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

                labelcount = dict(self.data_cut.groupby(feature).size())
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
            if meta_name != None:
                table_info_doc[meta_name] = meta_basic
            else:
                for n in self.columns:
                    table_info_doc["feature_information"][n]["statistics"] = meta_basic["feature_information"][n]["statistics"]

            print(table_info_doc["feature_information"]["in_voc"].keys())
            print(table_info_doc["feature_information"]["in_voc"]["statistics"].keys())
            
            table_doc.post_database_collection_document(mode, table_info_doc)

        elif (mode == "update") | (mode == "insert"):
            table_doc.post_database_collection_document(mode, meta_basic)

        else:
            print("The mode is incorrect.")


if __name__ == "__main__":

    from pprint import pprint
    import sys
    sys.path.append("/home/hwangjisoo/바탕화면/Clust")
    #sys.path.append("C:\\Users\\wuk34\\바탕 화면\\Clust")
    from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
    from KETIPreDataIngestion.data_influx import influx_Client
    
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

    ## ----------------------Describe Dict Create----------------------
#     domain="air"
#     subdomain="indoor_초등학교"
#  #   ms = "ICL1L2000283"
#     dirname = "/home/hwangjisoo/바탕화면/케이웨더 데이터 2차/{}/{}".format(subdomain.split("_")[0], subdomain.split("_")[1])
#     mss = os.listdir(dirname)
#     count = 0
#     for ms in mss:
#         ms = ms.split(".")[0]
#         print(ms)
#         average = MetaDataUpdate(domain, subdomain, ms)
#         average.describe_meta_insert("update")
#         count+=1
#         print(count)

    ## ----------------------Describe Dict&Holiday&WorkinTime Meta Create&Insert----------------------
    domain="air"
    subdomain="indoor_초등학교"
 #   ms = "ICL1L2000283"
    dirname = "/home/hwangjisoo/바탕화면/케이웨더 데이터 2차/{}/{}".format(subdomain.split("_")[0], subdomain.split("_")[1])
    mss = os.listdir(dirname)
    count = 0
    for ms in mss:
        ms = ms.split(".")[0]
        print(ms)
        total_meta = MetaDataUpdate(domain, subdomain, ms)
        total_meta.data_describe_holiday_working_meta_insert("save")
        count+=1
        print(count)
    

        """
            # Data Holiday Create
            # def transform_holiday(self):
            #     self.data = self.transform_day()
            #     self.data = self.transform_public_holiday()
                
            #     sat_list = self.data.index[self.data.Day == "Sat"].tolist()
            #     sun_list = self.data.index[self.data.Day == "Sun"].tolist()
                
            #     pd.set_option('mode.chained_assignment',  None)
                
            #     for sat in sat_list:
            #         self.data["HoliDay"][sat] = "holiday"
            #     for sun in sun_list:
            #         self.data["HoliDay"][sun] = "holiday"
                
            #     return self.data

            # Data Weekend, Weekday Meta Create & Insert (by Holiday column - transform_holiday)     
        """

        