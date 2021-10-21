import pandas as pd
import datetime
from pytimekr import pytimekr
import meta_read_write as mrw
import json
import os
import wiz_mongo_meta_api as wiz

class MetaDataUpdate():
    def __init__(self, domain="all", sub_domain="all", measurement="all" , data="all"):
        self.data = data
        self.domain = domain
        self.subdomain = sub_domain
        self.tablename = measurement
        self.data_cut = pd.DataFrame()
        if type(self.data) != str:
            self.columns = list(self.data.columns)
    
    # Data Insert all
    def data_meta_all(self):
        # 한개씩 DB-MS 뽑기 -> keti setting & ibd.BasicDatasetRead(ins, "air_indoor_경로당", "ICL1L2000281")
        # for 문 돌려서 아래 함수들 실행
        pass
    
    # Meta Data Create (통)
    def meta_json(self, key1, meta):
        column_dict={}
        feature_dict={}
        for column in self.columns:
            column_dict[column] = {key1:meta[column]}
        feature_dict["Feature"] = column_dict
        return feature_dict

    # Data Feature Describe Create
    def data_feature_describe_meta(self):
        describe_dict = self.data.describe().to_dict()
        return describe_dict

    # Data Feature Describe Insert
    def describe_meta_insert(self):
        des_dict = self.data_feature_describe_meta()
        if (mrw.check_field(self.domain, self.subdomain, self.tablename, "Feature")) & (mrw.check_field(self.domain, self.subdomain, self.tablename, "Feature.{}".format(self.columns[0]))):
            for column in self.columns:
                res = mrw.update_metadata(self.domain, self.subdomain, self.tablename,{"Feature."+column+".describe":des_dict[column]})
            print("Success!")
        else:
            feature_dict = self.meta_json("describe", des_dict)
            print(feature_dict)
            res = mrw.update_metadata(self.domain, self.subdomain, self.tablename,feature_dict)
            print("Success!")
        
    # Data Day Create    
    def transform_day(self):
        days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        day_list = [days[x] for x in self.data.index.weekday]
        self.data["Day"] = day_list
        return self.data
    
    # Data Public Holiday Create
    def transform_public_holiday(self):
        years = range(self.data.index.min().year, self.data.index.max().year+1)
        holi_list = []
        for year in years:
            holi_list += [x.strftime("%Y-%m-%d") for x in pytimekr.holidays(year)]
        holidays = ["Holiday" if x.strftime("%Y-%m-%d") in holi_list else "Weekday" for x in self.data.index]
        self.data["HoliDay"] = holidays

        return self.data
    
    # Data Holiday Create
    def transform_holiday(self):
        self.data = self.transform_day()
        self.data = self.transform_public_holiday()
        
        sat_list = self.data.index[self.data.Day == "Sat"].tolist()
        sun_list = self.data.index[self.data.Day == "Sun"].tolist()
        
        pd.set_option('mode.chained_assignment',  None)
        
        for sat in sat_list:
            self.data["HoliDay"][sat] = "Holiday"
        for sun in sun_list:
            self.data["HoliDay"][sun] = "Holiday"
        
        return self.data

    # Data Weekend, Weekday Meta Create (by Holiday column - transform_holiday)        
    def data_weekday_weekend_meta(self):
        self.data = self.transform_holiday()
        week_dict = self.data.groupby("HoliDay").mean().to_dict()
        
        return week_dict
    
    # Data WorkingTime Meta Create 
    def data_workingtime_othertime_meta(self):
        self.data = self.transform_holiday()
        #week_dict = self.data.groupby("HoliDay").mean().to_dict()
        # holiday + 잠자는 시간 - Other Time 으로 추가
        #return week_dict
        pass

    # KWeather DataBase Info Meta Save
    def kw_database_info_meta_save(self, mode):
        if "indoor" in self.subdomain:
            with open(os.path.dirname(os.path.realpath(__file__))+"/[20211008] indoor_kweather.json", "r", encoding="utf-8") as f:
                feature_json_file = json.load(f)
        else:
            with open(os.path.dirname(os.path.realpath(__file__))+"/[20211008] outdoor_kweather.json", "r", encoding="utf-8") as f:
                feature_json_file = json.load(f)
        
        feature_json_file["table_name"] = self.tablename

        wizapi = wiz.WizApiMongoMeta(self.domain, self.subdomain, self.tablename) # table name = db_information
        wizapi.post_database_collection_document(mode, feature_json_file)

    # Data Label Information Meta Create
    def data_label_information_meta(self):
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

    # Kweather Data Label Information Meta Save (by data_label_information_meta)
    def data_label_information_meta_save(self, mode):
        feature_information = self.data_label_information_meta()
        table_doc = wiz.WizApiMongoMeta(self.domain, self.subdomain, self.tablename)
        table_info_doc = table_doc.get_database_collection_document()
        
        table_info_doc["feature_information"] = feature_information

        table_doc.post_database_collection_document(mode, table_info_doc)

    # Other Data Label Information Meta Insert (by data_label_information_meta & 새롭게 Data Meta Document 를 생성할때 사용)
    def data_label_information_meta_insert(self):
        pass




if __name__ == "__main__":

    from pprint import pprint
    import sys
    sys.path.append("/home/hwangjisoo/바탕화면/Clust")
    #sys.path.append("C:\\Users\\wuk34\\바탕 화면\\Clust")
    from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
    from KETIPreDataIngestion.data_influx import ingestion_basic_dataset as ibd
    
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
    domain = "air"  # DataServer 에서 버튼 클릭으로 받는 값
    sub_domain = "indoor_경로당"
    measurement = "ICL1L2000236"
    
    data_by_influxdb = ibd.BasicDatasetRead(ins, domain +"_"+sub_domain, measurement) # DataServer 에서 Meta 추가 코드에 넣어서 사용
    data = data_by_influxdb.get_data()
    meta = MetaDataUpdate(domain = domain, sub_domain=sub_domain, measurement=measurement, data=data)
    meta.data_label_information_meta_save("save")