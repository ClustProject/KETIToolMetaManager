import pandas as pd
import datetime
from pytimekr import pytimekr
import meta_read_write as mrw
import json

class MetaDataUpdate():
    def __init__(self, data="all", domain="all", sub_domain="all", measurement="all"):
        self.data = data
        self.domain = domain
        self.subdomain = sub_domain
        self.msname = measurement
        self.data_cut = pd.DataFrame()
        if type(self.data) != str:
            self.columns = list(self.data.columns)
            print(self.columns)
    
    def data_meta_all(self):
        # 한개씩 DB-MS 뽑기 -> keti setting & ibd.BasicDatasetRead(ins, "air_indoor_경로당", "ICL1L2000281")
        # for 문 돌려서 아래 함수들 실행
        pass
    
    def meta_json(self, key1, meta):
        column_dict={}
        feature_dict={}
        for column in self.columns:
            column_dict[column] = {key1:meta[column]}
        feature_dict["Feature"] = column_dict
        return feature_dict

    def data_feature_describe_meta(self):
        describe_dict = self.data.describe().to_dict()
        return describe_dict

    def describe_meta_insert(self):
        des_dict = self.data_feature_describe_meta()
        if (mrw.check_field(self.domain, self.subdomain, self.msname, "Feature")) & (mrw.check_field(self.domain, self.subdomain, self.msname, "Feature.{}".format(self.columns[0]))):
            for column in self.columns:
                res = mrw.update_metadata(self.domain, self.subdomain, self.msname,{"Feature."+column+".describe":des_dict[column]})
            print("Success!")
        else:
            feature_dict = self.meta_json("describe", des_dict)
            print(feature_dict)
            res = mrw.update_metadata(self.domain, self.subdomain, self.msname,feature_dict)
            print("Success!")
        
        
    def transform_day(self):
        days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        day_list = [days[x] for x in self.data.index.weekday]
        self.data["Day"] = day_list
        return self.data
    
    def transform_public_holiday(self):
        years = range(self.data.index.min().year, self.data.index.max().year+1)
        holi_list = []
        for year in years:
            holi_list += [x.strftime("%Y-%m-%d") for x in pytimekr.holidays(year)]
        holidays = ["Holiday" if x.strftime("%Y-%m-%d") in holi_list else "Weekday" for x in self.data.index]
        self.data["HoliDay"] = holidays

        return self.data
    
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
            
    def data_weekday_weekend_meta(self):
        self.data = self.transform_holiday()
        week_dict = self.data.groupby("HoliDay").mean().to_dict()
        
        return week_dict
    
    def data_workingtime_othertime_meta(self):
        self.data = self.transform_holiday()
        #week_dict = self.data.groupby("HoliDay").mean().to_dict()
        
        #return week_dict
        pass

    def data_level_label_count_meta(self):
        if "indoor" in self.subdomain:
            with open("[20211008] indoor_kweather.json", "r", encoding="utf-8") as f:
                feature_json_file = json.load(f)
        else:
            with open("[20211008] outdoor_kweather.json", "r", encoding="utf-8") as f:
                feature_json_file = json.load(f)

        Feature_Information = {}
        for feature in feature_json_file["Feature Information"]:
            if "lebel_information" not in feature_json_file["Feature Information"][feature].keys():
                Feature_Information[feature] = {"label_information":{"There is not a Label Information"}}
            else:
                # data 에서 feature 추출 할때 "in_" 꼭 붙여주기
                self.data_cut[feature] = pd.cut(x=self.data["in_"+feature], 
                                     bins=feature_json_file["Feature Information"][feature]["lebel_information"]["level"],
                                    labels=feature_json_file["Feature Information"][feature]["lebel_information"]["label"])
                
                print(self.data_cut.groupby(feature).size())
                #list(df.groupby(feature).size())
                #print(feature_json_file["Feature Information"][feature]["lebel_information"]["level"])
                #print(feature_json_file["Feature Information"][feature]["lebel_information"]["label"])
                Feature_Information[feature] = {"label_information":
                                               {"level":feature_json_file["Feature Information"][feature]["lebel_information"]["level"],
                                                "label":feature_json_file["Feature Information"][feature]["lebel_information"]["label"], 
                                                "levelcount":list(self.data_cut.groupby(feature).size())
                                               }}

        return Feature_Information

    def data_level_label_count_meta_insert(self):
        Feature_Information = self.data_level_label_count_meta()
        if (mrw.check_field(self.domain, self.subdomain, self.msname, "Feature")) & (mrw.check_field(self.domain, self.subdomain, self.msname, "Feature.{}".format(self.columns[0]))):
            for column in self.columns:
                res = mrw.update_metadata(self.domain, self.subdomain, self.msname,{"Feature."+column+".label_information":Feature_Information[column]})
            print("Success!")
        else:
            feature_dict = self.meta_json("label_information", Feature_Information)
            print(feature_dict)
            res = mrw.update_metadata(self.domain, self.subdomain, self.msname, feature_dict)
            print("Success!")




if __name__ == "__main__":


    import sys
    ##sys.path.append("/home/hwangjisoo/바탕화면/Clust/KETIPreDataIngestion/data_influx")
    #sys.path.append("/home/hwangjisoo/바탕화면/Clust")
    sys.path.append("C:\\Users\\wuk34\\바탕 화면\\Clust")
    from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
    from KETIPreDataIngestion.data_influx import ingestion_basic_dataset as ibd
    
    domain = "air"  # DataServer 에서 버튼 클릭으로 받는 값
    sub_domain = "indoor_경로당"
    measurement = "ICL1L2000280"
    
    test1 = ibd.BasicDatasetRead(ins, domain +"_"+sub_domain, measurement) # DataServer 에서 Meta 추가 코드에 넣어서 사용
    rud283 = test1.get_data()
    rud_features = MetaDataUpdate(data = rud283, domain = domain, sub_domain=sub_domain, measurement=measurement)
    ##des = rud_features.data_feature_describe_meta()
    ##day = rud_features.data_weekday_weekend_meta()
    #rud_features.describe_meta_insert()

    # read functions
    import pprint

    print("===========read===========")
    print("===all db and collections===")
    res = mrw.read_all_db_coll_list()
    pprint.pprint(res)
    print("===all collection list on a db===")
    res = mrw.read_coll_list(domain)
    pprint.pprint(res)
    print("===all metadata list on a collection===")
    res = mrw.read_db_coll(domain,sub_domain)
    pprint.pprint(res)