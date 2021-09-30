import pandas as pd
import datetime
from pytimekr import pytimekr

class MetaDataUpdate():
    def __init__(self, data="all"):
        self.data = data
        if type(self.data) != str:
            self.columns = list(self.data.columns)
    
    def data_meta_all(self):
        # 한개씩 DB-MS 뽑기 -> keti setting & ibd.BasicDatasetRead(ins, "air_indoor_경로당", "ICL1L2000283")
        # for 문 돌려서 아래 함수들 실행
        pass

    def data_feature_describe_meta(self):
        describe_dict = self.data.describe().to_dict()
        return describe_dict

    def describe_meta_insert(self):
        des_dict = self.data_feature_describe_meta()
        pass
    
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


if __name__ == "__main__":


    import sys
    ##sys.path.append("/home/hwangjisoo/바탕화면/Clust/KETIPreDataIngestion/data_influx")
    sys.path.append("/home/hwangjisoo/바탕화면/Clust")
    #sys.path.append("C:\\Users\\wuk34\\바탕 화면\\Clust")
    from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
    from KETIPreDataIngestion.data_influx import ingestion_basic_dataset as ibd
    
    test1 = ibd.BasicDatasetRead(ins, "air_indoor_경로당", "ICL1L2000283") # DataServer 에서 Meta 추가 코드에 넣어서 사용
    rud283 = test1.get_data()
    rud_features = MetaDataUpdate(data = rud283)
    des = rud_features.data_feature_describe_meta()
    day = rud_features.data_weekday_weekend_meta()