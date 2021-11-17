import pandas as pd
import json
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
from KETIToolMetaManager.manual_data_insert import data_feature_meta_insert as dfmi
from KETIToolMetaManager.manual_data_insert import wiz_mongo_meta_api as wiz
from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
from KETIPreDataIngestion.data_influx import influx_Client
from pprint import pprint

class DbMetaCreateInsert():
    def __init__(self, domain, subdomain):
        self.domain = domain
        self.subdomain = subdomain
        
        data_client = influx_Client.influxClient(ins)
        self.ms_list = data_client.measurement_list(self.domain+"_"+self.subdomain)
        self.columns = data_client.get_fieldList(self.domain+"_"+self.subdomain, self.ms_list[0])

    def columns_statistics_dict_create(self, timestep_labels=["dawn", "morning", "afternoon", "evening", "night"]):
        """
        DataBase 의 Meta를 생성하기 앞서 Document의 statistics 값을 Column, Statistics 별로 저장하는 함수

        Args:
            timestep_labels : Document statistics 의 time_step Meta 의 Label 값 
        Returns:
            Document의 statistics 값이 Column, Statistics 별로 저장된 Dictionary
        """
        data_avg_dict = {}
        for column in self.columns:
            data_avg_dict[column]= {"holiday" : [], "notHoliday":[], "working":[], "notWorking":[], 
                                        "des_min":[], "des_max":[], "des_mean":[]}
            for timestep_label in timestep_labels:
                data_avg_dict[column][timestep_label] = []
        return data_avg_dict
    
    def total_statistics_meta_dictionary_create(self, timestep_labels=["dawn", "morning", "afternoon", "evening", "night"]):
        """
        Document statistics 의 Meta 정보를 Column, Statistics 별 평균 값으로 계산하는 함수

        - statistics 별로 평균을 계산하기 위해 Column, Statistics 별로 저장된 Meta의 값을 반환하는 columns_statistics_dict_create 함수를 활용

        Args:
            timestep_labels : Document statistics 의 time_step Meta 의 Label 값 
        Returns:
            해당 DataBase의 Column, Statistics 값 별로 계산한 평균 값
        """
        data_avg_dict_list = self.columns_statistics_dict_create(timestep_labels)
        for tablename in self.ms_list:
            wiz_con = wiz.WizApiMongoMeta(self.domain, self.subdomain, tablename)
            get_meta_dict = wiz_con.get_database_collection_document()
            
            for n in get_meta_dict["feature_information"].keys():
                holilabel = get_meta_dict["feature_information"][n]["statistics"]["day_related_statistics"]["holiday"]["label"]
                holiaverages = get_meta_dict["feature_information"][n]["statistics"]["day_related_statistics"]["holiday"]["average"]
                data_avg_dict_list[n][holilabel[0]].append(holiaverages[0])
                data_avg_dict_list[n][holilabel[1]].append(holiaverages[1])

                worklabel = get_meta_dict["feature_information"][n]["statistics"]["time_related_statistics"]["work"]["label"]
                workaverages = get_meta_dict["feature_information"][n]["statistics"]["time_related_statistics"]["work"]["average"]
                data_avg_dict_list[n][worklabel[0]].append(workaverages[0])
                data_avg_dict_list[n][worklabel[1]].append(workaverages[1])
                
                desaverages = get_meta_dict["feature_information"][n]["statistics"]["average"]
                data_avg_dict_list[n]["des_min"].append(desaverages["min"])
                data_avg_dict_list[n]["des_max"].append(desaverages["max"])
                data_avg_dict_list[n]["des_mean"].append(desaverages["mean"])
                
                tslabels= get_meta_dict["feature_information"][n]["statistics"]["time_related_statistics"]["time_step"]["label"]
                tsaverages = get_meta_dict["feature_information"][n]["statistics"]["time_related_statistics"]["time_step"]["average"]
                for num in range(len(tslabels)):
                    data_avg_dict_list[n][tslabels[num]].append(tsaverages[num])
        
        return data_avg_dict_list
    
    def database_statistics_mean_meta_create(self, timestep_labels=["dawn", "morning", "afternoon", "evening", "night"], timestep_step=[0, 6, 12, 17, 20, 24], 
                                worktime_labels=["notWorking", "working", "notWorking"], worktime_step=[0, 9, 18, 24]):
        """
        해당 DataBase의 statistics Meta 값을 생성하는 함수

        - 해당 DataBase의 Column, Statistics 별로 계산한 평균 값을 DB Meta 구조에 맞게 Dictionary를 구축하는 함수
        - statistics 별로 계산한 평균값을 사용하기 위해 total_statistics_meta_dictionary_create 함수를 활용

        Args:
            timestep_labels : Document statistics 의 time_step Meta 의 나눈 시간 범위에 따른 명칭
            timestep_step : Document statistics 의 time_step Meta 의 나눈 시간의 범위 값
            worktime_labels : Document statistics 의 work Meta의 나눈 시간 범위에 따른 명칭
            worktime_step : Document statistics 의 work Meta의 나눈 시간의 범위 값


        Returns:
            해당 DataBase의 statistics Meta 값
        """
        total_meta_dict = self.total_statistics_meta_dictionary_create(timestep_labels)
        db_meta = {}
        import statistics
        for column in self.columns:
            holi_mean = statistics.mean(total_meta_dict[column]["holiday"])
            notholi_mean = statistics.mean(total_meta_dict[column]["notHoliday"])
            work_mean = statistics.mean(total_meta_dict[column]["working"])
            notwork_mean = statistics.mean(total_meta_dict[column]["notWorking"])
            min_mean = statistics.mean(total_meta_dict[column]["des_min"])
            max_mean = statistics.mean(total_meta_dict[column]["des_max"])
            mean_mean = statistics.mean(total_meta_dict[column]["des_mean"])
            ts_dict = {}
            for x in range(len(timestep_labels)):
                ts_dict[timestep_labels[x]] = statistics.mean(total_meta_dict[column][timestep_labels[x]])
            
            db_meta[column] = {
                "statistics":{
                    "day_related_statistics":{
                        "holiday":{
                            "label":["holiday", "notHoliday"],
                            "average":[holi_mean, notholi_mean]
                        }},
                    "time_related_statistics":{
                        "work":{
                            "label":["working", "notWorking"],
                            "average":[work_mean, notwork_mean],
                            "label_info":{
                                "time_step":worktime_step,
                                "time_label":worktime_labels
                            }
                        },
                        "time_step":{
                            "label":[],
                            "average":[],
                            "label_info":{
                                "time_step":timestep_step,
                                "time_label":timestep_labels
                            }
                        }
                    },
                    "average":{
                        "mean":mean_mean,
                        "min":min_mean,
                        "max":max_mean
                    }}}

            for y in range(len(ts_dict)):
                db_meta[column]["statistics"]["time_related_statistics"]["time_step"]["label"].append(list(ts_dict.items())[y][0])
                db_meta[column]["statistics"]["time_related_statistics"]["time_step"]["average"].append(list(ts_dict.items())[y][1])
        
        db_meta_final_dict = {"table_name":"db_information", "db_feature_information":db_meta}
        return db_meta_final_dict

    def database_statistics_mean_meta_save(self, db_statistics_meta):
        """
        해당 DataBase의 statistics Meta 값을 MongoDB에 저장하는 함수

        - database_statistics_mean_meta_create 함수를 활용해서 구한 DB statistics Meta를 MongoDB에 저장

        Args:
            db_statistics_meta : 해당 DataBase의 statistics Meta 값
        """
        table_doc = wiz.WizApiMongoMeta(self.domain, self.subdomain, "db_information")
        
        table_info_doc = table_doc.get_database_collection_document()
        for n in self.columns:
            table_info_doc["db_feature_information"][n]["statistics"] = db_statistics_meta["db_feature_information"][n]["statistics"]
        
        print(table_info_doc["db_feature_information"][self.columns[0]].keys())
        print(table_info_doc["db_feature_information"][self.columns[0]]["statistics"].keys())
        print(table_info_doc["db_feature_information"][self.columns[0]]["statistics"]["time_related_statistics"].keys())
        
        table_doc.post_database_collection_document("save", table_info_doc)

if __name__ == "__main__":
    ## ----------------------DataBase statistics Meta Create&Save----------------------
    domain = "air"
    sub_domain = "indoor_도서관"
    db_meta_con = DbMetaCreateInsert(domain, sub_domain)
    db_meta_dict = db_meta_con.database_statistics_mean_meta_create()
    pprint(db_meta_dict)
    db_meta_con.database_statistics_mean_meta_save(db_meta_dict)
