import numpy as np
import pandas as pd

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

from KETIToolMetaManager.data_manager import collector
from KETIToolMetaManager.data_manager.descriptor import WriteData
from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
from KETIPreDataIngestion.data_influx import influx_Client

class AnalysisResultDbMeta():
    def __init__(self, metasave_info):
        self.metasave_info = metasave_info
        self.db = metasave_info["database"]
        self.mode = metasave_info["mode"]
        self.function_list = metasave_info["function_list"]
        
        self.ms_list = influx_Client.influxClient(ins.CLUSTDataServer).measurement_list(self.db)
        self.columns_list = influx_Client.influxClient(ins.CLUSTDataServer).get_fieldList(self.db, self.ms_list[0])
        
        self.labels = {
            "StatisticsAnalyzer" : ["min", "max", "mean"],
            "MeanByHoliday" : ["holiday", "notHoliday"],
            "MeanByWorking" : ["working", "notWorking"],
            "MeanByTimeStep" : ["dawn", "morning", "afternoon", "evening", "night"]}
        
    def set_labels(self, labels): 
        self.labels = labels
    
    def none_convert_nan(self,value):
        """
        "None" 으로 저장되었던 Meta 값들을 계산에 용이하게 nan 으로 변형해주는 함수
        
        - Meta 값이 "None"인지 판단 후에 "None"일 경우 nan으로 변형

        Args:
            value (string): 변형해주고 싶은 Meta value로 string type이여야 한다.

        Returns:
            - "None"이면 nan으로 변형된 값
            - "None"이 아니였다면 value 그대로 반환
        """
        if value == "None":
            value = np.nan
        return value
    
    def nan_convert_none(self, value):
        """
        nan 으로 저장되었던 Meta 값들을 "None"으로 변형해주는 함수
        
        - Meta 값이 nan인지 판단 후에 nan일 경우 "None"으로 변형

        Args:
            value (string): 변형해주고 싶은 Meta value로 string type이여야 한다.

        Returns:
            - nan이면 "None"으로 변형된 값
            - nan이 아니였다면 value 그대로 반환
        """
        if np.isnan(value):
            value = "None"
        return value
    
    def create_result_form(self):
        """
        DataBase 의 Meta를 생성하기 앞서 Document의 statistics 값을 Column, Statistics 별로 저장하는 함수

        Returns:
            Document의 statistics 값이 Column, Statistics 별로 저장된 Dictionary
        """
        mean_dict = {}
        for column in self.columns_list:
            for key in self.labels.keys():
                mean_dict[column + "_"+ key] = {}
                for label in self.labels[key]:
                    mean_dict[column + "_"+ key][label] = []
        
        return mean_dict
    
    def read_all_ms_meta(self):
        ms_result_dict = self.create_result_form()
        for ms in self.ms_list:
            print(ms)
            ms_meta = collector.ReadData(self.db, ms).get_ms_meta()
            for analyzer in self.function_list:
                for column in self.columns_list:
                    for label in self.labels[analyzer]:
                        label_idx = list(ms_meta["analysisResult"][analyzer][column].keys()).index(label)
                        ms_result_dict[column + "_"+ analyzer][label].append(list(ms_meta["analysisResult"][analyzer][column].values())[label_idx])
        return ms_result_dict
    
    def get_mean_analysis_result(self):
        result_dict = self.read_all_ms_meta()
        analysis_result = []
        for analysis_key in result_dict.keys():
            analysis_result_bycolumn = {}
            analysis_result_bycolumn["columnName"] = analysis_key.rpartition("_")[0]
            analysis_result_bycolumn["analyzerName"] = analysis_key.rpartition("_")[2]
            label = []
            result_value = []
            for label_key in result_dict[analysis_key].keys():
                label.append(label_key)
                value = self.none_convert_nan(result_dict[analysis_key][label_key]) # none -> nan (계산을 위해)
                result_value.append(np.nanmean(value))
            result_value = list(map(self.nan_convert_none, result_value)) # nan -> None (UI를 위해)
            analysis_result_bycolumn["label"] = label
            analysis_result_bycolumn["resultValue"] = result_value
            analysis_result.append(analysis_result_bycolumn)
        #print(analysis_result)
        
        return analysis_result