import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
from KETIPreDataIngestion.data_influx import influx_Client
from KETIToolMetaManager.data_manager import collector
from KETIToolAnalyzer.batchMetaMaker import BatchMetaMaker as BMM

class AnalysisInputControl(): # 얘가 data_collector한테 self.db 넘겨줘야 할듯 
    def __init__(self, database, tablename, function_list):
        self.db = database
        self.tablename = tablename
        self.function_list = function_list
        
    def get_input_source(self):
        flist_data = ["StatisticsResult"] #Data
        flist_data_meta = ["MeanByHoliday", "MeanByWorking", "MeanByTimeStep", "FeatureLabel"] #Additional Info, Data
        flist_meta = [] #Additional Info
        
        data_flag = any(function in flist_data for function in self.function_list)
        data_meta_flag = any(function in flist_data_meta for function in self.function_list)
        meta_flag = any(function in flist_meta for function in self.function_list)
        
        if data_meta_flag:
            data = collector.ReadData(self.db, self.tablename).get_ms_data()
            base_meta = collector.ReadData(self.db, self.tablename).get_db_meta()
        else:
            if data_flag:
                data = collector.ReadData(self.db, self.tablename).get_ms_data()
                base_meta = None
            elif meta_flag:
                data = None
                base_meta = collector.ReadData(self.db, self.tablename).get_db_meta()
        
        return data, base_meta

class AnalysisMetaControl(AnalysisInputControl):
    def __init__(self, metasave_info):
        self.db = metasave_info["database"]
        self.ms_list = metasave_info["measurements"]
        self.function_list = metasave_info["function_list"]
        self.mode = metasave_info["mode"]
        
    def get_metaset(self): # measurement 조건별 생성한 metaset Output을 결정하는 아이
        if self.ms_list == []: # measurement 를 all 로 기입
            all_ms_list = influx_Client.influxClient(ins.CLUSTDataServer).measurement_list(self.db)
            self.meta_set = self.get_analyzed_meta(all_ms_list)
        else:
            if len(self.ms_list) == 1: # measurement 한개 기입
                self.meta_set = self.get_analyzed_meta(self.ms_list)[0]
            else: # measurement 를 여러개 기입
                self.meta_set = self.get_analyzed_meta(self.ms_list)
                
        return self.meta_set
    
    def get_analyzed_meta(self, tablenames):
        self.meta_set = []
        for self.tablename in tablenames:
            data, base_meta = super().get_input_source()
            bmmC = BMM(data, base_meta)
            bmmC.set_funclist(self.function_list)
            analysis_result_set = bmmC.get_result_set()
            self.meta_set.append(analysis_result_set)
        return self.meta_set
    
