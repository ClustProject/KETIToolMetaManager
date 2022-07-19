import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

from KETIToolMetaManager.metaDataManager import collector
from KETIToolAnalyzer.batchMetaMaker import BatchMetaMaker as BMM

class AnalysisInputControl(): 
    def __init__(self, database, tablename, function_list, influx_instance):
        """
        분석에 필요한 Data, Basic Meta를 분석 종류에 따라 읽어오는 모듈
        - input Arg는 단독으로 AnalysisInputControl를 사용할때 사용한다.
        
        Args:
            :param database: database name
            :type database: DataFrame
            
            :param tablename: measurement name (data name)
            :type tablename: string
            
            :param fuction_list: Analyzed Method Name
            :type function_list: list
            
            :param influx_instance: instance to get data from influx DB
            :type influx_instance: instance of influxClient class
        """
        self.db = database
        self.tablename = tablename
        self.function_list = function_list
        self.influx_instance = influx_instance
        pass
        
    def get_input_source(self):
        flist_data = ["StatisticsAnalyzer", "MeanByHoliday", "MeanByWorking", "MeanByTimeStep"] #Data
        flist_data_meta = ["CountByFeatureLabel"] #Additional Info, Data
        flist_meta = [] #Additional Info
        
        data_flag = any(function in flist_data for function in self.function_list)
        data_meta_flag = any(function in flist_data_meta for function in self.function_list)
        meta_flag = any(function in flist_meta for function in self.function_list)
        
        collect_read = collector.ReadData(self.influx_instance, self.db, self.tablename)
        
        if data_meta_flag:
            data = collect_read.get_ms_data_by_days()
            base_meta = collect_read.get_db_meta() # collector.ReadData(self.db, self.tablename).get_db_meta()
        else:
            if data_flag:
                data = collect_read.get_ms_data_by_days()
                base_meta = None
            elif meta_flag:
                data = None
                base_meta = collect_read.get_db_meta()  #collector.ReadData(self.db, self.tablename).get_db_meta()
        
        return data, base_meta

class AnalysisMetaControl(AnalysisInputControl):
    def __init__(self, metasave_info, influx_instance):
        self.db = metasave_info["databaseName"]
        self.ms_list = metasave_info["measurementsName"]
        self.function_list = metasave_info["functionList"]
        self.influx_instance = influx_instance
        
    def get_metaset(self): # measurement 조건별 생성한 metaset Output을 결정하는 아이
        print("====== Start Get MetaSet ======")
        if self.ms_list == []: # measurement 를 all 로 기입
            all_ms_list = self.influx_instance.measurement_list(self.db)
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
            print(f"====== Analyze Meta By {self.tablename}... ======")
            data, base_meta = super().get_input_source() # 여기 오래걸림
            print("====== Start Batch Meta Maker ... ======")
            bmmC = BMM(data, base_meta)
            bmmC.set_funclist(self.function_list)
            analysis_result_set = bmmC.get_result_set()
            analysis_result_set["table_name"] = self.tablename
            self.meta_set.append(analysis_result_set)
            print(f"====== SUCCESS {self.tablename} ======")
        return self.meta_set
    
