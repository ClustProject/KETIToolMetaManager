import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

from KETIToolMetaManager.metaDataManager import collector
from KETIToolAnalyzer.meanAnalyzer import holiday, timeStep, working
from KETIToolAnalyzer.simpleAnalyzer import countAnalyzer, statisticsAnalyzer

class AnalysisMSMetaGenerator():
    """
        MS-분석 A Meta를 생성하는 Generator
        
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


    def __init__(self, bucket_name, measuremnet_list, influx_instance, mongo_instance):
        """
        :param analysis_param: analysis를 위한 param
        :type analysis_param: dictionary

        >>> analysis_param={
            "bucket_name": ,
            "measurement_list": 
        }

        :param measurement_list: 개별 MS 데이터
        :type measurement_list: pd.DataFrame

        :returns: meta_set : 각 테이블에 대한 분석 결과에 따른 테이블
        :rtype: array of dictionaries

        """

        self.mongodb_db = self.bucketName././.
        self.mongodb_collection =  self.bucketName././.
        self.mongo_instance = mongo_instance
        self.influx_measurementList = analysis_param["measurement_list"]
        self.influxdb_bucketName = analysis_param['domain']
        self.influx_instance = influx_instance
        self.function_list = analysis_param["measurement_list"]
        self.function_list = self.checkFunctionList(self.function_list)

    def checkFunctionList(self, function_list):
        """
        
        """
        if function_list==None:
            function_list = [ ]


    def get_metaset(self):
        """
        - 각 데이터 Measurement에 따른 분석 결과

        :param measurement_list: 개별 MS 데이터
        :type measurement_list: pd.DataFrame

        :returns: meta_set : 각 테이블에 대한 분석 결과에 따른 테이블
        :rtype: array of dictionaries

        """
        self.ms_list = self.check_ms_list(self.ms_list)
        collect_read = collector.ReadData()
        bucket_meta = collect_read.get_bucket_meta(self.mongodb_db, self.mongodb_collection, self.mongo_instance)

        self.analysis_meta_set = []
        for measurement in self.ms_list:
            print(f"====== Analyze Meta By {measurement}... ======")
            data = collect_read.get_ms_data_by_days(bucket_name, measurement, self.influx_instance)
            analysis_result_set = self.get_result_set(data, bucket_meta, self.function_list)
            analysis_result_set["table_name"] = measurement
            self.analysis_meta_set.append(analysis_result_set)
            print(f"====== SUCCESS {measurement} ======")
        return self.analysis_meta_set

    def _check_ms_list(self, ms_list): 
        """
        - ms_list를 체크하고 None일 경우 전체 Bucket 안의 list를 불러옴

        :param ms_list: ms_list
        :type ms_list: (array of string) or None

        :returns: ms_list : 각 테이블에 대한 분석 결과에 따른 테이블
        :rtype: array of string

        """

        if ms_list is None:
            ms_list = self.influx_instance.measurement_list(self.db)

        return ms_list
    
    def get_result_set(self, data, meta, function_list):# FL 에 있는 function들을 다 호출해서 output을 개별적으로 받아 set을 하나 만들어야함.
        """
        - functionList에 의거하여 분석 결과를 생성함

        :param data: 개별 MS 데이터
        :type data: pd.DataFrame

        :param meta: Bucket Meta
        :type meta: dictionary

        :param function_list: 수행할 분석 Funcion List
        :type function_list: array of string
        
        :returns: "analysisResult" Key를 갖는 분석 결과
        :rtype: dictionary
        """

        result ={}
        for function in function_list:
            print(f"====== Start {function} Analyzed ======")
            if "StatisticsAnalyzer"  == function:
                result[function] = statisticsAnalyzer.Statistics(data).get_result()
            elif "MeanByHoliday" ==function:
                result[function] = holiday.MeanByHoliday(data).get_result()
            elif "MeanByWorking" ==function:
                result[function] = working.MeanByWorking(data).get_result()
            elif "MeanByTimeStep" ==function:
                result[function] = timeStep.MeanByTimeStep(data).get_result()
            elif "CountByFeatureLabel" ==function:
                result[function] = countAnalyzer.CountByFeatureLabel(data, meta).get_result()

        return {"analysisResult":result}
    

