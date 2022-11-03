import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

if __name__ == '__main__':
    from KETIToolMetaManager.measurementAnalysisMeta.metaSetGenerator import AnalysisMetaControl
    from KETIToolMetaManager.metaDataManager import descriptor
    from KETIPreDataIngestion.KETI_setting.influx_setting_KETI import CLUSTDataServer2 as ins
    from KETIPreDataIngestion.data_influx import influx_Client_v2 as iC
    
    db_client = iC.influxClient(ins)
    
    measurement_list = []
    function_list=["StatisticsAnalyzer", "MeanByHoliday", "MeanByWorking", "MeanByTimeStep", "CountByFeatureLabel"]
    #indoor_ls = ["요양원", "어린이집", "아파트", "도서관", "경로당", "고등학교", "초등학교"]
    data_info = {
    "databaseName" : "air_indoor_유치원",
    "measurementsName" : measurement_list,
    "functionList" : function_list,
    "mode" : "update"
    }
    
    meta_set = AnalysisMetaControl(data_info, db_client).get_metaset()
    descriptor.write_data(data_info, meta_set)