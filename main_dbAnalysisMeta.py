import numpy as np
import pandas as pd
        
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

if __name__ == '__main__':
    from KETIToolMetaManager.databaseAnalysisMeta.dbMetaGenerator import AnalysisResultDbMeta
    from KETIToolMetaManager.metaDataManager import descriptor
    from KETIPreDataIngestion.KETI_setting.influx_setting_KETI import CLUSTDataServer2 as ins
    from KETIPreDataIngestion.data_influx import influx_Client_v2 as iC
    
    db_client = iC.influxClient(ins)
    
    function_list = ["StatisticsAnalyzer", "MeanByHoliday", "MeanByWorking", "MeanByTimeStep"]
    
    #indoor_ls = ["요양원", "어린이집", "아파트", "도서관", "경로당", "고등학교",  "초등학교"]
    input_param = {
        "dbName":"air",
        "collectionName":"indoor_유치원",
        "tableName" : "db_information",
        "functionList" : function_list,
        "mode" : "update",
        "columnSameByMS" : True
    }
    save_db = AnalysisResultDbMeta(input_param, db_client)
    analysis_result_meta = save_db.get_mean_analysis_result()
    print(analysis_result_meta)
    descriptor.write_data(input_param, {"table_name":"db_information", "analysisResult":analysis_result_meta})
    print("--------------------")