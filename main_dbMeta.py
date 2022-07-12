import numpy as np
import pandas as pd

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

if __name__ == '__main__':
    from KETIToolMetaManager.databaseMeta.dbMetaGenerator import AnalysisResultDbMeta
    from KETIToolMetaManager.data_manager.descriptor import WriteData
    from KETIPreDataIngestion.KETI_setting.influx_setting_KETI import CLUSTDataServer2 as ins
    from KETIPreDataIngestion.data_influx import influx_Client_v2 as iC
    
    db_client = iC.influxClient(ins)
    
    function_list = ["StatisticsAnalyzer", "MeanByHoliday", "MeanByWorking", "MeanByTimeStep"]
    
    #indoor_ls = ["유치원", "요양원", "어린이집", "아파트", "도서관", "경로당", "고등학교", "체육시설", "초등학교"]
    input_param = {
        "database" : "air_indoor_체육시설",
        "function_list" : function_list,
        "mode" : "update"
    }
    save_db = AnalysisResultDbMeta(input_param, db_client)
    analysis_result_meta = save_db.get_mean_analysis_result()
    print(analysis_result_meta)
    WriteData(input_param, {"table_name":"db_information", "analysisResult":analysis_result_meta}).set_db_meta()
    print("--------------------")