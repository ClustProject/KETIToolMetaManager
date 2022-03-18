import numpy as np
import pandas as pd

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

if __name__ == '__main__':
    from KETIToolMetaManager.databaseMeta.dbMetaGenerator import AnalysisResultDbMeta
    from KETIToolMetaManager.data_manager.descriptor import WriteData
    
    function_list = ["StatisticsAnalyzer", "MeanByHoliday", "MeanByWorking", "MeanByTimeStep"]
    
    input_param = {
        "database" : "air_indoor_고등학교",
        "function_list" : function_list,
        "mode" : "update"
    }
    save_db = AnalysisResultDbMeta(input_param)
    analysis_result_meta = save_db.get_mean_analysis_result()
    print(analysis_result_meta)
    WriteData(input_param, {"table_name":"db_information", "analysisResult":analysis_result_meta}).set_db_meta()
    print("--------------------")