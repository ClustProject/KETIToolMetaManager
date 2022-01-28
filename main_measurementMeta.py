import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

if __name__ == '__main__':
    from KETIToolMetaManager.measurementMeta.metaSetGenerator import AnalysisMetaControl
    from KETIToolMetaManager.data_manager.descriptor import WriteData
    
    measurement_list = ["ICW0W2001042", "ICW0W2001043", "ICW0W2001044"]
    function_list=["StatisticsResult", "MeanByHoliday"]
    
    data_info = {
    "database" : "air_indoor_체육시설",
    "measurements" : measurement_list,
    "function_list" : function_list,
    "mode" : "Update"
    }
    
    meta_set = AnalysisMetaControl(data_info).get_metaset()
    print(meta_set)
    
    WriteData(data_info, meta_set).set_ms_meta()