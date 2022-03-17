import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

if __name__ == '__main__':
    from KETIToolMetaManager.measurementMeta.metaSetGenerator import AnalysisMetaControl
    from KETIToolMetaManager.data_manager.descriptor import WriteData
    
    measurement_list = []
    function_list=["StatisticsAnalyzer", "MeanByHoliday", "MeanByWorking", "MeanByTimeStep", "CountByFeatureLabel"]
    
    data_info = {
    "database" : "air_indoor_아파트",
    "measurements" : measurement_list,
    "function_list" : function_list,
    "mode" : "update"
    }
    
    meta_set = AnalysisMetaControl(data_info).get_metaset()
    WriteData(data_info, meta_set).set_ms_meta()