import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

from KETIToolMetaManager.metaDataManager import wizMongoDbApi as wiz
from KETIPrePartialDataPreprocessing import data_preprocessing

# packcage : InputSourceController
# class : Collector
refine_param = {
            "removeDuplication":{"flag":True},
            "staticFrequency":{"flag":True, "frequency":None}
        }
        
outlier_param  = {
    "certainErrorToNaN":{"flag":True},
    "unCertainErrorToNaN":{
        "flag":False,
        "param":{"neighbor":0.5}
    },
    "data_type":"air"
}

imputation_param = {
    "serialImputation":{
        "flag":True,
        "imputation_method":[{"min":0,"max":20,"method":"linear" , "parameter":{}}],
        "totalNonNanRatio":70
    }
}
process_param = {'refine_param':refine_param, 'outlier_param':outlier_param, 'imputation_param':imputation_param}

class ReadData(): # GetInputSource / InputSourceCollector
    def __init__(self, influx_instance, database, tablename=None):
        self.db = database
        self.tablename = tablename
        self.influx_instance = influx_instance
        
    def set_process_param(self, new_process_param):
        global process_param
        process_param = new_process_param
    
    def get_db_meta(self):
        domain = self.db.split("_", maxsplit=1)[0]
        sub_domain = self.db.split("_", maxsplit=1)[1]
        mongodb_c = wiz.WizApiMongoMeta()
        base_meta = mongodb_c.get_database_collection_document(domain, sub_domain, "db_information")
        
        return base_meta
    
    def get_ms_data_by_days(self):
        days = 365
        print(self.db, self.tablename)
        end_time = self.influx_instance.get_last_time(self.db, self.tablename)
        data_nopreprocessing = self.influx_instance.get_data_by_days(end_time, days, self.db, self.tablename)
        # preprocessing
        partialP = data_preprocessing.packagedPartialProcessing(process_param)
        dataframe = partialP.allPartialProcessing(data_nopreprocessing)["imputed_data"]

        return dataframe
    
    def get_ms_data(self):
        data_nopreprocessing = self.influx_instance.get_data(self.db, self.tablename)
        # preprocessing
        partialP = data_preprocessing.packagedPartialProcessing(process_param)
        dataframe = partialP.allPartialProcessing(data_nopreprocessing)["imputed_data"]

        return dataframe
    
    def get_ms_meta(self):
        domain = self.db.split("_", maxsplit=1)[0]
        sub_domain = self.db.split("_", maxsplit=1)[1]
        mongodb_c = wiz.WizApiMongoMeta()
        base_meta = mongodb_c.get_database_collection_document(domain, sub_domain, self.tablename)
        
        return base_meta