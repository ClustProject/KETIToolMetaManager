import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

from KETIToolMetaManager.data_manager import wizMongoDbApi as wiz
from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
from KETIPreDataIngestion.data_influx import influx_Client
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
    def __init__(self, database, tablename=None):
        self.db = database
        self.tablename = tablename
        
    def set_process_param(self, new_process_param):
        global process_param
        process_param = new_process_param
    
    def get_db_meta(self):
        domain = self.db.split("_", maxsplit=1)[0]
        sub_domain = self.db.split("_", maxsplit=1)[1]
        mongodb_c = wiz.WizApiMongoMeta()
        base_meta = mongodb_c.get_database_collection_document(domain, sub_domain, "db_information")
        
        return base_meta
    
    def get_ms_data(self):
        data_nopreprocessing = influx_Client.influxClient(ins.CLUSTDataServer).get_data(self.db, self.tablename)
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