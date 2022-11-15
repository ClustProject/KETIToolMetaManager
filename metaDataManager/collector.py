import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

from KETIPrePartialDataPreprocessing import data_preprocessing
from KETIToolMetaManager.metaDataManager import wizMongoDbApi as wiz
# packcage : InputSourceController
# class : Collector


class ReadData(): 
    def __init__(self):
        """"
        Data를 상황에 맞게 읽는 함수
        """
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
        self.process_param = {'refine_param':refine_param, 'outlier_param':outlier_param, 'imputation_param':imputation_param}
        
    def set_process_param(self, new_process_param):
        """
        Process Param을 새롭게 정의한다. 
        :param new_process_param: 새롭게 정의할 processParam
        :type new_process_param: dictionary

        """
        self.process_param = new_process_param
    
    def get_bucket_meta(self, domain, sub_domain, mongo_instance):
        """
        bucket meta를 읽는다.
        :param domain: domain
        :type domain: string

        :param domain: sub_domain
        :type domain: string

        :param domain: mongo_instance
        :type domain: string

        :returns:bucket_meta 정보
        :rtype: dictionary

        """
        db_name ="bucket"
        collection_name = "meta_info"
        mongodb_c = wiz.WizApiMongoMeta(mongo_instance)
        bucket_meta = mongodb_c.read_mongodb_document_by_get(db_name, collection_name, domain+'_'+sub_domain)

        return bucket_meta
    
    def get_ms_meta(self, domain, sub_domain, mongo_instance, table_name):
        """
        MS meta를 읽는다.
        :param domain: domain
        :type domain: string

        :param domain: sub_domain
        :type domain: string

        :param domain: mongo_instance
        :type domain: string

        :returns:ms_meta 정보
        :rtype: dictionary

        """
        mongodb_c = wiz.WizApiMongoMeta(mongo_instance)
        ms_meta = mongodb_c.read_mongodb_document_by_get(domain, sub_domain, table_name)
        
        return ms_meta

    def get_ms_data_by_days(self, bucket_name, measurement_name, influx_instance):
        """
        Influx에서 시계열 데이터 인출하고 전처리 하여 전달 (1년)

        :param bucket_name: domain
        :type bucket_name: string

        :param measurement_name: sub_domain
        :type measurement_name: string

        :param influx_instance: influx_instance
        :type influx_instance: instance # TODO

        :returns: 결과 데이터
        :rtype: dataframe 

        """
        days = 365
        end_time = influx_instance.get_last_time(bucket_name, measurement_name)
        data_nopreprocessing = self.influx_instance.get_data_by_days(end_time, days, bucket_name, measurement_name)
        # preprocessing
        partialP = data_preprocessing.packagedPartialProcessing(self.process_param)
        dataframe = partialP.allPartialProcessing(data_nopreprocessing)["imputed_data"]

        return dataframe
    
    def get_ms_data(self, bucket_name, measurement_name, influx_instance):
        """
        Influx에서 시계열 데이터 인출하고 전처리 하여 전달 (1년)

        :param bucket_name: domain
        :type bucket_name: string

        :param measurement_name: sub_domain
        :type measurement_name: string

        :param influx_instance: influx_instance
        :type influx_instance: instance # TODO

        :returns: 결과 데이터
        :rtype: dataframe 

        """
        data_nopreprocessing = influx_instance.get_data(bucket_name, measurement_name)
        # preprocessing
        partialP = data_preprocessing.packagedPartialProcessing(self.process_param)
        dataframe = partialP.allPartialProcessing(data_nopreprocessing)["imputed_data"]

        return dataframe
    
