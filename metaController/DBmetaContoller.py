import sys
import os
import json
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
#from KETIToolMetaManager.metaDataManager.descriptor import WriteData

class metaController():
    def __init__(self, metaUploadParam):
        self.path = metaUploadParam["filePath"]
        self.filename = metaUploadParam["fileName"]
        self.ms_list = metaUploadParam["measurementsName"]
        self.domain = self.upload_param["dbName"]
        self.sub_domain = self.upload_param["collectionName"]

            
    # JIAN ####################################################################################

    def saveDBMeta(self, path, filename, database_info_param):
        """
        DataBase의 컬럼별 Label 정보 & 기본 컬럼 정보를 담은 Meta 를 생성 및 저장 하는 함수
        
        현재는 db_meta_json 을 물리 파일에서 추출하고 있으나, 
        추후 파라미터를 통해 라벨 정보를 넘길 예정이다.
        """
        from KETIToolMetaManager.metaDataManager import descriptor
       
        domain = database_info_param["dbName"]
        sub_domain = database_info_param["collectionName"]

        """
        db_meta_json["table_name"] = "db_information"
        db_meta_json["domain"] = domain
        db_meta_json["subDomain"] = sub_domain

        """
        
        descriptor.write_data(database_info_param, {}).set_meta()


    # JIAN end ####################################################################################