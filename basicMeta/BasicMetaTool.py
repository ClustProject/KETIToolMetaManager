import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

import json

class MetaGenerator():
    def __init__(self):
        pass
    
    def create_additional_meta(self, basic_meta, additional_meta):
        """
        - 기존 존재하는 Meta에 추가 Meta 기입을 통해 저장할 최종 Meta를 생성하는 함수
        - 기존 Meta에는 TableName이 필수로 작성되어 있어야함

        :param basic_meta: 기존 존재하는 Meta로 TableName이 필수로 기입되어 있어야 함
        :type basic_meta: dictionary
        
        :param additional_meta : 추가로 기입하고 싶은 정보를 담은 Meta
        :type additional_meta : dictionary

        :returns: basic_meta
        :rtype: dictionary
        """
        basic_meta.update(additional_meta)
        return basic_meta
    
    def create_additional_meta_by_tablename(self, basic_metaset, additional_meta):
        """
        - 기존 존재하는 MetaSet에 TableName 별 추가 Meta 기입을 통해 저장할 최종 Meta를 생성하는 함수
        - 기존 MetaSet의 각 Meta별 TableName이 필수로 작성되어 있어야함

        :param basic_metaset: 기존 존재하는 Meta로 TableName이 필수로 기입되어 있어야 함
        :type basic_metaset: list
        
        :param additional_meta : 추가로 기입하고 싶은 정보를 담은 Meta
        :type additional_meta : dictionary

        :returns: metaset
        :rtype: list
        """
        metaset = []
        for basic_meta in basic_metaset:
            meta = self.create_additional_meta(basic_meta, additional_meta)
            metaset.append(meta)
        return metaset
    
    def save_db_label_meta(self, path, filename, database_info_param):
        """
        DataBase의 컬럼별 Label 정보 및 기본 컬럼 정보를 담은 Meta 를 생성 및 저장 하는 함수
        
        :param path: Label 정보 및 기본 컬럼 정보를 담은 Json 파일의 주소
        :type path: string
        
        :param filename: Label 정보 및 기본 컬럼 정보를 담은 Json 파일 명
        :type filename: string
        
        :param database_info_param: 해당 데이터베이스의 정보 파라미터
        :type database_info_param: dictionary
        
        >>> database_info_param = {
                "databaseName" : air_indoor_체육시설,
                "measurementsName" : "db_information",
                "mode" : "insert"
            }
        """
        from KETIToolMetaManager.metaDataManager.descriptor import WriteData
        
        with open(os.path.join(path, filename), "r", encoding="utf-8") as f:
            db_meta_json = json.load(f)

        domain = database_info_param["databaseName"].split("_", maxsplit=1)[0]
        sub_domain = database_info_param["databaseName"].split("_", maxsplit=1)[1]

        db_meta_json["table_name"] = "db_information"
        db_meta_json["domain"] = domain
        db_meta_json["subDomain"] = sub_domain
        
        WriteData(database_info_param, db_meta_json).set_meta()

    # JIAN ####################################################################################

    def save_db_label_meta2(self, path, filename, database_info_param):
        """
        DataBase의 컬럼별 Label 정보 & 기본 컬럼 정보를 담은 Meta 를 생성 및 저장 하는 함수
        
        현재는 db_meta_json 을 물리 파일에서 추출하고 있으나, 
        추후 파라미터를 통해 라벨 정보를 넘길 예정이다.
        """
        from KETIToolMetaManager.metaDataManager.descriptor import WriteData
        
        with open(os.path.join(path, filename), "r", encoding="utf-8") as f:
            db_meta_json = json.load(f)

        domain = database_info_param["databaseName"].split("_", maxsplit=1)[0]
        sub_domain = database_info_param["databaseName"].split("_", maxsplit=1)[1]

        db_meta_json["table_name"] = "db_information"
        db_meta_json["domain"] = domain
        db_meta_json["subDomain"] = sub_domain
        
        WriteData(database_info_param, db_meta_json).set_meta()


    # JIAN end ####################################################################################
    