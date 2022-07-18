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
        - 기존 존재하는 Meta에 추가 Meta 기입을 통해 저장할 최종 Meta를 생성하는 모듈
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
        - 기존 존재하는 MetaSet에 TableName 별 추가 Meta 기입을 통해 저장할 최종 Meta를 생성하는 모듈
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
    
    def kweather_data_column_info_meta_save(upload_param):
        """
        케이웨더 데이터베이스의 정보를 읽은 후 Meta 데이터로 저장하는 함수
        
        uploadParam = {
            "databaseName" : domain+"_"+sub_domain,
            "mode" : "insert"
        }
        """
        
        if "indoor" in subdomain:
            with open(os.path.dirname(os.path.realpath('__file__'))+"/indoor_kweather_basic_meta_ver3.json", "r", encoding="utf-8") as f:
                db_meta_json = json.load(f)
        else:
            with open(os.path.dirname(os.path.realpath('__file__'))+"/outdoor_kweather_basic_meta_ver3.json", "r", encoding="utf-8") as f:
                db_meta_json = json.load(f)

        db_meta_json["table_name"] = "db_information"
        db_meta_json["domain"] = domain
        db_meta_json["subDomain"] = subdomain

        mongodb_c = wiz.WizApiMongoMeta()
        mongodb_c.post_database_collection_document(mode, db_meta_json, domain, subdomain)
    