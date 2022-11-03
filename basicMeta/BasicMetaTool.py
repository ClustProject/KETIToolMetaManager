import sys
import os
import json
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
#from KETIToolMetaManager.metaDataManager.descriptor import WriteData

class metaGeneratorByMode():
    def __init__(self, metaUploadParam):
        self.generator_mode = metaUploadParam["generatorMode"]
        self.path = metaUploadParam["filePath"]
        self.filename = metaUploadParam["fileName"]
        self.ms_list = metaUploadParam["measurementsName"]
        self.domain = metaUploadParam["databaseName"].split("_", maxsplit=1)[0]
        self.sub_domain = metaUploadParam["databaseName"].split("_", maxsplit=1)[1]

    def get_meta_by_mode(self, additional_meta = None):
        if self.generator_mode == "onlyFile":
            meta = self.read_json_file_meta()
        elif self.generator_mode == "fileAndMeta":
            if self.ms_list == str:
                meta = self.integration_jsonfile_and_meta(additional_meta)
            else:
                meta = self.integration_jsonfile_and_meta_by_ms(additional_meta)
        elif self.generator_mode == "fileMetaWithoutDatainfo":
            meta = self.get_json_file_meta_without_datainfo()
        
        return meta

    def read_json_file_meta(self):
        with open(os.path.join(self.path, self.filename), "r", encoding="utf-8") as meta:
            file_meta = json.load(meta)
        return file_meta

    def integration_jsonfile_and_meta(self, additional_meta):
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
        file_meta = self.read_json_file_meta()
        file_meta.update(additional_meta)
        return file_meta

    def integration_jsonfile_and_meta_by_ms(self, additional_meta):
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
        int_metaset = []
        file_metaset = self.read_json_file_meta()
        for file_meta in file_metaset:
            file_meta.update(additional_meta)
            int_metaset.append(file_meta)
        return int_metaset

    def get_json_file_meta_without_datainfo(self):
        """
        Json File 로 메타 데이터가 저장되어 있지만, Json File 정보에 저장하고 싶은 데이터관련 table_name, database name이 존재 하지 않을 경우
        """
        file_meta = self.read_json_file_meta()
        
        file_meta["table_name"] = self.ms_list
        file_meta["domain"] = self.domain
        file_meta["subDomain"] = self.sub_domain
        
        return file_meta