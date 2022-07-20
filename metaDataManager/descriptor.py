import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

from KETIToolMetaManager.metaDataManager import wizMongoDbApi as wiz

class WriteData():
    def __init__(self, metasave_info, meta_data):
        self.db = metasave_info["databaseName"]
        self.mode = metasave_info["mode"]
        self.meta_data = meta_data
        self.ms_list = metasave_info["measurementsName"]
    
    def set_meta(self):
        domain = self.db.split("_", maxsplit=1)[0]
        sub_domain = self.db.split("_", maxsplit=1)[1]
        mongodb_c = wiz.WizApiMongoMeta()
        print(self.ms_list)
        if type(self.ms_list) == str:
            mongodb_c.post_database_collection_document(self.mode, self.meta_data, domain, sub_domain)
            print("SUCCESS")
        else:
            mongodb_c.post_database_collection_documents(self.mode, self.meta_data, domain, sub_domain)
            print("SUCCESS")