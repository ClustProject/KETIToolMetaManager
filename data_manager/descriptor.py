import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

from KETIToolMetaManager.data_manager import wizMongoDbApi as wiz

class WriteData():
    def __init__(self, metasave_info, meta_data):
        self.db = metasave_info["database"]
        self.mode = metasave_info["mode"]
        self.meta_data = meta_data
        self.ms_list = metasave_info["measurements"]

    def set_ms_meta(self):
        domain = self.db.split("_", maxsplit=1)[0]
        sub_domain = self.db.split("_", maxsplit=1)[1]
        mongodb_c = wiz.WizApiMongoMeta()
        
        if len(self.ms_list) == 1:
            mongodb_c.post_database_collection_document(self.mode, self.meta_data, domain, sub_domain)
        else:
            mongodb_c.post_database_collection_documents(self.mode, self.meta_data, domain, sub_domain)