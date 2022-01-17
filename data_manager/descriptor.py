import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

from KETIToolMetaManager.data_manager import wizMongoDbApi as wiz

class WriteData():
    def __init__(self, database, tablename, mode, meta_data):
        self.db = database
        self.tablename = tablename
        self.mode = mode
        self.meta_data = meta_data
        
    def set_ms_meta(self):
        domain = self.db.split("_", maxsplit=1)[0]
        sub_domain = self.db.split("_", maxsplit=1)[1]
        mongodb_c = wiz.WizApiMongoMeta(domain, sub_domain, self.tablename)
        mongodb_c.post_database_collection_documents(self.mode, self.meta_data)