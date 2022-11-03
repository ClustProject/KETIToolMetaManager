import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

from KETIToolMetaManager.metaDataManager import wizMongoDbApi as wiz

def write_data(metasave_info, meta_data):
    db_name = metasave_info["databaseName"]
    write_mode = metasave_info["mode"]
    ms_list = metasave_info["measurementsName"]
    
    domain = db_name.split("_", maxsplit=1)[0]
    sub_domain = db_name.split("_", maxsplit=1)[1]
    mongodb_c = wiz.WizApiMongoMeta()
    
    print(ms_list)
    if type(ms_list) == str:
        mongodb_c.post_database_collection_document(write_mode, meta_data, domain, sub_domain)
        print("SUCCESS")
    else:
        mongodb_c.post_database_collection_documents(write_mode, meta_data, domain, sub_domain)
        print("SUCCESS")