import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

from KETIToolMetaManager.metaDataManager import wizMongoDbApi as wiz

def write_data(uploadParam, meta_data):
    """
    mongoDB에 필요한 데이터를 writing

    :param uploadParam: 데이터를 쓰기 위해 필요한 파라미터
    :type uploadParam: dictionary

    >>> uploadParam = {
        "dbName":"farm",
        "collectionName":"swine_air",
        "mode" : "update"# insert / update / save
    }

    :param meta_data: 쓰기 위한 data
    :type meta_data: dictionary

    """

    write_mode = uploadParam["mode"]
    dbName = uploadParam["dbName"]
    collectionName = uploadParam["collectionName"]

    mongodb_c = wiz.WizApiMongoMeta()
    mongodb_c.save_mongodb_document_by_post(write_mode, meta_data, dbName, collectionName)
    print("SUCCESS")

"""  
def write_data(metasave_info, meta_data):
    db_name = metasave_info["databaseName"]
    write_mode = metasave_info["mode"]
    ms_list = metasave_info["tableName"]
    
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
"""    