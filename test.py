
import sys, json
from meta_generator.generator import MetaGenerator
from meta_generator.inte_generator import IntegrationGenerator
from influxdb_management.influx_crud import InfluxCRUD
from influxdb_management import influx_setting as ifs
from mongo_management.mongo_crud import MongoCRUD

if __name__=="__main__":
    from pprint import pprint

    with open('./meta_manager/config.json', 'r') as f:
        config = json.load(f)
    
    db_info = config['DB_INFO']
    mydb = MongoCRUD(db_info['USER_ID']\
        ,db_info["USER_PWD"],db_info["HOST_ADDR"]\
        ,db_info["HOST_PORT"],db_info["DB_NAME"])

    with open('./meta_generator/config.json', 'r') as f:
        gener_config = json.load(f)
    
    print("=== exploration metadata ===")
    gener = MetaGenerator(gener_config)
    
    data = {
    "domain" : "Environment",
    "sub_domain" : "Air",
    "have_location" : "True",
    "location" : {
      "syntax" : "경북 상주시 북천로 63 북문동주민센터"
    },
    "description" : "it is the open data ..",
    "source_agency":"Air Korea",
    "collector":"korea government",
    "source_type":"server",
    "method":"csv",
    "number_of_columns":6
    }
    metadata = gener.generate(data)
    collection_name = metadata["domain"]+"_"+metadata["sub_domain"]
    #mydb.insertOne(collection_name,metadata)

    colls = mydb.getCollList()
    print(colls)

    items = mydb.getManyData(collection_name)
    for item in items:
        pprint(item)
        print()
    
    print()
    # === integration metadata ===
    print("=== integration metadata ===")
    import pandas as pd
    import numpy as np
    # r_0 = pd.date_range(start='1/1/2018', end= '1/02/2018', freq='10T')
    
    # import random
    # original_list=['apple','orange','pineapple']
    # data_0 = {'datetime': r_0,
    #         'data0':np.random.randint(0, 100, size=(len(r_0))),
    #         'data1':np.random.randint(0, 100, size=(len(r_0))),
    #         'data2':np.random.randint(0, 100, size=(len(r_0))),
    #         'data3':random.choices (original_list, k=len(r_0))}

    # df0 = pd.DataFrame (data = data_0).set_index('datetime')
    # #print(df0)
    # gener = IntegrationGenerator(df0)
    
    # pprint(gener.get_column_meta())

    # === get sample data ===
    db_name = data['domain']+"_"+data['sub_domain']
    test = InfluxCRUD(ifs.host_, ifs.port_, ifs.user_,
                      ifs.pass_, db_name, ifs.protocol)
    print(test.get_all_db_measurements())
    #print(test.get_df_all("4a2dcd058cae2019b14b5ff38bbbe924847d39eeebd7bd269a0291b6"))
    sample = test.get_df_all("4a2dcd058cae2019b14b5ff38bbbe924847d39eeebd7bd269a0291b6")
    gener = IntegrationGenerator(sample)
    pprint(gener.get_column_meta())