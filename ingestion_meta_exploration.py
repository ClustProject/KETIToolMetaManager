import sys, os
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
from KETIToolMetaManager.mongo_management.mongo_crud import MongoCRUD

def get_meta_table(db_info):
    main_domian_list =  ['air', 'farm', 'factory', 'bio', 'life', 'energy',\
         'weather', 'city', 'traffic', 'culture', 'economy']
    mydb = MongoCRUD(db_info)
    db_list = mydb.getDBList()
    exploration_df = pd.DataFrame()

    for db_name in db_list :
        if db_name in main_domian_list:
            mydb.switchDB(db_name)
            colls = mydb.getCollList()
            for coll in colls:
                items = mydb.getManyData(coll)
                for item in items:
                    influx_db_name = item['domain']+"_"+item["sub_domain"]
                    measurement_name = item['table_name']
                    start_time = item['start_time']
                    end_time = item['end_time']
                    frequency = item['frequency']
                    number_of_columns = item['number_of_columns']
                    
                    #exploration_df = exploration_df.append([[influx_db_name, measurement_name, start_time, end_time, frequency, number_of_columns]])
    
#    exploration_df.columns = ['db_name', 'measurement_name', 'start_time', 'end_time', 'frequency', 'number_of_columns']
    exploration_df.reset_index(drop=True, inplace = True)
    exploration_js = exploration_df.to_json(orient = 'records')
    
    return exploration_js

if __name__=="__main__":
    import json
    with open('KETIPreDataIngestion/KETI_setting/config.json', 'r') as f:
        config = json.load(f)
    
    exploration_df = get_meta_table(config['DB_INFO'])
    print(exploration_df)
    print(exploration_df.columns)
    