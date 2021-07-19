from influxdb import DataFrameClient, InfluxDBClient
import pandas as pd

class InfluxCRUD:
    def __init__(self, host='localhost', port=8086,\
                id='admin', pwd='admin', dbname='test', protocol='line'):
        self.host = host
        self.port = port
        self.id = id
        self.pwd = pwd
        self.dbname = dbname
        self.protocol = protocol
        self.client = self.connect_DB()
        self.check_db()

    ## Connect
    def connect_DB(self):
        return DataFrameClient(self.host,self.port,self.id,self.pwd,self.dbname)

    def check_db(self):
        if {"name": self.dbname} in self.get_all_db_list():
            print("The db exists already")
        else:
            print("Create the db named "+self.dbname)
            self.client.create_database(self.dbname)

    def check_start(self,feature,table):
        if {"name": table} in self.get_all_db_measurements():
            query = "select last("+feature+") from "+table
            ori = self.client.query(query)[table]
            lastDay = ori.index[0].strftime('%Y%m%d')
            print(lastDay)
            return lastDay
        return '20190101'
    
    ## Read
    def get_all_db_list(self):
        return self.client.get_list_database()
    
    def get_all_db_measurements(self):
        return self.client.get_list_measurements()
        
    def get_df_by_time(self, table, time_start, time_end):
        query_string = 'select * from "'+ table +'" where time>="' + time_start+'" and time<="'+time_end+'" ' 
        return self.get_query(query_string,table)
    
    def get_df_by_timestamp(self, table, time_start, time_end):
        query_string = 'SELECT * FROM "' + table + \
            '" where time > ' + time_start + ' AND time < '+ time_end
        return self.get_query(query_string, table)
    
    def get_df_all(self, table):
        query_string = "select * from "+ table 
        return self.get_query(query_string,table)
    
    def get_query(self,query,table):
        result = self.client.query(query)[table]
        result.index.names = ['time']
        #print("Data Length:", len(result))
        result = result.groupby(result.index).first()
        result.index = pd.to_datetime(result.index)
        result = result.drop_duplicates(keep='last')
        #print("After removing duplicates:", len(result))
        result = result.sort_index(ascending=True)
        return result
    
    ## Update
    def write_db(self, df, table):
        self.client.write_points(df, table, protocol=self.protocol)

    ## Delete
    def drop_db(self,dbname):
        print("Drop the DB named "+dbname)
        self.client.drop_database(dbname)

    def drop_and_recreate_db(self, dbname, result):
        self.client.drop_database(dbname)
        self.client.create_database(dbname)
        if len(result) >0:
             self.client.write_points(result, dbname, protocol=self.protocol )
        return result

    ## Setting
    def change_db(self,dbname):
        self.dbname = dbname
        self.client = self.connect_DB()
        self.check_db()
        print('Switch to DB named '+dbname)

    def change_user(self,id,pwd):
        self.id = id
        self.pwd = pwd
        self.client.switch_user(self.id, self.pwd)


if __name__ == "__main__":
    import influx_setting as ifs
    test = InfluxCRUD(ifs.host_, ifs.port_, ifs.user_,
                      ifs.pass_, "OUTDOOR_AIR", ifs.protocol)
    print(test.get_all_db_measurements())
    print(test.get_all_db_list())

    print(test.get_df_all("sangju"))
    test.change_db("OUTDOOR_RIVER")
    
    print(test.check_start('conductivity', 'pyeongchang'))
    print(test.get_df_all("pyeongchang"))
    
    
