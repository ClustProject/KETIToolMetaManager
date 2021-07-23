from datetime import timedelta
import pandas as pd
import numpy as np
from pandas.tseries.frequencies import to_offset

class IntegrationGenerator():
    def __init__(self,data, freq_check_length = 5) -> None:
        self.dataFrame = data
        #print(self.dataFrame)
        self.freq_check_length = freq_check_length 
        #print(self.dataFrame[self.dataFrame.columns[0]])
        self.partial_data_set = []
        for col in self.dataFrame.columns:
            self.partial_data_set.append(self.dataFrame[[col]])

        self.column_meta={}
        self.column_meta['number_of_columns']=len(self.dataFrame.columns)
        self.column_meta['overap_duration'] = self._get_partial_data_set_start_end()
        self.column_meta['column_characteristics'] = self._get_partial_data_freqeuncy_list()

    def get_column_meta(self):
        return self.column_meta
        
    def _get_partial_data_freqeuncy_list(self):
    
        data_length = len(self.partial_data_set)
        column_list={}
        for i in range(data_length):
            data = self.partial_data_set[i]
            columns = data.columns
            for column in columns:
                column_info = {"column_name":'', "column_frequency":'', "column_type":''}
                column_info['column_name'] = column
                freq = self.get_df_freq_timedelta(data)
                column_info['column_frequency'] = str(freq)
                if freq is not None:
                    column_info['occurence_time'] = "Continuous"
                else:
                    column_info['occurence_time'] = "Event" 
                column_info['pointDependency']="Yes" #default

                column_type = data[column].dtype
                column_info['column_type'] = str(column_type)
                if column_type == np.dtype('O') :
                    column_info['upsampling_method']= 'ffill' #default
                    column_info['downsampling_method']='ffill' #default
                    column_info['fillna_function']= 'bfill' #default
                else:
                    column_info['upsampling_method']=str(np.mean) #default
                    column_info['downsampling_method']=str(np.mean) #default
                    column_info['fillna_function']= 'interpolate' #default

                column_info['fillna_limit'] = 31 #default
                column_list[column] = column_info

        return column_list

    def _get_partial_data_set_start_end(self):
        start_list=[]
        end_list =[]
        duration={}
        data_length = len(self.partial_data_set)
        for i in range(data_length):
            data =self.partial_data_set[i]
            start = data.index[0]
            end = data.index[-1]
            start_list.append(start)
            end_list.append(end)
        duration['start_time'] = str(max(start_list))
        duration['end_time'] = str(min(end_list))
        return duration
    
    def get_df_freq_sec(self,data):
        freq = to_offset(pd.infer_freq(data[:self.freq_check_length].index))
        freq_sec = pd.to_timedelta(freq, errors='coerce').total_seconds()
        return freq_sec

    def get_df_freq_timedelta(self,data):
        freq = to_offset(pd.infer_freq(data[:self.freq_check_length].index))
        freq_timedelta = pd.to_timedelta(freq, errors='coerce')
        return freq_timedelta


if __name__=="__main__":
    from pprint import pprint
    import datetime, re
    r_0 = pd.date_range(start='1/1/2018', end= '1/05/2018', freq='1440T')
    print(r_0)
    import random
    original_list=['apple','orange','pineapple']
    data_0 = {'datetime': r_0,
            'data0':np.random.randint(0, 100, size=(len(r_0))),
            'data1':np.random.randint(0, 100, size=(len(r_0))),
            'data2':np.random.randint(0, 100, size=(len(r_0))),
            'data3':random.choices (original_list, k=len(r_0))}

    df0 = pd.DataFrame (data = data_0).set_index('datetime')
    #print(df0)
    genre = IntegrationGenerator(df0)
    res = genre.get_column_meta()
    
    ## Recovery
    pprint(res)
    td = res['column_characteristics']['data1']['column_frequency']
    cn = res['column_characteristics']['data1']['column_type']
    dm = res['column_characteristics']['data1']['downsampling_method']
    um = res['column_characteristics']['data1']['upsampling_method']
    pprint(td)

    arr = [10, 20, 30]
    print('np.mean()')
    #print(str(np.mean))
    res = eval('print(np.mean(arr))')
    print(res)

    re_compile = re.compile("(\d+)\ days (\d+)\:(\d+)\:(\d+)")
    _c = re_compile.search(td)
    _time = (int(_c.group(1))*60*60*24) + (int(_c.group(2))*60*60) + (int(_c.group(3))*60) + (int(_c.group(4)))
    print(_time)
   
    tt = datetime.timedelta(0,_time,0)
    print(tt)