import numpy as np
import pandas as pd
import re
import datetime

class MetaAnalysis:
    
    def __init__(self) -> None:
        pass

    def recovery_time_frequency(td):
        re_compile = re.compile("(\d+)\ days (\d+)\:(\d+)\:(\d+)")
        _c = re_compile.search(td)
        _time = (int(_c.group(1))*60*60*24) + (int(_c.group(2))*60*60) + (int(_c.group(3))*60) + (int(_c.group(4)))
        tt = datetime.timedelta(0,_time,0)
        return tt
        
    def test():
        print("TEST")


if __name__=="__main__":
    ss = getattr(MetaAnalysis,'test')
    print(dir(MetaAnalysis))
    ss()
    print(ss.__name__)