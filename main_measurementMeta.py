import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))

if __name__ == '__main__':
    from KETIToolMetaManager.measurementMeta.metaSetGenerator import AnalysisMetaControl
