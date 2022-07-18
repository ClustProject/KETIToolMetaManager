# backend run.py 실행 후 http://localhost:5000/rest/1.0 실행
import requests
import os
import json
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
from KETIPreDataIngestion.KETI_setting.influx_setting_KETI import wiz_url

class WizApiMongoMeta():
    # get - database list
    def get_database_list(self):
        url = wiz_url+"/rest/1.0/mongodb/{}".format("databases")
        header = {'accept': 'application/json'}
        response = requests.get(url, headers=header)
        print(response.status_code)
        text = response.text

        return json.loads(text)
    
    def get_collection_list(self, domain):
        url = wiz_url+"/rest/1.0/mongodb/collections/{}".format(domain)
        header = {'accept': 'application/json'}
        response = requests.get(url, headers=header)
        print(response.status_code)
        text = response.text

        return json.loads(text)

    # get - database/collection/document?table_name - 지정 table name 출력
    def get_database_collection_document(self, domain, subdomain, tablename=None):
        url = wiz_url+"/rest/1.0/mongodb/document/{}/{}?table_name={}".format(domain, subdomain, tablename)
        response = requests.get(url)
        print(response.status_code)
        text = response.text

        return json.loads(text)
    
    def get_database_collection_documents(self, domain, subdomain):
        url = wiz_url+"/rest/1.0/mongodb/documents/{}/{}".format(domain, subdomain)
        response = requests.get(url)
        print(response.status_code)
        text = response.text

        return json.loads(text)

    # post - database/collection/document insert, save
    def post_database_collection_document(self, mode, data, domain, subdomain):

        url = wiz_url+"/rest/1.0/mongodb/document/{}/{}?mode={}".format(domain, subdomain, mode)
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, data=json.dumps(data), headers=headers)

        print(response.status_code)
        
    def post_database_collection_documents(self, mode, data, domain, subdomain):
    
        url = wiz_url+"/rest/1.0/mongodb/documents/{}/{}?mode={}".format(domain, subdomain, mode)
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, data=json.dumps(data), headers=headers)

        print(response.status_code)

if __name__ == "__main__":
    from pprint import pprint
    test = WizApiMongoMeta()
    import json
    
    meta = test.get_database_collection_document("air", "indoor_유치원", "ICW0W2000132")
    print(meta)
    '''
    get - database/collection/document - 첫번째 document 만 출력
    get - database/collection/documents 는 head 5개만 출력 -> 갯수 변경하고 싶을 시 limit 수정하면 가능
    get - database/collection/document?table_name = 지정해주기 -> 원하는 table name의 document 출력

    post - 새롭게 document 를 생성할땐 insert
    post - 기존에 있는 document에 수정을 위해 덮어씌우기는 save
    post - document에는 필수적으로 "table_name" 을 기입해야한다

    '''