# backend run.py 실행 후 http://localhost:5000/rest/1.0 실행
import requests
import os
import json
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))))
from KETIPreDataIngestion.KETI_setting.influx_setting_KETI import wiz_url

class WizApiMongoMeta():
    def __init__(self, domain, subdomain, tablename=None):
        self.domain = domain
        self.subdomain = subdomain
        self.tablename = tablename

    # get - database/collection/document?table_name - 지정 table name 출력
    def get_database_collection_document(self):
        url = wiz_url+"/rest/1.0/mongodb/document/{}/{}?table_name={}".format(self.domain, self.subdomain, self.tablename)
        response = requests.get(url)
        print(response.status_code)
        text = response.text

        return json.loads(text)
    
    def get_database_collection_documents(self):
        url = wiz_url+"/rest/1.0/mongodb/documents/{}/{}".format(self.domain, self.subdomain)
        response = requests.get(url)
        print(response.status_code)
        text = response.text

        return json.loads(text)

    # post - database/collection/document insert, save
    def post_database_collection_document(self, mode, data):

        url = wiz_url+"/rest/1.0/mongodb/document/{}/{}?mode={}".format(self.domain, self.subdomain, mode)
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, data=json.dumps(data), headers=headers)

        print(response.status_code)

if __name__ == "__main__":
    from pprint import pprint
    test = WizApiMongoMeta("air", "indoor_요양원", "IS70W2000849")
    doc = test.get_database_collection_document()
    
    print("======Data Collection Document======")
    pprint(doc)
    
    # print("======DataBase Collection Document======")
    # pprint(doc)
    #test.post_database_collection_document("insert")

    '''
    get - database/collection/document - 첫번째 document 만 출력
    get - database/collection/documents 는 head 5개만 출력 -> 갯수 변경하고 싶을 시 limit 수정하면 가능
    get - database/collection/document?table_name = 지정해주기 -> 원하는 table name의 document 출력

    post - 새롭게 document 를 생성할땐 insert
    post - 기존에 있는 document에 수정을 위해 덮어씌우기는 save
    post - document에는 필수적으로 "table_name" 을 기입해야한다

    '''