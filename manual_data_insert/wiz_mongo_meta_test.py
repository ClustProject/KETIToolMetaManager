import requests
import os
import json

class WizApiMongoMeta():
    def __init__(self, domain, subdomain, tablename=None):
        self.domain = domain
        self.subdomain = subdomain
        self.tablename = tablename

    # get - database/collection/document?table_name - 지정 table name 출력
    def get_database_collection_document_tablename(self):
        pass
    
    def get_database_collection_documents(self):
        url = "http://localhost:5000/rest/1.0/mongodb/documents/{}/{}".format(self.domain, self.subdomain)
        response = requests.get(url)
        print(response.status_code)
        print(response.text)



    # post - database/collection/document insert, save
    def post_database_collection_document(self, mode):
        ''' 
        - 새롭게 document 를 생성할땐 insert
        - 기존에 있는 document에 수정을 위해 덮어씌우기는 save
        - document에는 필수적으로 "table_name" 을 기입해야한다
        '''

        with open(os.path.dirname(os.path.realpath(__file__))+"/[20211008] indoor_kweather.json", "r", encoding="utf-8") as f:
                        db_meta_json_file = json.load(f)

        db_meta_json_file["table_name"] = self.tablename
        print(db_meta_json_file)

        url = "http://localhost:5000/rest/1.0/mongodb/document/{}/{}?mode={}".format(self.domain, self.subdomain, mode)
        data = db_meta_json_file
        headers = {'Content-Type': 'application/json'}
        res = requests.post(url, data=json.dumps(data), headers=headers)

        print(res.status_code)

if __name__ == "__main__":
    test = WizApiMongoMeta("test", "test", "ICL1L2000234")
    test.get_database_collection_document()
    #test.post_database_collection_document("insert")

    '''
    get - database/collection/document - 첫번째 document 만 출력
    get - database/collection/documents 는 head 5개만 출력 -> 갯수 변경하고 싶을 시 limit 수정하면 가능
    get - database/collection/document?table_name = 지정해주기 -> 원하는 table name의 document 출력
    '''