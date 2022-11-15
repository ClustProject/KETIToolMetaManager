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

    def get_tableName_list(self, domain, subdomain):
        url = wiz_url + "/rest/1.0/mongodb/tableNames/{}/{}".format(domain, subdomain)
        header = {'accept': 'application/json'}
        response = requests.get(url, headers=header)
        #print(response.status_code)
        text = response.text

        return json.loads(text)

    # get - database/collection/document?table_name - 지정 table name 출력
    def read_mongodb_document_by_get(self, domain, subdomain, tableName=None):
        # TODO 아래 주석 수정할 것
        """
        :param domain: database
        :type domain: string

        :param subdomain: database
        :type subdomain: string

        :param tableName: database
        :type tableName: string or None

        - type(tableName) == string : 특정 table만 읽어옴
        - tableName == None : domain/subdomain(Collection) 아래 모든 table을 읽어옴

        :return: result
        :rtype: dictionary (single) or array (multiple)

        """

        if tableName: #one document
            url = wiz_url+"/rest/1.0/mongodb/document/{}/{}?table_name={}".format(domain, subdomain, tableName)
        else: #all documents under domain/subdomain/
            url = wiz_url+"/rest/1.0/mongodb/documents/{}/{}".format(domain, subdomain)

        response = requests.get(url)
        print(response.status_code)
        text = response.text
        result = json.loads(text)

        return result

    # post - database/collection/document insert, save
    def save_mongodb_document_by_post(self, mode, data, domain, subdomain):
        """
        mongodb의 document를 post로 저장함

        :param mode: data를 mongodb에 처리 하기 위한 방법 [update|insert|save]
        :type mode: string

        :param data: mongodb에 처리할 data
        :type data: dictionary or array[dictionaries]

        :param domain: domain, mongodb의 database 이름
        :type domain: string

        :param subdomain: subdomain, mongodb의 collection 이름
        :type subdomain: string

        """
        if type(data) is dict: #one dictionary document
            print("single document upload")
            print(data)
            self.updateDocumnetByTable(mode, data, domain, subdomain)
        elif isinstance(data, list): #multiple dictonary documents
            print("multiple documents upload")
            for oneData in data:
                self.updateDocumnetByTable(mode, oneData, domain, subdomain)

    def updateDocumnetByTable(self, mode, data, domain, subdomain):
        """
        dictionary data 단위로 데이터 저장

        :param mode: data를 mongodb에 처리 하기 위한 방법 [update|insert|save]
        :type mode: string

        :param data: mongodb에 처리할 data
        :type data: string

        :param domain: domain, mongodb의 database 이름
        :type domain: string

        :param subdomain: subdomain, mongodb의 collection 이름
        :type subdomain: string

        """
        headers = {'Content-Type': 'application/json'}
        
        if "table_name" in data.keys():
            url = wiz_url+"/rest/1.0/mongodb/document/{}/{}?mode={}".format(domain, subdomain, mode)
            response = requests.post(url, data=json.dumps(data), headers=headers)
            print("Success:", data['table_name'], response.status_code)
        else:
            print("This dictionary data does not have table_name.")


if __name__ == "__main__":
    from pprint import pprint
    test = WizApiMongoMeta()
    import json
    
    meta = test.read_mongodb_document_by_get("air", "indoor_유치원", "ICW0W2000132")
    print(meta)
    '''
    get - database/collection/document - 첫번째 document 만 출력
    get - database/collection/documents 는 head 5개만 출력 -> 갯수 변경하고 싶을 시 limit 수정하면 가능
    get - database/collection/document?table_name = 지정해주기 -> 원하는 table name의 document 출력

    post - 새롭게 document 를 생성할땐 insert
    post - 기존에 있는 document에 수정을 위해 덮어씌우기는 save
    post - document에는 필수적으로 "table_name" 을 기입해야한다

    '''