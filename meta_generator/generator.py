import hashlib
import googlemaps, json

class MetaGenerator():
    def __init__(self,config) -> None:
        self.api_key = config['MAP_API_KEY']
        self.gmaps = googlemaps.Client(key=self.api_key)
        
    def createId(self,content):
        return hashlib.sha224(content).hexdigest()
    
    def geocoding(self,address):
        geocode_result = self.gmaps.geocode((address), language='ko')
        return geocode_result[0]['geometry']['location']
    
    def reverse_geocoding(self,pos):
        reverse_geocode_result = self.gmaps.reverse_geocode((pos['lat'], pos['lng']),language='ko')
        return reverse_geocode_result[0]["formatted_address"]
    
    def generate(self, data):
        '''
        {
            "domain" : "traffic",
            "sub_domain" : "seoul_subway",
            "table_name" : "line3_dongdae",
            "location" :{
            "lat" : "None",
            "lng" : "None",
            "syntax" : "서울특별시 중구 장충동2가 189-2"
            },
            "description" : "This is public data on the Seoul Metro, which has been divided monthly since 20210501",
            "source_agency" : "서울열린데이터광장",
            "source" : "None",
            "source_type" : "csv",
            "tag" : [
            "Traffic",
            "Seoul",
            "Subway",
            "Metro"
            ],
            "collectionFrequency":"day",
            #"column_characteristics":
        },'''
        #if(data["have_location"]=="True"):
        # if("lat" in data["location"]):
        #     pass
        if(data["location"]["syntax"] is not None):
            pos=self.geocoding(data["location"]["syntax"])
            pos["syntax"]=data["location"]["syntax"] #["syntax"]
            data["location"]=pos
        #data["_id"]=self.createId(str(data).encode('utf-8'))
        
        return data

if __name__=="__main__":

    with open('./meta_generator/config.json', 'r') as f:
        config = json.load(f)

    gener = MetaGenerator(config)

    # data = {
    #     "domain" : "Environment",
    #     "sub_domain" : "Air",
    #     "have_location" : "True",
    #     "location":{
    #         "syntax":"경북 상주시 북천로 63 북문동주민센터"
    #     },
    #     "description" : "it is the open data ..",
    #     "source_agency":"Air Korea",
    #     "collector":"korea government",
    #     "source_type":"server",
    #     "method":"csv",
    #     "number_of_columns":6,
    # }

    '''
    {
        "domain" : "traffic",
        "sub_domain" : "seoul_subway",
        "table_name" : "line3_dongdae",
        "location" :{
        "lat" : "None",
        "lng" : "None",
        "syntax" : "서울특별시 중구 장충동2가 189-2"
        },
        "description" : "This is public data on the Seoul Metro, which has been divided monthly since 20210501",
        "source_agency" : "서울열린데이터광장",
        "source" : "None",
        "source_type" : "csv",
        "tag" : [
        "Traffic",
        "Seoul",
        "Subway",
        "Metro"
        ],
        "collectionFrequency":"day",
        #"column_characteristics":
    },'''
    # data= {'main_domain': 'HealthCare', 
    #     'sub_domain': 'Covid', 
    #     'select_time': '자치구 기준일', 
    #     'location': 'Seoul', 
    #     'description': 'Covid Infected person', 
    #     'source_agency': '질병청', 
    #     'collector': 'kaggle', 
    #     'source_type': 'CSV', 
    #     'createdAt': '1627021951.7419167'}
    new_data = {
        "domain" : "traffic",
        "sub_domain" : "seoul_subway",
        "table_name" : "line3_dongdae",
        "location" :{
        "lat" : "None",
        "lng" : "None",
        "syntax" : "서울특별시 중구 장충동2가 189-2"
        },
        "description" : "This is public data on the Seoul Metro, which has been divided monthly since 20210501",
        "source_agency" : "서울열린데이터광장",
        "source" : "None",
        "source_type" : "csv",
        "tag" : [
        "Traffic",
        "Seoul",
        "Subway",
        "Metro"
        ],
        "collectionFrequency":"day",
        #"column_characteristics":
    }
    print(gener.generate(new_data))