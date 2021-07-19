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
        '''{
        "domain" : "Environment",
        "sub_domain" : "Air",
        "have_loaction" : "True",
        "description" : "it is the open data ..",
        "source_agency":"Air Korea",
        "collector":"korea government",
        "source_type":"server",
        "method":"csv",
        "number_of_columns":6,
        }'''
        
        if(data["have_location"]=="True"):
            if("lat" in data["location"]):
                pass
            else:
                pos=self.geocoding(data["location"]["syntax"])
                pos["syntax"]=data["location"]["syntax"]
                data["location"]=pos
        data["_id"]=self.createId(str(data).encode('utf-8'))
        
        return data

if __name__=="__main__":

    with open('./meta_generator/config.json', 'r') as f:
        config = json.load(f)

    gener = MetaGenerator(config)

    data = {
        "domain" : "Environment",
        "sub_domain" : "Air",
        "have_location" : "True",
        "location":{
            "syntax":"경북 상주시 북천로 63 북문동주민센터"
        },
        "description" : "it is the open data ..",
        "source_agency":"Air Korea",
        "collector":"korea government",
        "source_type":"server",
        "method":"csv",
        "number_of_columns":6,
    }
    print(gener.generate(data))