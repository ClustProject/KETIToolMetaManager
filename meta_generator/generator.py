import hashlib
import googlemaps, json

class MetaGenerator():
    def __init__(self,config) -> None:
        self.api_key = config['MAP_API_KEY']
        self.gmaps = googlemaps.Client(key=self.api_key)
        
    def createId(self,content):
        return hashlib.sha224(content).hexdigest()
    
    def geocoding(self,address):
        geocode_result = self.gmaps.geocode(('경북 상주시 북천로 63 북문동주민센터'), language='ko')
        return geocode_result[0]['geometry']['location']
    
    def reverse_geocoding(self,pos):
        reverse_geocode_result = self.gmaps.reverse_geocode((pos['lat'], pos['lng']),language='ko')
        return reverse_geocode_result[0]["formatted_address"]

if __name__=="__main__":

    with open('./meta_generator/config.json', 'r') as f:
        config = json.load(f)

    gener = MetaGenerator(config)
    print(gener.createId(b"Environment_Air_Korea_Government_KETI_API"))
    pos = gener.geocoding('경북 상주시 북천로 63 북문동주민센터')
    print(gener.reverse_geocoding(pos))