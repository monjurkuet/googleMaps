import json
from fake_useragent import UserAgent
import requests as rq
import time
from geodatalocation import geodata_database_location
import sqlite3

ua = UserAgent()
headers = {'user-agent': ua.random}

conn = sqlite3.connect('database.db', check_same_thread=False)
cursor = conn.cursor()

conn_location = sqlite3.connect(geodata_database_location, check_same_thread=False)
cursor_location = conn_location.cursor()

base_url='https://nominatim.openstreetmap.org/reverse?format=json&lat={latitude}&lon={longitude}&email=muhamad.manjur@outlook.com'

googlemaps_select_Query = "SELECT DISTINCT latitude,longitude FROM `gmaps_details` where (latitude,longitude) not in (SELECT DISTINCT latitude,longitude FROM location_data.osm_reverse_geocode)"
location_select_Query = "SELECT latitude,longitude FROM location_data.osm_reverse_geocode)"

cursor.execute(googlemaps_select_Query)
geocodes_googlemaps = cursor.fetchall()   

list(set(x).symmetric_difference(set(f)))

TOTAL=len(geocodes)
COUNTER=0

for latitude,longitude in geocodes:
   COUNTER+=1
   try: 
    print(latitude,longitude)
    response=rq.get(base_url.format(latitude=latitude,longitude=longitude),headers=headers).json()
    address=response['address']
    display_name=response['display_name']
   except:
    display_name=address=str(response) 
   sqlite_insert_with_param = """INSERT IGNORE INTO osm_reverse_geocode
                          (address,display_name,latitude,longitude) 
                          VALUES (%s,%s,%s,%s);""" 
   data_tuple = (json.dumps(address),display_name,latitude,longitude)
   cursor.execute(sqlite_insert_with_param, data_tuple)
   connection.commit()
   print(data_tuple)
   time.sleep(1)
   print('Done : ',COUNTER,' Out of : ',TOTAL)

connection.close()    