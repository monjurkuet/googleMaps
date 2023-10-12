import json
from fake_useragent import UserAgent
import requests as rq
import time
from geodatalocation import geodata_database_location
import sqlite3
from tqdm import tqdm

ua = UserAgent()
headers = {'user-agent': ua.random}

conn = sqlite3.connect('database.db', check_same_thread=False)
cursor = conn.cursor()

conn_location = sqlite3.connect(geodata_database_location, check_same_thread=False)
cursor_location = conn_location.cursor()

base_url = 'https://nominatim.openstreetmap.org/reverse?format=json&lat={latitude}&lon={longitude}&email=muhamad.manjur@outlook.com'

googlemaps_select_Query = "SELECT DISTINCT latitude,longitude FROM `gmaps_details`"
location_select_Query = "SELECT latitude,longitude FROM osm_reverse_geocode"

cursor.execute(googlemaps_select_Query)
geocodes_googlemaps = cursor.fetchall()

cursor_location.execute(location_select_Query)
geocodes_osm = cursor_location.fetchall()

data_to_crawl = list(set(geocodes_googlemaps).symmetric_difference(set(geocodes_osm)))

for latitude, longitude in tqdm(data_to_crawl):
   try:
      print(latitude, longitude)
      response = rq.get(base_url.format(latitude=latitude,
                        longitude=longitude), headers=headers).json()
      address = response['address']
      display_name = response['display_name']
   except:
      display_name = address = json.dumps(response)
   sqlite_insert_with_param = """INSERT OR IGNORE INTO osm_reverse_geocode
                        (address,display_name,latitude,longitude) 
                        VALUES (?,?,?,?);"""
   data_tuple = (json.dumps(address), display_name, latitude, longitude)
   cursor_location.execute(sqlite_insert_with_param, data_tuple)
   conn_location.commit()
   print(data_tuple)
   time.sleep(1)

conn.close()
