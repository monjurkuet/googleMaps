import json
from fake_useragent import UserAgent
import requests as rq
import time
import sqlite3
from tqdm import tqdm

ua = UserAgent()
headers = {'user-agent': ua.random}

conn = sqlite3.connect('database.db', check_same_thread=False)
cursor = conn.cursor()

base_url = 'https://nominatim.openstreetmap.org/reverse?format=json&lat={latitude}&lon={longitude}&email=muhamad.manjur@outlook.com'

googlemaps_select_Query = "SELECT id,gmaps_url FROM `gmaps_links` where id not in (Select id from url_geo_mapping)"

cursor.execute(googlemaps_select_Query)
urls_googlemaps = cursor.fetchall()

# parse locations
for id, gmaps_url in urls_googlemaps:
   try:
      try:
         latitude = float(gmaps_url.split("!3d")[1].split("!4d")[0])
      except:
         latitude = float(gmaps_url.split("@")[1].split(",")[0])
      try:
         longitude = float(gmaps_url.split("!3d")[1].split("!4d")[1].split("!")[-0])
      except:
         longitude = float(gmaps_url.split("@")[1].split(",")[1])
      print(id,latitude, longitude)
      sqlite_insert_with_param = """INSERT OR IGNORE INTO url_geo_mapping
                        (id,lat,lon) 
                        VALUES (?,?,?);"""
      data_tuple = (id, latitude, longitude)
      cursor.execute(sqlite_insert_with_param, data_tuple)
      conn.commit()
   except Exception as e:
      print(e)

# crawl location
location_select_Query = "SELECT id, lat, lon FROM url_geo_mapping WHERE NOT EXISTS (SELECT 1 FROM osm_reverse_geocode WHERE url_geo_mapping.lat = osm_reverse_geocode.latitude AND url_geo_mapping.lon = osm_reverse_geocode.longitude);"

cursor.execute(location_select_Query)
locations = cursor.fetchall()[::-1]


for id,latitude,longitude in tqdm(locations):
   try:
      try:
         response = rq.get(base_url.format(latitude=latitude,
                           longitude=longitude), headers=headers).json()
         address = response['address']
         display_name = response['display_name']
      except:
         display_name = address = json.dumps(response)
      sqlite_insert_with_param = """INSERT OR IGNORE INTO osm_reverse_geocode
                           (id,address,display_name,latitude,longitude) 
                           VALUES (?,?,?,?,?);"""
      data_tuple = (id,json.dumps(address), display_name, latitude, longitude)
      cursor.execute(sqlite_insert_with_param, data_tuple)
      conn.commit()
      print(data_tuple)
      time.sleep(1)
   except Exception as e:
      print(e)

conn.close()
