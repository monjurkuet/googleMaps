import json
import mysql.connector
from fake_useragent import UserAgent
import requests as rq
import time
#
ua = UserAgent()
headers = {'user-agent': ua.random}

connection = mysql.connector.connect(#host='localhost', 
                              host='161.97.97.183',
                              database='location_data',
                              user='root', 
                              password='$C0NTaB0vps8765%%$#', 
                              port=3306
                              ,auth_plugin='caching_sha2_password')
cursor = connection.cursor()  

base_url='https://nominatim.openstreetmap.org/reverse?format=json&lat={latitude}&lon={longitude}&email=muhamad.manjur@outlook.com'

sql_select_Query = "SELECT DISTINCT latitude,longitude FROM `gmaps_details` where (latitude,longitude) not in (SELECT DISTINCT latitude,longitude FROM location_data.osm_reverse_geocode)"
cursor = connection.cursor()
cursor.execute(sql_select_Query)
geocodes = cursor.fetchall()   
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
   data_tuple = (str(address),display_name,latitude,longitude)
   cursor.execute(sqlite_insert_with_param, data_tuple)
   connection.commit()
   print(data_tuple)
   time.sleep(1)
   print('Done : ',COUNTER,' Out of : ',TOTAL)

connection.close()    