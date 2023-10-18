import sqlite3
import pandas as pd
from geodatalocation import geodata_database_location
from datetime import datetime

conn = sqlite3.connect('database.db', check_same_thread=False)
cursor = conn.cursor()

conn_location = sqlite3.connect(geodata_database_location, check_same_thread=False)
cursor_location = conn_location.cursor()

sql_query = pd.read_sql_query ('SELECT * FROM `osm_reverse_geocode`', conn_location)
sql_query_gmapsdata = pd.read_sql_query ('SELECT * FROM gmaps_details', conn)

df_geocode = pd.DataFrame(sql_query)
df_gmapsdata=pd.DataFrame(sql_query_gmapsdata)

dictionary_geocode_normalized=[]

for i in range(len(df_geocode)):
    try:
        address=eval(df_geocode.loc[i,'address'])
        latitude=df_geocode.loc[i,'latitude']
        longitude=df_geocode.loc[i,'longitude']
        display_name=df_geocode.loc[i,'display_name']
        current_dictionary = pd.json_normalize(address).to_dict('index')[0]
        current_data={}
        current_data['latitude']=latitude
        current_data['longitude']=longitude
        for each_key in ['country','city','state','postcode']:
            if each_key in current_dictionary.keys():
                current_data[each_key]=current_dictionary[each_key]
            else:
                current_data[each_key]=None   
        dictionary_geocode_normalized.append(current_data) 
    except Exception as e:
        print(e)

df_geocode_normalized = pd.DataFrame.from_records(dictionary_geocode_normalized)
df_final=df_gmapsdata.merge(df_geocode_normalized, on=['latitude','longitude'], how='inner',sort=False, validate=None)

# remove duplicate by column
df_final=df_final.drop_duplicates(subset=['phone'], keep='first')

df_final.to_excel(f'{datetime.now().strftime("%d-%m-%y")}.xlsx') 