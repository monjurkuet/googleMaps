import mysql.connector
import pandas as pd

connection = mysql.connector.connect(host='localhost', 
                              #host='161.97.97.183',
                              database='google_maps',
                              user='root', 
                              password='$C0NTaB0vps8765%%$#', 
                              port=3306
                              ,auth_plugin='caching_sha2_password')
cursor = connection.cursor()  

sql_query = pd.read_sql_query ('SELECT * FROM `osm_reverse_geocode` WHERE latitude not in (SELECT latitude from geocode_data) and longitude not in (SELECT longitude from geocode_data)', connection)

df_geocode = pd.DataFrame(sql_query)
dictionary_geocode_normalized=[]

for i in range(len(df_geocode)):
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

df_geocode_normalized = pd.DataFrame.from_records(dictionary_geocode_normalized)

cols = "`,`".join([str(i) for i in df_geocode_normalized.columns.tolist()])
for i,row in df_geocode_normalized.iterrows():
    sql = "INSERT IGNORE INTO `geocode_data` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql, tuple(row))
    connection.commit()
    print(row)

connection.close()    