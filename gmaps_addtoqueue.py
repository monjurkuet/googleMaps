import mysql.connector

connection = mysql.connector.connect(
                              host='localhost', 
                              #host='161.97.97.183',
                              database='google_maps',
                              user='root', 
                              password='$C0NTaB0vps8765%%$#', 
                              port=3306
                              ,auth_plugin='caching_sha2_password')
cursor = connection.cursor()   