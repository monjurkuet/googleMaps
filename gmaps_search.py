import time,re,random
from time import sleep
import undetected_chromedriver as uc
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import mysql.connector
from urllib.parse import urlparse
import json
from datetime import datetime
from websocket_server import WebsocketServer
import sys
import time
import threading
from multiprocessing.pool import ThreadPool, Pool
from threading import Thread
import math


def tor_browser():
   #options = uc.ChromeOptions() 
   #options.add_argument(f'--proxy-server=socks5://127.0.0.1:9050')
   #options.user_data_dir = "/home/chromeprofileforbots"  
   #return uc.Chrome(user_data_dir="/home/chromeprofileforbots",options=options,version_main=109)  
   return uc.Chrome(user_data_dir="/home/bravescrapingprofile",browser_executable_path='/usr/bin/brave-browser',headless=False,version_main=109)      
                       

def waitfor(xpth):
 try: 
  WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, xpth)))
 except:
   pass 

def jsclick(xpth):
 try: 
  element=driver.find_element('xpath',xpth)
  driver.execute_script("arguments[0].click();", element)
 except:
   pass   

def formaturl(url):
    url=url.strip()
    if not re.match('(?:http|https)://', url):
        return 'http://{}/'.format(url)
    return url

def updatesocket(COUNTER,CURRENT_TASK,QUEUE_LENGTH):
    time_elapsed=datetime.now()-start_time  
    STATUS_DICTIONATY['type']='progress'
    STATUS_DICTIONATY['datavalue']=math.ceil((COUNTER/QUEUE_LENGTH)*100)
    send_data(STATUS_DICTIONATY)
    STATUS_DICTIONATY['type']='textmessage2'
    STATUS_DICTIONATY['datavalue']='Crawled {COUNTER} out of {QUEUE_LENGTH}, Time elapsed : {time_elapsed}'.format(COUNTER=COUNTER,QUEUE_LENGTH=QUEUE_LENGTH,time_elapsed=str(time_elapsed))
    send_data(STATUS_DICTIONATY)
    STATUS_DICTIONATY['type']='textmessage'
    STATUS_DICTIONATY['datavalue']='Current STEP RUNNING : {CURRENT_TASK} OUT OF TOTAL STEPS : {TOTAL_STEPS}'.format(CURRENT_TASK=CURRENT_TASK,TOTAL_STEPS=TOTAL_STEPS)
    send_data(STATUS_DICTIONATY)

def navigatepage(location,keyword,COUNTER):
 CURRENT_TASK="Scrolling googlemaps......"
 QUEUE_LENGTH=TASK1_QUEUE_LENGTH
 updatesocket(COUNTER,CURRENT_TASK,QUEUE_LENGTH)
 waitfor('//div[@id="searchbox"]//input')
 driver.find_element('xpath','//div[@id="searchbox"]//input[@id="searchboxinput"]').clear()
 time.sleep(1)
 driver.find_element('xpath','//div[@id="searchbox"]//input[@id="searchboxinput"]').send_keys(location)
 time.sleep(1)
 jsclick('//button[@aria-label="Search"]')
 time.sleep(random.uniform(5,10))
 driver.find_element('xpath','//div[@id="searchbox"]//input[@id="searchboxinput"]').clear()
 time.sleep(1)
 driver.find_element('xpath','//div[@id="searchbox"]//input[@id="searchboxinput"]').send_keys(keyword)
 jsclick('//button[@aria-label="Search"]')
 time.sleep(random.uniform(15,28))
 while True:
  listings=driver.find_elements('xpath','//div[@role="article"]')
  if len(listings)==0:
    break
  driver.execute_script('document.querySelector(\'[role="feed"]\').scrollBy(0,50000)')
  driver.execute_script('document.querySelector(\'[role="feed"]\').scrollBy(0,50000)')
  time.sleep(random.uniform(5,10))
  after_scrolling_listings= driver.find_elements('xpath','//div[@role="article"]')
  updatesocket(COUNTER,CURRENT_TASK,QUEUE_LENGTH)
  if (len(listings))== (len(after_scrolling_listings)):
   break
 listings_url=[i.find_element('xpath','.//a').get_attribute('href') for i in driver.find_elements('xpath','//div[@role="article"]')]
 return listings_url

def getqueue():
 connection = mysql.connector.connect(
                              host='localhost', 
                              #host='161.97.97.183',
                              database='google_maps',
                              user='root', 
                              password='$C0NTaB0vps8765%%$#', 
                              port=3306
                              ,auth_plugin='caching_sha2_password')
 cursor = connection.cursor()         
 cursor.execute("SELECT * FROM gmaps_queue WHERE status=0")
 rows=cursor.fetchall()    
 print('Total rows : ',len(rows))
 return rows


def clickprivacy():
   try:
    time.sleep(10) 
    driver.switch_to.frame(driver.find_element_by_tag_name("iframe"))   
    driver.find_element('xpath','//div[@id="introAgreeButton"]').click() 
    time.sleep(10)
   except:
      pass 
   driver.switch_to.default_content()        

def insert_data(listings_url,query_parameter):
 connection = mysql.connector.connect(
                              host='localhost', 
                              #host='161.97.97.183',
                              database='google_maps',
                              user='root', 
                              password='$C0NTaB0vps8765%%$#', 
                              port=3306
                              ,auth_plugin='caching_sha2_password')
 cursor = connection.cursor()  
 for listing_url in listings_url: 
  sql_insert_with_param = """INSERT IGNORE INTO gmaps_links
                          (gmaps_url,query_parameter) 
                          VALUES (%s,%s);""" 
  data_tuple = (listing_url,query_parameter)
  cursor.execute(sql_insert_with_param, data_tuple)
  connection.commit() 
  print(data_tuple)

def geturls():
 connection = mysql.connector.connect(
                              host='localhost', 
                              #host='161.97.97.183',
                              database='google_maps',
                              user='root', 
                              password='$C0NTaB0vps8765%%$#', 
                              port=3306
                              ,auth_plugin='caching_sha2_password')
 cursor = connection.cursor()         
 cursor.execute("SELECT gmaps_url FROM `gmaps_links` WHERE processed=0")
 rows=cursor.fetchall()   
 rows=[i[0] for i in rows] 
 print('Total rows : ',len(rows))
 return rows


def parse_details(driver,gmaps_url):
 company=driver.title.replace(' - Google Maps','').strip()
 try:
  rating=driver.find_element('xpath','//span[contains(@aria-label," stars ")]').get_attribute('aria-label').strip().split(' ')[0].strip()
 except:
  rating=None 
 try: 
  category=driver.find_element('xpath','//button[@jsaction="pane.rating.category"]').text.strip()
 except:
  category=None
 try:
  phone=driver.find_element('xpath','//button[@data-tooltip="Copy phone number"]').get_attribute('data-item-id').split(':')[-1].strip()
 except:
  phone=None 
 try:
  website=driver.find_element('xpath','//a[@data-tooltip="Open website"]').get_attribute('href').strip()
  website = urlparse(website)
  website = '{uri.scheme}://{uri.netloc}/'.format(uri=website)
 except:
  website=None 
 claim_status='Unclaimed' if driver.find_elements('xpath','//a[@aria-label="Claim this business"]') else 'Claimed'
 try:
  latitude=float(gmaps_url.split("!3d")[1].split("!4d")[0])
 except:
  latitude=float(driver.current_url.split("@")[1].split(",")[0]) 
 try: 
  longitude=float(gmaps_url.split("!3d")[1].split("!4d")[1].split("!")[-0])
 except:
  longitude=float(driver.current_url.split("@")[1].split(",")[1]) 
 insert_details(company,rating,category,phone,website,claim_status,latitude,longitude,gmaps_url)

def insert_details(company,rating,category,phone,website,claim_status,latitude,longitude,gmaps_url):
 connection = mysql.connector.connect(
                              host='localhost', 
                              #host='161.97.97.183',
                              database='google_maps',
                              user='root', 
                              password='$C0NTaB0vps8765%%$#', 
                              port=3306
                              ,auth_plugin='caching_sha2_password')
 cursor = connection.cursor()  
 sql_insert_with_param = """INSERT IGNORE INTO gmaps_details
                          (company,rating,category,phone,website,claim_status,latitude,longitude,gmaps_url) 
                          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
 val = (company,rating,category,phone,website,claim_status,latitude,longitude,gmaps_url)
 cursor.execute(sql_insert_with_param , val)
 connection.commit() 
 print(val)

def update_queue(queueid):
 connection = mysql.connector.connect(
                              host='localhost', 
                              #host='161.97.97.183',
                              database='google_maps',
                              user='root', 
                              password='$C0NTaB0vps8765%%$#', 
                              port=3306
                              ,auth_plugin='caching_sha2_password')
 cursor = connection.cursor()  
 sql = "UPDATE gmaps_queue SET status=1 WHERE id = %s"
 val = (queueid,)
 cursor.execute(sql, val)
 connection.commit() 
 print(val)

def update_gmaps_links(gmaps_url):
 connection = mysql.connector.connect(
                              host='localhost', 
                              #host='161.97.97.183',
                              database='google_maps',
                              user='root', 
                              password='$C0NTaB0vps8765%%$#', 
                              port=3306
                              ,auth_plugin='caching_sha2_password')
 cursor = connection.cursor()  
 sql = "UPDATE gmaps_links SET processed=1 WHERE gmaps_url = %s"
 val = (gmaps_url,)
 cursor.execute(sql, val)
 connection.commit() 
 print(val)

def scroll_gmaps_extract_data(queue_rows):  # scroll google maps and extract gmaps_links
  COUNTER=0
  CURRENT_TASK=1
  QUEUE_LENGTH=TASK1_QUEUE_LENGTH
  global driver
  driver=tor_browser() 
  for queuedata in queue_rows:
    try:
      keyword=queuedata[0] 
      zips=queuedata[1]
      country=queuedata[2]  
      queueid=queuedata[4] 
      query_parameter=keyword+' '+zips+' '+country 
      time.sleep(3) 
      driver.get('https://www.google.ca/maps/?hl=en')  
      #clickprivacy()
      listings_url=navigatepage(str(zips+' '+country),keyword,COUNTER)
      insert_data(listings_url,query_parameter)
      update_queue(queueid)
    except Exception as e:
      print(e) 
      driver.close()
      driver.quit()
      driver=tor_browser() 
    COUNTER+=1  
    updatesocket(COUNTER,CURRENT_TASK,QUEUE_LENGTH)
  try:
    driver.close()
    driver.quit()
  except Exception as e:
    print(e)    
    

def extract_gmaps_details(gmaps_urls):  # get unprocessed gmaps links and crawl more data
  COUNTER=0
  CURRENT_TASK=2
  QUEUE_LENGTH=TASK2_QUEUE_LENGTH
  global driver
  driver=tor_browser() 
  for gmaps_url in gmaps_urls:
    try: 
      time.sleep(1) 
      driver.get(gmaps_url)
      waitfor('//button[@data-tooltip="Copy phone number"]')
      time.sleep(1) 
      parse_details(driver,gmaps_url)
      update_gmaps_links(gmaps_url)
    except Exception as e:
      print(e) 
      driver.close()
      driver.quit()
      driver=tor_browser() 
    COUNTER+=1  
    updatesocket(COUNTER,CURRENT_TASK,QUEUE_LENGTH)
  try:
    driver.close()
    driver.quit()
  except Exception as e:
    print(e)    

if __name__ == "__main__":
  server = WebsocketServer(host='0.0.0.0', port=5902,cert=cert,key=key)
  #server.set_fn_new_client(new_client)
  server.set_fn_client_left(client_left)
  server.set_fn_message_received(message_received)  
  t = Thread(target=start_loop, args=(server,),daemon=True)
  t.start()
  OUTPUT_FILENAME='/root/Desktop/scripts/googleMaps/gmapsstatus.json'
  STATUS_DICTIONATY={'source':'gmaps.py','type':['progress','textmessage','textmessage2','totalqueries'],'datavalue':'datavalue'}
  start_time = datetime.now()
  TOTAL_STEPS=2
  queue_rows=getqueue()   # get list of unprocessed data from queue
  TASK1_QUEUE_LENGTH=len(queue_rows)
  scroll_gmaps_extract_data(queue_rows)
  gmaps_urls=geturls()
  TASK2_QUEUE_LENGTH=len(gmaps_urls)
  extract_gmaps_details(gmaps_urls)       