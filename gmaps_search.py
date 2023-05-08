import time,random
from time import sleep
import undetected_chromedriver as uc
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import mysql.connector
from datetime import datetime
import time
from multiprocessing.pool import ThreadPool, Pool
import numpy as np
                       
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

def navigatepage(location,keyword,COUNTER):
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
    print(f'Scrolling : {location,keyword,COUNTER}. Total Listing : {len(after_scrolling_listings)}')
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

def scroll_gmaps_extract_data(queue_rows):  # scroll google maps and extract gmaps_links
  driver = uc.Chrome(browser_executable_path='/usr/bin/brave-browser',headless=True,version_main=111)  
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
      driver=uc.Chrome(browser_executable_path='/usr/bin/brave-browser',headless=False,version_main=111)  
  try:
    driver.close()
    driver.quit()
  except Exception as e:
    print(e)    

if __name__ == "__main__":
  start_time = datetime.now()
  INSTANCES=5
  queue_rows=getqueue()   # get list of unprocessed data from queue
  queue_rows=np.split(queue_rows, INSTANCES)
  ThreadPool(INSTANCES).map(scroll_gmaps_extract_data, query_list)  