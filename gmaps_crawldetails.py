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
import sys
import time

def switchIP():    
    with Controller.from_port(port = 9051) as controller:         
        controller.authenticate()         
        controller.signal(Signal.NEWNYM)
        time.sleep(controller.get_newnym_wait())

def tor_browser():
   #options = uc.ChromeOptions() 
   #options.add_argument(f'--proxy-server=socks5://127.0.0.1:9050')
   #options.user_data_dir = "/home/chromeprofileforbots"  
   #return uc.Chrome(user_data_dir="/home/chromeprofileforbots",options=options,version_main=109)  
   return uc.Chrome(user_data_dir="/home/bravescrapingprofile",browser_executable_path='/usr/bin/brave-browser',headless=False,version_main=111)      
                       

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


def clickprivacy():
   try:
    time.sleep(10) 
    driver.switch_to.frame(driver.find_element_by_tag_name("iframe"))   
    driver.find_element('xpath','//div[@id="introAgreeButton"]').click() 
    time.sleep(10)
   except:
      pass 
   driver.switch_to.default_content()        


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

def extract_gmaps_details(gmaps_urls):  # get unprocessed gmaps links and crawl more data
    global driver
    driver=tor_browser() 
    TOTAL = len(gmaps_urls)
    COUNTER=0
    for gmaps_url in gmaps_urls:
        try: 
            time.sleep(1) 
            driver.get(gmaps_url)
            waitfor('//button[@data-tooltip="Copy phone number"]')
            time.sleep(4) 
            parse_details(driver,gmaps_url)
            update_gmaps_links(gmaps_url)
        except Exception as e:
            print(e) 
            driver.close()
            driver.quit()
            driver=tor_browser() 
        COUNTER+=1
        print(f'Done : {COUNTER} Out of : {TOTAL}') 
        if COUNTER%250==0:
            try:
                driver.close()
                driver.quit()
                driver=tor_browser() 
            except Exception as e:
                print(e)  
    try:
        driver.close()
        driver.quit()
    except Exception as e:
        print(e)    

if __name__ == "__main__":
    gmaps_urls=geturls()
    extract_gmaps_details(gmaps_urls)       