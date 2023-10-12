import time,re,random
import undetected_chromedriver as uc
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import sqlite3
from urllib.parse import urlparse
import time
from tqdm import tqdm

conn = sqlite3.connect('database.db', check_same_thread=False)
cursor = conn.cursor()

def waitfor(driver,xpth):
    try: 
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, xpth)))
    except:
        pass 

def parse_domain(url):
    urlfinal=urlparse(url).netloc.replace("www.", "")
    if not urlfinal:
        urlfinal =urlparse(url).path.replace("www.", "")
    print(urlfinal)
    return urlfinal.lower()

def geturls():       
    cursor.execute("SELECT gmaps_url FROM gmaps_links WHERE gmaps_url NOT IN (SELECT gmaps_url from gmaps_details)")
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
        website = parse_domain(website)
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
    sql_insert_with_param = """INSERT OR IGNORE INTO gmaps_details
                            (company,rating,category,phone,website,claim_status,latitude,longitude,gmaps_url) 
                            VALUES (?,?,?,?,?,?,?,?,?);"""
    val = (company,rating,category,phone,website,claim_status,latitude,longitude,gmaps_url)
    cursor.execute(sql_insert_with_param , val)
    conn.commit() 
    print(val)

def extract_gmaps_details(gmaps_urls):  # get unprocessed gmaps links and crawl more data
    driver=uc.Chrome(headless=True)
    COUNTER=0
    for gmaps_url in tqdm(gmaps_urls):
        try: 
            time.sleep(1) 
            driver.get(gmaps_url)
            waitfor(driver,'//button[@data-tooltip="Copy phone number"]')
            time.sleep(4) 
            parse_details(driver,gmaps_url)
        except Exception as e:
            print(e) 
            driver.close()
            driver.quit()
            driver=uc.Chrome(headless=True)
        COUNTER+=1
        if COUNTER%250==0:
            try:
                driver.close()
                driver.quit()
                driver=uc.Chrome(headless=True)
            except Exception as e:
                print(e)  
    try:
        driver.close()
        driver.quit()
    except Exception as e:
        print(e)    

if __name__ == "__main__":
    inputqueue=geturls()
    extract_gmaps_details(inputqueue)       