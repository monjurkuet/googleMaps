import time
import random
import undetected_chromedriver as uc
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import sqlite3
import time
from multiprocessing.pool import ThreadPool
import json
from tqdm import tqdm

conn = sqlite3.connect('database.db', check_same_thread=False)
cursor = conn.cursor()

def waitfor(xpth, driver):
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, xpth)))
    except:
        pass

def jsclick(xpth, driver):
    try:
        element = driver.find_element('xpath', xpth)
        driver.execute_script("arguments[0].click();", element)
    except:
        pass

def navigatepage(location, keyword, driver):
    waitfor('//div[@id="searchbox"]//input', driver)
    driver.find_element(
        'xpath', '//div[@id="searchbox"]//input[@id="searchboxinput"]').clear()
    time.sleep(1)
    driver.find_element(
        'xpath', '//div[@id="searchbox"]//input[@id="searchboxinput"]').send_keys(location)
    time.sleep(1)
    jsclick('//button[@aria-label="Search"]', driver)
    time.sleep(random.uniform(5, 10))
    driver.find_element(
        'xpath', '//div[@id="searchbox"]//input[@id="searchboxinput"]').clear()
    time.sleep(1)
    driver.find_element(
        'xpath', '//div[@id="searchbox"]//input[@id="searchboxinput"]').send_keys(keyword)
    jsclick('//button[@aria-label="Search"]', driver)
    time.sleep(random.uniform(15, 28))
    while True:
        listings = driver.find_elements('xpath', '//div/a[contains(@href,"/maps/place")]')
        if len(listings) == 0:
            break
        driver.execute_script(
            'document.querySelector(\'[role="feed"]\').scrollBy(0,50000)')
        driver.execute_script(
            'document.querySelector(\'[role="feed"]\').scrollBy(0,50000)')
        time.sleep(random.uniform(5, 10))
        after_scrolling_listings = driver.find_elements('xpath', '//div/a[contains(@href,"/maps/place")]')
        print(
            f'Scrolling : {location,keyword}. Total Listing : {len(after_scrolling_listings)}')
        if (len(listings)) == (len(after_scrolling_listings)):
            break
    listings_url = [i.get_attribute('href')
                    for i in driver.find_elements('xpath', '//div/a[contains(@href,"/maps/place")]')]
    return listings_url

def getqueue():
    cursor.execute("SELECT distinct keyword FROM gmaps_queue")
    rows = [i[0] for i in cursor.fetchall()]
    print(rows)
    keyword=input()
    cursor.execute(f"SELECT keyword,lat,lon,id FROM gmaps_queue WHERE status=0 and keyword='{keyword}'")
    rows = cursor.fetchall()
    print('Total rows : ', len(rows))
    return rows

def insert_data(listings_url, query_parameter):
    for listing_url in listings_url:
        sql_insert_with_param = """INSERT OR IGNORE INTO gmaps_links
                            (gmaps_url,query_parameter) 
                            VALUES (?,?);"""
        data_tuple = (listing_url, json.dumps(query_parameter))
        cursor.execute(sql_insert_with_param, data_tuple)
        conn.commit()
        print(data_tuple)

def update_queue(id):
    sql = "UPDATE gmaps_queue SET status=1 WHERE id = ?"
    val = (id,)
    cursor.execute(sql, val)
    conn.commit()
    print(val)

def clickprivacy(driver):
    try:
        # driver.switch_to.frame(driver.find_element_by_tag_name("iframe"))
        waitfor('//button[@aria-label="Accept all"]', driver)
        jsclick('//button[@aria-label="Accept all"]', driver)
        time.sleep(5)
    except:
        pass
    # driver.switch_to.default_content()

# scroll google maps and extract gmaps_links
def scroll_gmaps_extract_data(queuedata):
    try:
        driver = uc.Chrome(headless=True)
        try:
            keyword = queuedata[0]
            lat = queuedata[1]
            lon = queuedata[2]
            id = queuedata[3]
            query_parameter = {'keyword':keyword,'lat':lat,'lon':lon}
            time.sleep(3)
            driver.get('https://www.google.ca/maps/?hl=en')
            clickprivacy(driver)
            listings_url = navigatepage((str(lat)+', '+str(lon)), keyword, driver)
            insert_data(listings_url, query_parameter)
            update_queue(id)
        except Exception as e:
            print(e)
        try:
            driver.close()
            driver.quit()
        except Exception as e:
            print(e)
    except:
        pass

if __name__ == "__main__":
    INSTANCES = 2
    queue_rows = getqueue()   # get list of unprocessed data from queue
    tqdm(ThreadPool(INSTANCES).map(scroll_gmaps_extract_data, queue_rows), total=len(queue_rows))