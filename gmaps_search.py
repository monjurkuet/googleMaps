import time
import random
import sqlite3
from multiprocessing.pool import ThreadPool
import json
from tqdm import tqdm
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# Establishing connection with the database
conn = sqlite3.connect('database.db', check_same_thread=False)
cursor = conn.cursor()

# Function to wait for an element to appear on the page
def wait_for_element(driver, xpath, timeout=15):
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, xpath)))
    except Exception as e:
        print(f"Error while waiting for element: {e}")

# Function to click an element using JavaScript
def js_click(driver, xpath):
    try:
        element = driver.find_element(By.XPATH, xpath)
        driver.execute_script("arguments[0].click();", element)
    except Exception as e:
        print(f"Error while clicking element: {e}")

# Function to navigate through Google Maps
def navigate_page(location, keyword, driver):
    wait_for_element(driver, '//div[@id="searchbox"]//input')
    search_input = driver.find_element(By.XPATH, '//div[@id="searchbox"]//input[@id="searchboxinput"]')
    search_input.clear()
    time.sleep(1)
    search_input.send_keys(location)
    time.sleep(1)
    js_click(driver, '//button[@aria-label="Search"]')
    time.sleep(random.uniform(5, 10))
    search_input.clear()
    time.sleep(1)
    search_input.send_keys(keyword)
    js_click(driver, '//button[@aria-label="Search"]')
    time.sleep(random.uniform(15, 28))
    while True:
        listings = driver.find_elements(By.XPATH, '//div/a[contains(@href,"/maps/place")]')
        if not listings:
            break
        driver.execute_script('document.querySelector(\'[role="feed"]\').scrollBy(0,50000)')
        driver.execute_script('document.querySelector(\'[role="feed"]\').scrollBy(0,50000)')
        time.sleep(random.uniform(5, 10))
        after_scrolling_listings = driver.find_elements(By.XPATH, '//div/a[contains(@href,"/maps/place")]')
        print(f'Scrolling : {location, keyword}. Total Listing : {len(after_scrolling_listings)}')
        if len(listings) == len(after_scrolling_listings):
            break
    listings_url = [i.get_attribute('href') for i in driver.find_elements(By.XPATH, '//div/a[contains(@href,"/maps/place")]')]
    return listings_url

# Function to fetch data from the queue
def get_queue():
    cursor.execute("SELECT distinct keyword FROM gmaps_queue")
    keywords = [row[0] for row in cursor.fetchall()]
    print(keywords)
    keyword = input("Enter a keyword: ")
    cursor.execute(f"SELECT keyword, lat, lon, id FROM gmaps_queue WHERE status=0 AND keyword='{keyword}'")
    rows = cursor.fetchall()
    print('Total rows : ', len(rows))
    return rows

# Function to insert data into the database
def insert_data(listings_url, query_parameter):
    for listing_url in listings_url:
        sql_insert_with_param = """INSERT OR IGNORE INTO gmaps_links
                            (gmaps_url, query_parameter) 
                            VALUES (?, ?);"""
        data_tuple = (listing_url, json.dumps(query_parameter))
        cursor.execute(sql_insert_with_param, data_tuple)
        conn.commit()
        print(data_tuple)

# Function to update the queue status
def update_queue(id):
    sql = "UPDATE gmaps_queue SET status=1 WHERE id = ?"
    val = (id,)
    cursor.execute(sql, val)
    conn.commit()
    print(val)

# Function to click privacy buttons
def click_privacy(driver):
    try:
        wait_for_element(driver, '//button[@aria-label="Accept all"]')
        js_click(driver, '//button[@aria-label="Accept all"]')
        time.sleep(5)
    except Exception as e:
        print(f"Error while clicking privacy button: {e}")

# Function to scroll Google Maps and extract data
def scroll_gmaps_extract_data(queuedata):
    try:
        driver = uc.Chrome()
        try:
            keyword, lat, lon, id = queuedata
            query_parameter = {'keyword': keyword, 'lat': lat, 'lon': lon}
            time.sleep(3)
            driver.get('https://www.google.ca/maps/?hl=en')
            click_privacy(driver)
            listings_url = navigate_page((str(lat) + ', ' + str(lon)), keyword, driver)
            insert_data(listings_url, query_parameter)
            update_queue(id)
        except Exception as e:
            print(f"Error in processing data: {e}")
        finally:
            driver.quit()
    except Exception as e:
        print(f"Error in setting up Chrome driver: {e}")

if __name__ == "__main__":
    INSTANCES = 6
    queue_rows = get_queue()   # get list of unprocessed data from queue
    tqdm(ThreadPool(INSTANCES).map(scroll_gmaps_extract_data, queue_rows), total=len(queue_rows))
