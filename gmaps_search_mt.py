import threading
import queue
from selenium import webdriver
import json
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import random

class GmapsLinkExtractor:
    def __init__(self, callback):
        self.callback = callback
        self.task_queue = queue.Queue()
        self.workers = []
        self.num_workers = 4 # You can adjust the number of worker threads as needed
        self.page_view_limit = 3000
        self.conn = sqlite3.connect('database.db', check_same_thread=False)
        self.cursor = self.conn.cursor()
        # Initialize workers
        for _ in range(self.num_workers):
            worker = threading.Thread(target=self.worker_loop)
            worker.daemon = False
            worker.start()
            self.workers.append(worker)
            time.sleep(.1)
            
    def add_task(self, data):
        self.task_queue.put(data)
        
    def worker_loop(self):
        page_view_count = 0
        driver = self.get_new_driver()
        while True:
            try:
                task = self.task_queue.get(timeout=1)  # Adjust timeout as needed
                if task is None:
                    break
                page_view_count += 1
                if page_view_count > self.page_view_limit:
                    page_view_count = 1
                    driver.quit()
                    driver = self.get_new_driver()
                keyword, lat, lon, id = task
                query_parameter = {'keyword': keyword, 'lat': lat, 'lon': lon}
                try:
                    listings_url = self.navigate_page((str(lat) + ', ' + str(lon)), keyword, driver)
                    self.insert_data(listings_url, query_parameter)
                    self.update_queue(id)
                except Exception as e:
                    print(e)
            except queue.Empty:
                continue
        driver.quit()
        
    def get_new_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        caps = options.to_capabilities()
        #caps['goog:loggingPrefs'] = {'performance': 'ALL'}
        #proxy_server = "127.0.0.1:16379"
        #options.add_argument(f'--proxy-server={proxy_server}')
        return webdriver.Chrome(options=options)
    
    def navigate_page(self, location, keyword, driver):
        wait = WebDriverWait(driver, 15)
        driver.get('https://www.google.ca/maps/?hl=en')
        self.click_privacy(driver)
        wait.until(EC.presence_of_element_located((By.XPATH, '//div[@id="searchbox"]//input')))
        search_input = driver.find_element(By.XPATH, '//div[@id="searchbox"]//input[@id="searchboxinput"]')
        search_input.clear()
        time.sleep(1)
        search_input.send_keys(location)
        time.sleep(1)
        self.js_click(driver, '//button[@aria-label="Search"]')
        time.sleep(random.uniform(5, 10))
        search_input.clear()
        time.sleep(1)
        search_input.send_keys(keyword)
        self.js_click(driver, '//button[@aria-label="Search"]')
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
    
    def insert_data(self, listings_url, query_parameter):
        while True:
            try:
                self.cursor = self.conn.cursor()
                for listing_url in listings_url:
                    sql_insert_with_param = """INSERT OR IGNORE INTO gmaps_links
                                        (gmaps_url, query_parameter) 
                                        VALUES (?, ?);"""
                    data_tuple = (listing_url, json.dumps(query_parameter))
                    # You can uncomment the below lines if you have a 'cursor' object initialized outside the class
                    self.cursor.execute(sql_insert_with_param, data_tuple)
                    self.conn.commit()
                    print(data_tuple)
                break
            except Exception as e:
                print(e)
            time.sleep(2)
    
    def update_queue(self, id):
        while True:
            try:
                self.cursor = self.conn.cursor()
                sql = "UPDATE gmaps_queue SET status=1 WHERE id = ?"
                val = (id,)
                # You can uncomment the below lines if you have a 'cursor' object initialized outside the class
                self.cursor.execute(sql, val)
                self.conn.commit()
                print(val)
                break
            except Exception as e:
                print(e)
            time.sleep(2)
    
    def click_privacy(self, driver):
        try:
            wait = WebDriverWait(driver, 15)
            wait.until(EC.presence_of_element_located((By.XPATH, '//button[@aria-label="Accept all"]')))
            self.js_click(driver, '//button[@aria-label="Accept all"]')
            time.sleep(5)
        except Exception as e:
            #print(f"Error while clicking privacy button: {e}")
            pass
    
    def js_click(self, driver, xpath):
        try:
            element = driver.find_element(By.XPATH, xpath)
            driver.execute_script("arguments[0].click();", element)
        except Exception as e:
            #print(f"Error while clicking element: {e}")
            pass
    
    def close(self):
        # Stop workers
        for _ in range(self.num_workers):
            self.task_queue.put(None)
        for worker in self.workers:
            worker.join()


def get_queue():
    conn = sqlite3.connect('database.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute("SELECT distinct keyword FROM gmaps_queue")
    keywords = [row[0] for row in cursor.fetchall()]
    print(keywords)
    keyword = input("Enter a keyword: ")
    cursor.execute(f"SELECT keyword, lat, lon, id FROM gmaps_queue WHERE status=0 AND keyword='{keyword}' GROUP BY lat,lon")
    rows = cursor.fetchall()
    print('Total rows : ', len(rows))
    return rows

# Example usage:
def my_callback(data):
    print(f"Data processed: {data}")

gmaps_extractor = GmapsLinkExtractor(my_callback)

# Assuming 'queue_rows' contains data in the format (keyword, lat, lon, id)
queue_rows = get_queue()

for row in queue_rows:
    gmaps_extractor.add_task(row)


# Close the GmapsLinkExtractor
gmaps_extractor.close()
