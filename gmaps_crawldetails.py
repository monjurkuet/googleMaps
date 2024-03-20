import sqlite3
import time
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import threading
import queue

class GoogleMapsScraper:
    def __init__(self, callback):
        self.callback = callback
        self.task_queue = queue.Queue()
        self.workers = []
        self.num_workers = 6  # You can adjust the number of worker threads as needed
        self.page_view_limit = 1000
        database_file='database.db'
        self.conn = sqlite3.connect(database_file, check_same_thread=False)
        self.cursor = self.conn.cursor()
        # Initialize workers
        for _ in range(self.num_workers):
            worker = threading.Thread(target=self.worker_loop)
            worker.daemon = False
            worker.start()
            self.workers.append(worker)
            time.sleep(1)
    
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
                gmaps_url= task
                try:
                    print(self.extract_gmaps_details(gmaps_url, driver))
                except Exception as e:
                    print(e)
            except queue.Empty:
                continue
        driver.quit()

    def get_new_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        #proxy_server = "127.0.0.1:16379"
        #options.add_argument(f'--proxy-server={proxy_server}')
        return webdriver.Chrome(options=options)

    def wait_for_element(self, driver, xpath, timeout=15):
        try:
            WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, xpath)))
        except Exception as e:
            print(f"Error while waiting for element: {e}")

    def parse_domain(self, url):
        url_final = urlparse(url).netloc.replace("www.", "")
        if not url_final:
            url_final = urlparse(url).path.replace("www.", "")
        return url_final.lower()

    # def get_unprocessed_urls(self):
    #     self.cursor.execute("SELECT DISTINCT json_extract(query_parameter, '$.keyword') AS unique_keyword FROM gmaps_links \
    #                         WHERE json_extract(query_parameter, '$.keyword') IS NOT NULL;")
    #     keywords = [row[0] for row in self.cursor.fetchall()]
    #     print(keywords)
    #     keyword = input("Enter a keyword: ")
    #     self.cursor.execute(f"SELECT gmaps_url FROM gmaps_links WHERE gmaps_url NOT IN (SELECT gmaps_url from gmaps_details) and query_parameter like '%{keyword}%'")
    #     rows = self.cursor.fetchall()
    #     urls = [i[0] for i in rows]
    #     print('Total rows:', len(urls))
    #     return urls

    def get_unprocessed_urls(self):
        self.cursor.execute("SELECT gmaps_url FROM gmaps_links where id in (SELECT id from osm_reverse_geocode WHERE display_name like '%united states%') and query_parameter like '%insurance%' and gmaps_url not in (SELECT gmaps_url from gmaps_details)")
        rows = self.cursor.fetchall()
        urls = [i[0] for i in rows]
        print('Total rows:', len(urls))
        return urls

    def parse_details(self, driver, gmaps_url):
        company = driver.title.replace(' - Google Maps', '').strip()
        try:
            rating = driver.find_element(By.XPATH, '//span[contains(@aria-label," stars ")]').get_attribute('aria-label').strip().split(' ')[0].strip()
        except:
            rating = None
        try:
            category = driver.find_element(By.XPATH, '//button[@jsaction="pane.rating.category"]').text.strip()
        except:
            try:
                category = driver.find_element(By.XPATH, '//button[contains(@jsaction, "category")]').text.strip()
            except:
                category = None
        try:
            phone = driver.find_element(By.XPATH, '//button[@data-tooltip="Copy phone number"]').get_attribute('data-item-id').split(':')[-1].strip()
        except:
            phone = None
        try:
            website = driver.find_element(By.XPATH, '//a[@data-tooltip="Open website"]').get_attribute('href').strip()
            website = self.parse_domain(website)
        except:
            website = None
        claim_status = 'Unclaimed' if driver.find_elements(By.XPATH, '//a[@aria-label="Claim this business"]') else 'Claimed'
        try:
            latitude = float(gmaps_url.split("!3d")[1].split("!4d")[0])
        except:
            latitude = float(driver.current_url.split("@")[1].split(",")[0])
        try:
            longitude = float(gmaps_url.split("!3d")[1].split("!4d")[1].split("!")[-0])
        except:
            longitude = float(driver.current_url.split("@")[1].split(",")[1])
        self.insert_details(company, rating, category, phone, website, claim_status, latitude, longitude, gmaps_url)

    def insert_details(self, company, rating, category, phone, website, claim_status, latitude, longitude, gmaps_url):
        while True:
            try:
                self.cursor = self.conn.cursor()
                sql_insert_with_param = """INSERT OR IGNORE INTO gmaps_details
                                        (company, rating, category, phone, website, claim_status, latitude, longitude, gmaps_url) 
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"""
                val = (company, rating, category, phone, website, claim_status, latitude, longitude, gmaps_url)
                self.cursor.execute(sql_insert_with_param, val)
                self.conn.commit()
                print(val)
                break
            except Exception as e:
                print(e)
            time.sleep(2)

    def extract_gmaps_details(self, gmaps_url, driver):
        try:
            driver.get(gmaps_url)
            self.wait_for_element(driver, '//button[@data-tooltip="Copy phone number"]')
            time.sleep(2)
            self.parse_details(driver, gmaps_url)
        except Exception as e:
            print(f"Error while processing URL: {gmaps_url} - {e}")
            return None
        return 'Crawled' 
    
    def close(self):
        # Stop workers
        for _ in range(self.num_workers):
            self.task_queue.put(None)
        for worker in self.workers:
            worker.join()

def my_callback(data):
    print(f"Data processed: {data}")

if __name__ == "__main__":
    scraper = GoogleMapsScraper(my_callback)
    urls = scraper.get_unprocessed_urls()
    for url in urls:
        scraper.add_task(url)
    scraper.close()


