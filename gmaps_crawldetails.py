import sqlite3
import time
from urllib.parse import urlparse
from tqdm import tqdm
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc

class GoogleMapsScraper:
    def __init__(self, database_file='database.db'):
        self.conn = sqlite3.connect(database_file, check_same_thread=False)
        self.cursor = self.conn.cursor()

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

    def get_unprocessed_urls(self):
        self.cursor.execute("SELECT gmaps_url FROM gmaps_links WHERE gmaps_url NOT IN (SELECT gmaps_url from gmaps_details)")
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
        sql_insert_with_param = """INSERT OR IGNORE INTO gmaps_details
                                (company, rating, category, phone, website, claim_status, latitude, longitude, gmaps_url) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"""
        val = (company, rating, category, phone, website, claim_status, latitude, longitude, gmaps_url)
        self.cursor.execute(sql_insert_with_param, val)
        self.conn.commit()
        print(val)

    def extract_gmaps_details(self, gmaps_urls):
        driver = uc.Chrome()
        counter = 0
        for gmaps_url in tqdm(gmaps_urls):
            try:
                time.sleep(1)
                driver.get(gmaps_url)
                self.wait_for_element(driver, '//button[@data-tooltip="Copy phone number"]')
                time.sleep(4)
                self.parse_details(driver, gmaps_url)
            except Exception as e:
                print(f"Error while processing URL: {gmaps_url} - {e}")
                driver.close()
                driver.quit()
                driver = uc.Chrome()
            counter += 1
            if counter % 250 == 0:
                try:
                    driver.close()
                    driver.quit()
                    driver = uc.Chrome()
                except Exception as e:
                    print(f"Error while restarting driver: {e}")
        try:
            driver.close()
            driver.quit()
        except Exception as e:
            print(f"Error while closing driver: {e}")

if __name__ == "__main__":
    scraper = GoogleMapsScraper()
    urls = scraper.get_unprocessed_urls()
    scraper.extract_gmaps_details(urls)
