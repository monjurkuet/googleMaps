import time
import undetected_chromedriver as uc
from multiprocessing.pool import ThreadPool

def extract_data(query):
    driver = uc.Chrome(browser_executable_path='/usr/bin/brave-browser',headless=False,version_main=111)   
    driver.get('https://www.bing.com/')
    print(query)
    time.sleep(10)
    driver.close()
    driver.quit()

if __name__ == '__main__':
  query_list=[1,2,3,4,5,6,7,8,10]
  ThreadPool(4).map(extract_data, query_list)  