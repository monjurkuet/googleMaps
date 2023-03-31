import time
import undetected_chromedriver as uc


driver=uc.Chrome(user_data_dir="/home/bravescrapingprofile",browser_executable_path='/usr/bin/brave-browser',headless=False,version_main=109)      
driver.get('https://www.bing.com/')
time.sleep(10)
driver.close()
driver.quit()