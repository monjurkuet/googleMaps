import requests
import mysql.connector,re
from urllib.parse import urlsplit
from urllib.parse import urljoin
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from multiprocessing.pool import ThreadPool

ua = UserAgent()
headers = {'user-agent': ua.random}      

with open ('email_blacklist.txt') as blkfile:
  blacklist=[line.strip().lower() for line in blkfile.readlines() if line]

def getqueue():
 connection = mysql.connector.connect(
                              #host='localhost', 
                              host='161.97.97.183',
                              database='website_scraper',
                              user='root', 
                              password='$C0NTaB0vps8765%%$#', 
                              port=3306
                              ,auth_plugin='caching_sha2_password')
 cursor = connection.cursor()            
 query="SELECT website from website_scraper.gmaps_website WHERE website not in (SELECT DISTINCT source_website from website_scraper.website_scraper)"   
 cursor.execute(query)
 rows=cursor.fetchall()    
 rows=[i[0] for i in rows]
 print('Total rows : ',len(rows))
 return rows

def extract_email(source_html):
    emailList=[]
    reobj = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,6}\b", re.IGNORECASE)
    found_emails=re.findall(reobj, source_html)
    found_emails=[i for i in found_emails if i]
    if len(found_emails)!=0:
     for emailText in found_emails:
        emailText=emailText.lower().strip()
        if emailText not in emailList: 
          if not any(ext in emailText for ext in blacklist):
            emailList.append(emailText)
    return emailList       

def fullurl(base_url,url):
 if base_url not in url:
    url= urljoin(base_url, url)
 if not url.strip().startswith('http'):
   url='http://'+url
 return url   


def generatelinks(source_website):
 base_url = "{0.netloc}".format(urlsplit(source_website))
 allLinks=[]
 allLinks.append(base_url)
 emailList=[]
 linklimit=200
 i=0
 while i < len(allLinks) and i<linklimit:
  try:   
   current_url= fullurl(base_url,allLinks[i])  
   print('Crawling : ',current_url)
   response = requests.get(current_url,headers=headers)    
   source_html=response.text
   soup=BeautifulSoup(source_html)
   emailList.extend(extract_email(source_html))
   new_links=[a.attrs.get('href') for a in soup.select('a[href]')]
   allLinks.extend(new_links)
   allLinks=list(set(allLinks))
   ignore_list=['.mp4','.mp3','.doc','.jpg','.pdf','#','.jpeg','facebook','instagram','google','youtube','shopify','amazon','yelp','reddit','.png']
   allLinks=[i for i in allLinks if i and not any(ele in i for ele in ignore_list) and base_url in i]
  except Exception as e:
   print(e) 
   #emailList=['Retry']
  i+=1
 emailList=list(set(emailList)) 
 #print(emailList) 
 return str(emailList)

def insert_data(domain,source_website,email):
 connection = mysql.connector.connect(
                              #host='localhost', 
                              host='161.97.97.183',
                              database='website_scraper',
                              user='root', 
                              password='$C0NTaB0vps8765%%$#', 
                              port=3306
                              ,auth_plugin='caching_sha2_password')
 cursor = connection.cursor()     
 sql_insert_with_param = """REPLACE INTO website_scraper
                          (domain,source_website,email)
                          VALUES (%s,%s,%s);""" 
 data_tuple = (domain,source_website,email)
 cursor.execute(sql_insert_with_param, data_tuple)
 connection.commit() 
 print(data_tuple)

def crawl_website(source_website):
    domain="{0.netloc}".format(urlsplit(source_website))
    email=generatelinks(source_website)
    print(email)
    insert_data(domain,source_website,email)
    global counter
    counter+=1
    print('Done : ',counter,' out of :',queue_length)
    
if __name__ == '__main__':
 rows=getqueue()
 counter=0
 queue_length=len(rows)
 ThreadPool(4).map(crawl_website, rows) 

 
     
