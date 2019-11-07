# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 14:46:05 2019

@author: mosab
"""
from selenium import webdriver
from bs4  import BeautifulSoup
import pandas as pd
import time
from datetime import date
import schedule
import pymysql

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome("/app/chromedriver",chrome_options=chrome_options)

def job():
    
    print('the scraping job has started for 100soussi...flipp data')
    postal=['H2C0A0','H2A0A0','H1J0A0','H1P0A0','H1W0A0','H1X0A0','H2H0A0','H3N0A0','H3S0A0','H4C0A0','H4R0A0','H4W0A0','H7N0A0','H8S0A0','H8Z0A0','H9J0A0','H9B0A0','H9R0A0']
    magasins = ['3062050-walmart', '3051581-marche-bonichoix','3068312-les-marches-tradition','3072762-iga-quebec-et-nouveaubrunswick','3056575-marche-richelieu','3061982-super-c','3069103-marche-adonis','3063795-provigo','3065316-maxi']        
    date_time = date.today().strftime("%Y/%m/%d")
    full_date=[]
    name=[]
    price=[]
    store=[]
    zipc=[]
    
    
    connection = pymysql.connect(host='35.239.36.251',
                         user='root',
                         password='6551322Kl',
                         db='dbextract')
    cursor=connection.cursor()

    for c in magasins:
        for d in postal:
            full_data= pd.DataFrame(columns=['Product', 'Price', 'ZipCode','Store','Date'])
            lienc= "https://flipp.com/fr-ca/-/circulaire/"+c+"-circulaire?postal_code="+d
            driver.get(lienc)
            time.sleep(7)
            content = driver.page_source
            soup = BeautifulSoup(content)
            links=[]
            for a in soup.findAll('a',href=True, attrs={'class':'item-container'}):
                link1=a['href']
                links.append(link1)
            
            fulllink = []
            for i in links:
              link2="https://flipp.com" + i
              fulllink.append(link2)
            for n in fulllink:
                driver.get(n)
                time.sleep(7)
                content = driver.page_source
                soup = BeautifulSoup(content,'html.parser')
                
                if soup.find('span', attrs={'content-slot':'title'}):
                    for a in soup.find('span', attrs={'content-slot':'title'}):
                        if a !='Vous departez de Flipp.':
                            name.append(a)
                            if soup.find('flipp-price'):
                                link3 = soup.find('flipp-price').attrs["value"]
                            else:
                                link3 =''
                            price.append(link3)
                            zipc.append(d)
                            store.append(c)
                            full_date.append(date_time)
                            
            price = [0 if x=='' else x for x in price]
            
            full_data1 = pd.DataFrame({'Product':name,'Price':price,
                                       'ZipCode':zipc,'Store':store,
                                       'Date':full_date})
            
            full_data = full_data.append(full_data1)
                    
            cols = "`,`".join([str(i) for i in full_data.columns.tolist()])

            for i,row in full_data.iterrows():
                sql = "INSERT INTO `circulaire` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
                cursor.execute(sql, tuple(row))
                connection.commit()
    
schedule.every().thursday.at("21:33").do(job) 

while 1:
    schedule.run_pending()
    time.sleep(1)
