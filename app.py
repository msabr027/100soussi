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
from sqlalchemy import create_engine

driver = webdriver.Chrome("/app/chromedriver")

def job():
    
    postal=['H2C0A0','H2A0A0','H1J0A0','H1P0A0','H1W0A0','H1X0A0','H2H0A0','H3N0A0','H3S0A0','H4C0A0','H4R0A0','H4W0A0','H7N0A0','H8S0A0','H8Z0A0','H9J0A0','H9B0A0','H9R0A0']
    magasins = ['3062050-walmart', '3072487-supermarche-pa','3051581-marche-bonichoix','3068312-les-marches-tradition','3072762-iga-quebec-et-nouveaubrunswick','3056575-marche-richelieu','3061982-super-c','3069103-marche-adonis','3063795-provigo','3065316-maxi']        
    date_time = date.today().strftime("%Y/%m/%d")
    full_date=[]
    name=[]
    price=[]
    store=[]
    zipc=[]
    full_data= pd.DataFrame(columns=['Product', 'Price', 'ZipCode','Store','Date'])
    
    engine = create_engine('mysql://root:6551322Kl@35.239.36.251/dbextract')

    for c in magasins:
        for d in postal:
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
                
            full_data1 = pd.DataFrame({'Product':name,'Price':price,
                                       'ZipCode':zipc,'Store':store,
                                       'Date':full_date})
            
            full_data.append(full_data1)
        
    full_data.to_sql(con=engine, name='circulaire', if_exists='append', index=False)
    
schedule.every().thursday.at("15:00").do(job) 

while 1:
    schedule.run_pending()
    time.sleep(1)
