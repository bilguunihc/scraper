from datetime import datetime
from selenium import webdriver
import requests
from bs4 import BeautifulSoup
import json
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import csv
chrome_options = Options()
#chrome_options.add_argument("--disable-extensions")
#chrome_options.add_argument("--disable-gpu")
#chrome_options.add_argument("--no-sandbox") # linux only
chrome_options.add_argument("--headless")
user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
chrome_options.add_argument(f'user-agent={user_agent}')
import boto3
import uuid

s3 = boto3.client("s3")
    

def idax():
    try:
        url = 'https://www.idax.exchange/mn_MN/market'
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)
        time.sleep(15)
        elem = driver.find_element(By.XPATH, '/html/body/div[3]/div[1]/div/div/div/div[1]/ul/li[2]').click()
        time.sleep(2)
        element_mont = driver.find_element(By.XPATH, '/html/body/div[3]/div[1]/div/div/div/div[2]')
        source = element_mont.get_attribute('innerHTML')
        xml = BeautifulSoup(source, 'lxml')
        div = xml.find('div').find('div').find('div').find('div').find('div').find('div')
        table = div.find('ul')
        coins = []
        for table_row in table:
            rows = table_row.find_all('a')
            name = rows[0].getText()
            latest_price = rows[1].getText()
            change = rows[2].getText()
            high = rows[3].getText()
            low = rows[4].getText()
            volume = rows[5].getText()
            
            coin = [str(datetime.now()),
                    str(datetime.now().date()),
                    str(datetime.now()),
                    'idax',
                    str(uuid.uuid4()),
                    str(name),
                    str(change),
                    str(volume),
                    str(high),
                    str(low),
                    str(latest_price)
                    ]
            txt = ' '.join(map(str, coin))
            txt_line = txt + '\n'
            coins.append(txt_line)

        fields = ['created_at', 'date', 'market_time', 'exchange_name', 'name', 'diff', 'volume', 'max24', 'min24', 'last_price']
        timestr = time.strftime("%Y%m%d-%H%M%S")
        # name of csv file
        filename = "idax-%s.csv" %timestr
            # writing to csv file
        with open(filename, 'w') as csvfile:
            # creating a csv writer object
            csvwriter = csv.writer(csvfile)
            # writing the fields
            csvwriter.writerow(fields)
            # writing the data rows
            csvwriter.writerows(coins)
        bucket = "exchange-stats"
        file_name = "idax-%s" %timestr
        s3_path = 'idax/%s.csv' %file_name
        body = ' '.join(map(str, coins))
        print(body)
        res = s3.put_object(Body=body, Bucket=bucket,Key=s3_path)
        print(res)
        return ''
    except BaseException as e:
        print(str(e))
        return ''

if __name__ == "__main__":
    idax()