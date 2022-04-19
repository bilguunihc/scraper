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
import botocore
import boto3

s3 = boto3.client("s3")

def coinhub():
    driver = webdriver.Chrome(options=chrome_options)
    driver.get("https://sapi.coinhub.mn/v1/market/tickers")
    time.sleep(5)
    elem = driver.find_element(by=By.XPATH, value="//*")
    source_code = elem.get_attribute("innerHTML")
    xml = BeautifulSoup(source_code, 'lxml')
    pre = xml.find('pre').text
    data = json.loads(pre)
    coins = []
    for key, val in data['data'].items():
        timestamp = val['timestamp'] / 1000
        coin = [datetime.now(),
                datetime.now().date(),
                datetime.fromtimestamp(timestamp),
                'coinhub',
                val['market'],
                val['change'],
                val['volume'],
                val['high'],
                val['low'],
                val['open']]
        coins.append(coin)
    # field names
    fields = ['created_at', 'date', 'market_time', 'exchange_name', 'name', 'diff', 'volume', 'max24', 'min24', 'last_price']
    # name of csv file
    timestr = time.strftime("%Y%m%d-%H%M%S")
    filename = "coinhub-%s.csv" %timestr
    with open(filename, 'w') as csvfile:
        	# creating a csv writer object
        	csvwriter = csv.writer(csvfile)
        	# writing the fields
        	csvwriter.writerow(fields)
        	# writing the data rows
        	csvwriter.writerows(coins)

    bucket = "exchange-stats"
    file_name = "coinhub-%s" %timestr
    s3_path = 'coinhub/%s.csv' %file_name
    body = ' '.join(map(str, coins))
    print(body)
    res = s3.put_object(Body=body, Bucket=bucket,Key=s3_path)
    print(res)
    return ''

if __name__ == "__main__":
    coinhub()
    