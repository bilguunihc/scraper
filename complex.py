from datetime import datetime
from selenium import webdriver
import requests
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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

def complex():
    driver = webdriver.Chrome(options=chrome_options)
    driver.get("https://complex.mn/markets")
    time.sleep(10)
    driver.set_window_size(2000, 2000)
    table = driver.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div/div/div/div[2]/div[1]/div/div/div/div/div/table/tbody')
    source_code = table.get_attribute('innerHTML')
    xml = BeautifulSoup(source_code, 'lxml')
    # field names
    fields = ['created_at', 'date', 'market_time', 'exchange_name', 'name', 'diff', 'volume', 'max24', 'min24', 'last_price']
    # name of csv file
    timestr = time.strftime("%Y%m%d-%H%M%S")
    filename = "complex-%s.csv" %timestr
    coins = []
    for row in table.find_elements_by_xpath(".//tr"):
        data = [td.text for td in row.find_elements_by_tag_name("td")]
        coin = [str(datetime.now()),
                str(datetime.now().date()),
                str(datetime.now()),
                'complex',
                str(uuid.uuid4()),
                str(data[0].replace('\n', '')),
                str(data[2].replace('%', '')),
                str(data[3].split()[0].replace(',', '')),
                str(data[5].split()[0].replace(',', '')),
                str(data[4].split()[0].replace(',', '')),
                str(data[1].split()[0].replace(',', ''))]
        txt = ' '.join(map(str, coin))
        txt_line = txt + '\n'
        coins.append(txt_line)

    with open(filename, 'w') as csvfile:
        # creating a csv writer object
        csvwriter = csv.writer(csvfile)
        # writing the fields
        csvwriter.writerow(fields)
        # writing the data rows
        csvwriter.writerows(coins)

    bucket = "exchange-stats"
    file_name = "complex-%s" %timestr
    s3_path = 'complex/%s.csv' %file_name
    body = ' '.join(map(str, coins))
    print(body)
    res = s3.put_object(Body=body, Bucket=bucket,Key=s3_path)
    print(res)
    return ''


if __name__ == "__main__":
    complex()