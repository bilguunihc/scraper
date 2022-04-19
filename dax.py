from datetime import datetime
import requests
import json
import time
import csv
import botocore
import boto3
import uuid

s3 = boto3.client("s3")

def dax():
    url = 'https://api.dax.mn/v1/graphql'
    pay_load = {
        "operationName": "ActivePairs",
        "query": "query ActivePairs {sys_pair(where: {is_active: {_eq: true}}, order_by: {id: asc})  {id  symbol    base_max_size   base_min_size    base_tick_size    quote_max_size   quote_min_size    quote_tick_size    baseAsset {      id      code      name      scale      is_crypto      __typename   }    quoteAsset {      id      code      name      scale      __typename    }  price {     last_price     last_price_dt     __typename   }   stats24 {      high  low     change24h      vol      __typename    }    __typename }}",
        "variables": {}
    }
    headers = {
        'content-type': 'application/json'
    }
    response = requests.post(url, headers=headers, data=json.dumps(pay_load))
    data = response.json()
    pairs = data['data']['sys_pair']
    fields = ['created_at', 'date', 'market_time', 'exchange_name', 'name', 'diff', 'volume', 'max24', 'min24', 'last_price']
    timestr = time.strftime("%Y%m%d-%H%M%S")
    # name of csv file
    file_name = "dax-%s" %timestr
    filename = "dax-%s.csv" %timestr 
    coins = []
    coins.append(fields)
    for pair in pairs:
        coin = [str(datetime.now()),
                str(datetime.now().date()),
                str(pair['price']['last_price_dt']),
                'dax',
                str(uuid.uuid4()),
                str(pair['symbol']),
                str(pair['stats24']['change24h']),
                str(pair['stats24']['vol']),
                str(pair['stats24']['high']),
                str(pair['stats24']['low']),
                str(pair['price']['last_price'])]
        txt = ' '.join(map(str, coin))
        txt_line = txt + '\n'
        coins.append(txt_line)

    # writing to csv file
    with open(filename, 'w') as csvfile:
        # creating a csv writer object
        csvwriter = csv.writer(csvfile)
        # writing the fields
        csvwriter.writerow(fields)
        # writing the data rows
        csvwriter.writerows(coins)
    bucket = "exchange-stats"
    s3_path = 'dax/%s.csv' %file_name
    body = ' '.join(map(str, coins))
    print(body)
    res = s3.put_object(Body=body, Bucket=bucket,Key=s3_path)
    print(res)

    return ''

if __name__ == "__main__":
    dax()