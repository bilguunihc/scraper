from datetime import datetime
import requests
import time
import csv
import boto3
import uuid

s3 = boto3.client("s3")

def x_meta():
    try:
        coins = []
        url_1 = 'https://www.x-meta.com/v1/market/trading-pair?symbol=BTC_MNTC'
        response = requests.get(url_1)
        data = response.json()
        timestamp = data['timestamp'] / 1000
        coin = [str(datetime.now()),
                str(datetime.now().date()),
                str(datetime.fromtimestamp(timestamp)),
                'x-meta',
                str(uuid.uuid4()),
                str(data['data']['symbol']),
                str(data['data']['change24h']),
                str(data['data']['amount']),
                str(data['data']['high']),
                str(data['data']['low']),
                str(data['data']['close'])]
        txt = ' '.join(map(str, coin))
        txt_line = txt + '\n'
        coins.append(txt_line)
        url_2 = 'https://www.x-meta.com/v1/market/trading-pair?symbol=IHC_MNTC'
        response = requests.get(url_2)
        data = response.json()
        timestamp = data['timestamp'] / 1000
        coin = [datetime.now(),
                datetime.now().date(),
                datetime.fromtimestamp(timestamp),
                'x-meta',
                str(uuid.uuid4()),
                data['data']['symbol'],
                data['data']['change24h'],
                data['data']['amount'],
                data['data']['high'],
                data['data']['low'],
                data['data']['close']]
        txt = ' '.join(map(str, coin))
        txt_line = txt + '\n'
        coins.append(txt_line)
        url_3 = 'https://www.x-meta.com/v1/market/trading-pair?symbol=SHIB_MNTC'
        response = requests.get(url_3)
        data = response.json()
        timestamp = data['timestamp'] / 1000
        coin = [datetime.now(),
                datetime.now().date(),
                datetime.fromtimestamp(timestamp),
                'x-meta',
                str(uuid.uuid4()),
                data['data']['symbol'],
                data['data']['change24h'],
                data['data']['amount'],
                data['data']['high'],
                data['data']['low'],
                data['data']['close']]
        txt = ' '.join(map(str, coin))
        txt_line = txt + '\n'
        coins.append(txt_line)
        
        # field names
        fields = ['created_at', 'date', 'market_time', 'exchange_name', 'name', 'diff', 'volume', 'max24', 'min24', 'last_price']
        # name of csv file
        timestr = time.strftime("%Y%m%d-%H%M%S")
        filename = "x-meta-%s.csv" %timestr
     
        # writing to csv file
        with open(filename, 'w') as csvfile:
        	# creating a csv writer object
        	csvwriter = csv.writer(csvfile)

        	# writing the fields
        	csvwriter.writerow(fields)

        	# writing the data rows
        	csvwriter.writerows(coins)
        bucket = "exchange-stats"
        file_name = "x_meta-%s" %timestr
        s3_path = 'x_meta/%s.csv' %file_name
        body = ' '.join(map(str, coins))
        print(body)
        res = s3.put_object(Body=body, Bucket=bucket,Key=s3_path)
        print(res)
        return 'ok'
    except BaseException as e:
        print(str(e))
        return 'aldaa'

if __name__ == "__main__":
    x_meta()