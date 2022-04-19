from datetime import datetime
import requests
import time
import csv
import boto3
import uuid

s3 = boto3.client("s3")

def trade():
    try:
        url = 'https://trade.mn:116/api/v2/exchange/checkpair?pair=TRD/MNT'
        min_max_url = 'https://trade.mn:116/api/v2/exchange/getMinMax'
        headers = {
            'content-type': 'application/json'
        }
        response = requests.get(url)
        data = response.json()
        pairs = data['pairName']
        coins = []
        for pair in pairs:
            batch = dict()
            batch['currency1'] = pair['fromCurrencyId']
            batch['currency2'] = pair['toCurrencyId']
            response = requests.post(min_max_url, headers=headers, json=batch)
            
            min_max = response.json()
            coin = []
            try:
                coin = [str(datetime.now()),
                        str(datetime.now().date()),
                        str(pair['createDate']),
                        'trade',
                        str(uuid.uuid4()),
                        str(pair['name']),
                        str(pair['diff']),
                        str(pair['q']),
                        str(min_max[0]['max24']),
                        str(min_max[0]['min24']),
                        str(pair['lastPrice'])]
            except:
                pass
            txt = ' '.join(map(str, coin))
            txt_line = txt + '\n'
            coins.append(txt_line)
            
        # field names
        fields = ['created_at', 'date', 'market_time', 'exchange_name', 'name', 'diff', 'volume', 'max24', 'min24', 'last_price']
        # name of csv file
        timestr = time.strftime("%Y%m%d-%H%M%S")
        filename = "trade-%s.csv" %timestr 
	
        # writing to csv file
        with open(filename, 'w') as csvfile:
        	# creating a csv writer object
        	csvwriter = csv.writer(csvfile)
        	# writing the fields
        	csvwriter.writerow(fields)
        	# writing the data rows
        	csvwriter.writerows(coins)
        bucket = "exchange-stats"
        file_name = "trade-%s" %timestr
        s3_path = 'trade/%s.csv' %file_name
        body = ' '.join(map(str, coins))
        print(body)
        res = s3.put_object(Body=body, Bucket=bucket,Key=s3_path)
        print(res)
        return 'ok'
        
    except BaseException as e:
        print(str(e))
        return 'aldaa'

if __name__ == "__main__":
    trade()