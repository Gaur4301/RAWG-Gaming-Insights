from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from datetime import date
import requests
import boto3
import json
ACCESS_KEY = ""
SECRET_KEY = ""
BUCKET_NAME = ""
s3 = boto3.client(
    "s3",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)
url=f'https://api.rawg.io/api/games?key={api_key}&page=1'
cnt=0
while (url ) and ( cnt<100):
    res=requests.get(url)
    if res.status_code==200:
        data=res.json()
        today=date.today()
        FILE_NAME = f"data-{today}-{cnt}.json"
        json_data = json.dumps(data)

        # Upload to S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=FILE_NAME,
            Body=json_data,
            ContentType="application/json"
        )

        print(f"JSON file '{FILE_NAME}' uploaded successfully to S3!")
        url=data['next']
        # print(url)
        cnt+=1
    else :
        print(f"Error : {res.status_code}-{res.text}")
        break

url2=f'https://api.rawg.io/api/stores?key={api_key}&page=1'
cnt=0
while (url2 ) :
    res=requests.get(url2)
    if res.status_code==200:
        data=res.json()
        today=date.today()
        FILE_NAME = f"data-{today}-{cnt}.json"
        json_data = json.dumps(data)

        # Upload to S3
        s3.put_object(
            Bucket='mystoresofgames',
            Key=FILE_NAME,
            Body=json_data,
            ContentType="application/json"
        )

        print(f"JSON file '{FILE_NAME}' uploaded successfully to S3!")
        url2=data['next']
        cnt+=1
        # print(url)
        
    else :
        print(f"Error : {res.status_code}-{res.text}")
        break

url3=f'https://api.rawg.io/api/genres?key={api_key}&page=1'
cnt=0
while (url3 ) :
    res=requests.get(url3)
    if res.status_code==200:
        data=res.json()
        today=date.today()
        FILE_NAME = f"data-{today}-{cnt}.json"
        json_data = json.dumps(data)

        # Upload to S3
        s3.put_object(
            Bucket='mygenres',
            Key=FILE_NAME,
            Body=json_data,
            ContentType="application/json"
        )

        print(f"JSON file '{FILE_NAME}' uploaded successfully to S3!")
        url3=data['next']
        cnt+=1
        # print(url)
        
    else :
        print(f"Error : {res.status_code}-{res.text}")
        break

url4=f'https://api.rawg.io/api/platforms?key={api_key}&page=1'
cnt=0
while (url4 ) :
    res=requests.get(url4)
    if res.status_code==200:
        data=res.json()
        today=date.today()
        FILE_NAME = f"data-{today}-{cnt}.json"
        json_data = json.dumps(data)

        # Upload to S3
        s3.put_object(
            Bucket='myplatforms',
            Key=FILE_NAME,
            Body=json_data,
            ContentType="application/json"
        )

        print(f"JSON file '{FILE_NAME}' uploaded successfully to S3!")
        url4=data['next']
        cnt+=1
        # print(url)
        
    else :
        print(f"Error : {res.status_code}-{res.text}")
        break