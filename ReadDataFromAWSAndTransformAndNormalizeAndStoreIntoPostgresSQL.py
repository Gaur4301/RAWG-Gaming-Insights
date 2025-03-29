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
object_list=s3.list_objects_v2(Bucket='myawsgames').get("Contents")
Total_data=[]
for obj in object_list:
    obj_name=obj["Key"]
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=obj_name)
    data = json.loads(obj["Body"].read().decode("utf-8"))
    Total_data.append(data)
    print(f"Data from {obj_name}:Sucessfully loaded" )

fact_games_list=[]
dim_games_list=[]
platform_list=[]
ratings_list=[]
stores_list=[]
genres_list=[]
# platform_list
# print(Total_data[0]["results"][0])
for data in Total_data:

    if "results" in data :
        value=data.get("results",[])
        for data in value:
            value=data
            fact_games_list.append((value['id'],value['name'],value['rating'],value['playtime'],value['metacritic'],value['updated'],value['reviews_count']))
            dim_games_list.append((value['id'],value['name'],value['released']))
            game_id=value['id']
            game_name=value['name']
            ratings=data.get("ratings",[])
            for rating in ratings:
                ratings_list.append((game_id,game_name,rating['id'],rating['title'],rating['count'],rating['percent']))
            genres=data.get('genres',[])
            for genre in genres:
                genres_list.append((game_id,game_name,genre['id'],genre['name'],genre['games_count']))  

            stores=data.get('stores',[])
            for st in stores:
                store=st.get('store','{}')
                stores_list.append((game_id,game_name,store['id'],store['name'],store['games_count']))   

            platforms=data.get('platforms',[])
            for pf in platforms:
                platform=pf.get('platform','{}') 
                platform_list.append((game_id,game_name,platform['id'],platform['name'],platform['slug']) )   


            
print(platform_list)    
spark=SparkSession.builder.appName('GamesApplication').getOrCreate()
# %sql
# CREATE DATABASE IF NOT EXISTS gamesdatabase LOCATION '/view'

columns=['id','name','rating','playtime','metacritic','updated','reviews_count']
fact_games_df=spark.createDataFrame(fact_games_list,columns)
# fact_games_df.show(6)
fact_games_df.write.format('delta').saveAsTable('gamesdatabase.fact_games')

dim_games_columns=['id','name','released']
dim_games_df=spark.createDataFrame(dim_games_list,dim_games_columns)
dim_games_df.show(6)
dim_games_df.write.format('delta').saveAsTable('gamesdata.dim_games')
fact_platform_columns=['game_id','game_name','id','name','slug']
fact_platform_df=spark.createDataFrame(platform_list,fact_platform_columns)
fact_platform_df.show(6)
fact_platform_df.write.format('delta').saveAsTable('gamesdatabase.fact_platform')
fact_ratings_columns=['game_id','game_name','id','title','count','percent']
fact_ratings_df=spark.createDataFrame(ratings_list,fact_ratings_columns)
fact_ratings_df.show(6)
fact_ratings_df.write.format('delta').saveAsTable('gamesdatabase.fact_ratings')
fact_stores_columns=['game_id','game_name','id','name','games_count']
fact_stores_df=spark.createDataFrame(stores_list,fact_stores_columns)
fact_stores_df.show(6)
fact_stores_df.write.format('delta').saveAsTable('gamesdatabase.fact_stores')
fact_genres_columns=['game_id','game_name','id','name','games_count']
fact_genres_df=spark.createDataFrame(genres_list,fact_genres_columns)
fact_genres_df.show(6)
fact_genres_df.write.format('delta').saveAsTable('gamesdatabase.fact_genres')