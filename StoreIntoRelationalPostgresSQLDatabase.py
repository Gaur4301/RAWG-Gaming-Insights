dim_games=spark.read.table('gamesdata.dim_games')
dim_games.show()
jdbc_url = "jdbc:postgresql://database.rds.amazonaws.com:5432/GamesDatabase"
user=''
password=''
driver= "org.postgresql.Driver"
dim_games.write.format('jdbc').\
    option('url',jdbc_url).\
    option('dbtable','public.dim_games').\
    option('user',user).\
    option('password',password).\
    mode('overwrite').\
    save()      
fact_games=spark.read.table('gamesdatabase.fact_games')
fact_games.show()
fact_games.write.format('jdbc').\
    option('url',jdbc_url).\
    option('dbtable','public.fact_games').\
    option('user',user).\
    option('password',password).\
    mode('overwrite').\
    save()  
fact_platform=spark.read.table('gamesdatabase.fact_platform')
fact_platform.show()
fact_platform.write.format('jdbc').\
    option('url',jdbc_url).\
    option('dbtable','public.fact_platform').\
    option('user',user).\
    option('password',password).\
    mode('overwrite').\
    save() 
fact_genres=spark.read.table('gamesdatabase.fact_genres')
fact_genres.show()
fact_genres.write.format('jdbc').\
    option('url',jdbc_url).\
    option('dbtable','public.fact_genres').\
    option('user',user).\
    option('password',password).\
    mode('overwrite').\
    save()  
fact_ratings=spark.read.table('gamesdatabase.fact_ratings')
fact_ratings.show()
fact_ratings.write.format('jdbc').\
    option('url',jdbc_url).\
    option('dbtable','public.fact_ratings').\
    option('user',user).\
    option('password',password).\
    mode('overwrite').\
    save() 
fact_stores=spark.read.table('gamesdatabase.fact_stores')
fact_stores.show()
fact_stores.write.format('jdbc').\
    option('url',jdbc_url).\
    option('dbtable','public.fact_stores').\
    option('user',user).\
    option('password',password).\
    mode('overwrite').\
    save()  