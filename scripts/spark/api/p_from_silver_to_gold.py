import findspark
findspark.init()
import pyspark

from pyspark import SparkContext
from pyspark.sql import SparkSession

import pandas as pd
import pyspark.pandas as ps


from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, FloatType

from pyspark.sql.functions import lit

from pyspark.sql import functions as f
from pyspark.sql import types as t
from datetime import datetime

import sys

sys.path.append('/home/acsantos/Documents/Facens_Architecture-for-Data-Processing/scripts/minio')
from list_games import fn_get_games_in_gold_layer

sys.path.append('/home/acsantos/Documents/Facens_Architecture-for-Data-Processing/scripts/extract_reviews_from_steam_api/')
from credentials import credentials

sys.path.append('/home/acsantos/Documents/Facens_Architecture-for-Data-Processing/scripts/mongodb')
from update_list_of_files_processed import update_path_list

from minio import Minio
from minio.error import *
import boto3
from io import BytesIO

host = 'localhost:9000'
access_key = 'minio'
secret_key = 'miniostorage'

client = Minio(host, access_key=access_key,
                    secret_key=secret_key, secure=False)

from pymongo import MongoClient, collection
import urllib.parse
import pymongo

import time
import logging

#Configurações de log
logging.basicConfig(

    format='%(asctime)s %(levelname)-8s %(message)s',
    filename='/home/acsantos/Documents/Facens_Architecture-for-Data-Processing/logs/spark/from_silver_to_gold' + time.strftime('%Y%m%d-%H%M%S') +'.log',
    level=logging.DEBUG,
    datefmt='%Y%m%d-%H%M%S'
)

credentials = credentials()

username = urllib.parse.quote_plus(credentials['username'])
password = urllib.parse.quote_plus(credentials['password'])

CONNECTION_STRING = f'mongodb://{username}:{password}@localhost:27017/steam'

def get_silver_layer_metadata(prefix=None):
    '''
    Retorna uma lista com os metadados presentes no bucket silver.
    
    Parametro:
    -prefix: caminho + nome do arquivo.
    
    Output:
    -appid: id do Jogo.
    -silverPath: Diretorio de todos os arquivos localizados na camada Silver.
    -silverLastModified: data de modificação.
    -silverSize: tamanho do arquivo.
    '''
    objects = client.list_objects(
        "silver", 
        prefix=prefix, 
        recursive=True
    )

    games_list = list()

    for obj in objects:
        result = client.stat_object("silver", obj.object_name)

        app_id = obj.object_name[37:]
        app_id = app_id[:app_id.index('/')]

        game = {
        'appid' : app_id,
        'silverPath' : obj.object_name,
        'silverLastModified' : result.last_modified,
        'silverSize' : result.size

        }

        games_list.append(game)
        
    return games_list    

def get_mongodb_metadata(app_id=None):
    '''
    Retorna metadados com informações presentes no MongoDB.
    
    Parametro:
    -app_id: ID identificador do jogo.
    
    Output:
    -silverPath: Diretorio do arquivo na camada Silver. Representa o ultimo arquivo processado pela camada gold.
    '''
    # Buscando no mongoDB quais arquivos da camada Silver já foram processados para remover os mesmo da lista de arquivos que serão processados.
    client = MongoClient(CONNECTION_STRING)
    with client:
        db = client.steam
        games = db.games.find({'appid' : app_id})
    
        for game in games:
            silverPath = game['silverPath']
            
        return silverPath

# Iniciando sessão do Spark

spark = SparkSession.builder.appName('ETL - Send to Gold').getOrCreate()

def load_config(spark_context: SparkContext):
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.path.style.access', 'true')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 'http://localhost:9000')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.connection.ssl.enabled', 'false')
    
load_config(spark.sparkContext)

reviews_schema = StructType(
    [StructField("_c0", IntegerType(), False),
     StructField("app_id", IntegerType(), False),
     StructField("app_name", StringType(), False),
     StructField("review_id", IntegerType(), False),
     StructField("language", IntegerType(), False),
     StructField("timestamp_created", IntegerType(), False),
     StructField("timestamp_updated", IntegerType(), False),
     StructField("recommended", StringType(), False),
     StructField("votes_helpful", IntegerType(), False),
     StructField("votes_funny", IntegerType(), False),
     StructField("weighted_vote_score", IntegerType(), False),
     StructField("comment_count", IntegerType(), False),
     StructField("steam_purchase", StringType(), False),
     StructField("received_for_free", StringType(), False),
     StructField("written_during_early_access", StringType(), False),
     StructField("author.steamid", IntegerType(), False),
     StructField("author.num_games_owned", IntegerType(), False),
     StructField("author.num_reviews", IntegerType(), False),
     StructField("author.playtime_forever", IntegerType(), False),
     StructField("author.playtime_last_two_weeks", IntegerType(), False),
     StructField("author.playtime_at_review", IntegerType(), False),
     StructField("author.last_played", IntegerType(), False)])


def fn_move_from_silver_to_gold():

    app_id = fn_get_games_in_gold_layer()

    for appid in app_id:

        print(f'Processando appid: {appid}')
        
        #1º Busca as informações na camada silver.
        prefix = '/steam_reviews/reviews.parquet/app_id=' + str(appid) + '/'
        game_info = get_silver_layer_metadata(prefix)

        print(game_info)
        
        #4º Iniciando leitura dos dados e envio para camada Gold.
        if len(game_info) > 0:

            for game in game_info:

                df = spark.read.parquet(f's3a://silver/steam_reviews/reviews.parquet/app_id='+appid, multiLine=True, header=True, schema=reviews_schema) 
                df = df.withColumn("appid", lit(game['appid']))

                try:

                    df2 = df.select('appid', 'recommendationid', 'language', 'steamid',\
                    'playtime_last_two_weeks', 'num_games_owned', 'playtime_forever',\
                    'review', 'votes_up', 'votes_funny', 'timestamp_created')

                    df2.write.partitionBy('appid').mode('append').parquet('s3a://gold/steam_reviews/reviews.parquet')

                except Exception as e:

                    df.write.partitionBy('appid').mode('append').parquet('s3a://gold/steam_reviews/reviews.parquet')


    return 0