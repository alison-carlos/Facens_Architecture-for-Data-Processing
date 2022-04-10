from minio import Minio
from minio.error import *
import boto3
from io import BytesIO


host = 'localhost:9000'
access_key = 'minio'
secret_key = 'miniostorage'

client = Minio(host, access_key=access_key,
                    secret_key=secret_key, secure=False)

def fn_get_games_in_gold_layer():

    # List objects information whose names starts with "steam".
    objects = client.list_objects("gold", prefix="/steam_reviews/reviews.parquet/")

    appid = list()

    for obj in objects:
        appid.append(obj.object_name) 

    # Removendo "_SUCCESS" file
    appid.pop(0)

    list_appid = list()

    for a in appid:
        a = a.replace('steam_reviews/reviews.parquet/appid=', '')
        a = a.replace('/','')
        list_appid.append(a)
        
    return list_appid


