from minio import Minio
from minio.error import *
import boto3
from io import BytesIO

from minio.commonconfig import REPLACE, CopySource

host = 'localhost:9000'
access_key = 'minio'
secret_key = 'miniostorage'

client = Minio(host, access_key=access_key,
                    secret_key=secret_key, secure=False)

# List objects information whose names starts with "steam".
objects = client.list_objects("bronze", prefix="topics/steam/partition=0/")

for obj in objects:
    
    objectName = obj.object_name
    objectName = objectName.replace('.bin', '.json')
    
    if '.json' not in obj.object_name:
        
        result = client.copy_object(
            "bronze",
            objectName,
            CopySource("bronze", obj.object_name),
        )

        client.remove_object("bronze", obj.object_name)