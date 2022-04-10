from minio import Minio
from minio.error import *
import boto3
from io import BytesIO

from minio.commonconfig import REPLACE, CopySource

import logging
import time 

host = 'localhost:9000'
access_key = 'minio'
secret_key = 'miniostorage'

client = Minio(host, access_key=access_key,
                    secret_key=secret_key, secure=False)

#Configurações de log
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    
    filename='/home/acsantos/Documents/Facens_Architecture-for-Data-Processing/logs/minio/move_files_' + time.strftime('%Y%m%d-%H%M%S') +'.log',
    level=logging.DEBUG,
    datefmt='%Y%m%d-%H%M%S'
)

def fn_move_files(bucket=None, sourcePath=None, destinationPath=None):
    '''
    Move os arquivos no bucket do Minio.

    * bucket: nome do bucket, exemplo: 'meu-bucket'.
    * sourcePath: caminho do arquivo e nome, exemplo: '/pasta1/exemplo.txt'
    * destinationPath: caminho do local de destino junto com nome do arquivo, exemplo: '/pasta2/exemplo.txt'
    '''
    
    objects = client.list_objects(bucket, prefix=sourcePath)
        
    for obj in objects:
        
        objectName = obj.object_name.replace(sourcePath, '')
        objectName = destinationPath + objectName
        
        result = client.copy_object(
            bucket,
            objectName,
            CopySource(bucket, obj.object_name),
        )
        
        client.remove_object(bucket, obj.object_name)