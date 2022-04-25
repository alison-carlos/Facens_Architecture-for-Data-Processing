from kafka import KafkaConsumer
import json
from minio import Minio
from minio.error import *

from io import BytesIO

host = 'localhost:9000'
access_key = 'minio'
secret_key = 'miniostorage'

minioClient = Minio(host, access_key=access_key,
                    secret_key=secret_key, secure=False)


bucket = 'bronze'


if __name__=='__main__':
    consumer = KafkaConsumer(
        'steam',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest', # Specifies that consumer can star to read from the firsts messages
        group_id='minio')
    
    print('Starting the consumer')

    for msg in consumer:
        try:
            content = BytesIO(bytes(msg.value))

            recommendationid = json.loads(msg.value)
            
            key = 'topics/steam/' + recommendationid['recommendationid'] + '.json'
            size = content.getbuffer().nbytes

            try:
                minioClient.put_object(bucket, key, content, size)
            except ValueError as err:
                print('Error: ', err)

        except ValueError:
            print('error')