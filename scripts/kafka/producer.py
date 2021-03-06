from kafka import KafkaProducer
import json
import time
import sys
import io

import time 
import logging

#Configurações de log
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    filename='/home/acsantos/Documents/Facens_Architecture-for-Data-Processing/logs/kafka/producer_' + time.strftime('%Y%m%d-%H%M%S') +'.log',
    level=logging.DEBUG,
    datefmt='%Y%m%d-%H%M%S'
)

sys.path.append('/home/acsantos/Documents/Facens_Architecture-for-Data-Processing/scripts/extract_reviews_from_steam_api')
from get_last_review import get_reviews_for_game
from steam_api_extract import update_last_review

sys.path.append('/home/acsantos/Documents/Facens_Architecture-for-Data-Processing/scripts/minio')
from list_games import fn_get_games_in_gold_layer
from convert_to_json import convert_to_json

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Instances the Kafka Producer passing the IP of cluster
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8-sig'))

def fn_start_producer():

    logging.info(f'Iniciando processo.')  
    list_appid = fn_get_games_in_gold_layer()

    for appid in list_appid:

        reviews_list = get_reviews_for_game(appid)
        
        if len(reviews_list) > 0:
            recommendationid = 0

            for review in reviews_list:
                recommendationid = int(review['recommendationid']) if int(review['recommendationid']) > recommendationid else recommendationid
                producer.send(topic='steam', value=review)
                #convert_to_json() # Converte o arquivo .bin em .json no bucket
                
            update_last_review(game_id=appid, last_review_retrieved=recommendationid)

        else:
            print('Nenhum review novo.')
    
    return 0


if __name__ == '__main__':
    fn_start_producer()