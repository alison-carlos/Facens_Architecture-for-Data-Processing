from kafka import KafkaProducer
import json
import time
import sys
import io

sys.path.append('Facens_Architecture-for-Data-Processing/scripts/extract_reviews_from_steam_api')
from get_last_review import get_reviews_for_each_game
from steam_api_extract import update_last_review

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Instances the Kafka Producer passing the IP of cluster
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8-sig'))

if __name__ == '__main__':
    while True:
        reviews_list = get_reviews_for_each_game()
        
        if len(reviews_list) > 0:

            for review in reviews_list:
                print(review)
                producer.send(topic='steam', value=review)
                time.sleep(1)

            update_last_review(review['appid'], review['recommendationid'])

        else:
            print('Nenhum review novo.')
            time.sleep(60)