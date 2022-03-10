from kafka import KafkaProducer
import json
#from data import get_registered_user
import time
import sys

sys.path.append('extract_reviews_steam_api/scripts')
from get_last_review import get_reviews_for_each_game
from steam_api_extract import update_last_review

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Instances the Kafka Producer passing the IP of cluster
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                        value_serializer=json_serializer)

if __name__ == '__main__':
    while True:
        appid, reviews = get_reviews_for_each_game()
        
        if len(reviews) > 0:

            print()

            for review in reviews:
                producer.send(topic='steam',   value=review)
                print(f'Sending: {review}')
                time.sleep(1)
                pass

            update_last_review(appid, reviews[0]['recommendationid'])

        else:
            print('Nenhum review novo.')
            time.sleep(60)