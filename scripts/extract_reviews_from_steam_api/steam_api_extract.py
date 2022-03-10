import requests
import json
from pymongo import MongoClient
import urllib.parse
from update_last_review import update_last_review
from credentials import credentials
import time 

credentials = credentials()

username = urllib.parse.quote_plus(credentials['username'])
password = urllib.parse.quote_plus(credentials['password'])

CONNECTION_STRING = f'mongodb://{username}:{password}@localhost:27017/steam'
client = MongoClient(CONNECTION_STRING)

# Parametros da API da Steam
CURSOR_KEY = 'cursor'
RECOMMENDATION_ID = 'recommendationid'
REVIEWS_KEY = 'reviews'
STEAM_API_BASE_URL = 'https://store.steampowered.com/'
SORTED_APP_REVIEWS_PATH = 'appreviews/{}'
MANDATORY_QUERY_PARAMS = '?json=1&num_per_page=100&filter=recent'
CURSOR_QUERY_PARAM = '&cursor={}'

def get_app_reviews_from_steam(app_id, cursor=None):
    url_path = SORTED_APP_REVIEWS_PATH.format(app_id)
    url = '{}{}{}'.format(STEAM_API_BASE_URL, url_path, MANDATORY_QUERY_PARAMS)
    if cursor:
        url += CURSOR_QUERY_PARAM.format(cursor.replace('+', '%2B'))

    raw_response = requests.get(url)
    return json.loads(raw_response.text)

def extract_cursor_from_response(response):
    if CURSOR_KEY in response:
        cursor = response[CURSOR_KEY]
        return cursor 
    else:
        return None

def get_review_updates_for_app_id(app_id, most_recent_review_id=None):
    reviews_to_process = []
    should_make_request = True
    found_last_new_comment = False
    cursor = None

    while should_make_request:
        response = get_app_reviews_from_steam(app_id, cursor)
        cursor = extract_cursor_from_response(response)
        for review in response[REVIEWS_KEY]:
            if review[RECOMMENDATION_ID] == most_recent_review_id:
                found_last_new_comment = True
                break
            else:
                reviews_to_process.append(review)
        should_make_request = cursor is not None and len(cursor) > 0 and (not found_last_new_comment)
    
    return reviews_to_process

def extract_reviews(game_id=None, last_review_retrieved=None):

    last_review = None
    reviews = get_review_updates_for_app_id(app_id=game_id, most_recent_review_id=last_review_retrieved)
    return reviews         

    
    

