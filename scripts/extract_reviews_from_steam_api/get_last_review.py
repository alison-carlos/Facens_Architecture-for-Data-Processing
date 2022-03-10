from pymongo import MongoClient, collection
import urllib.parse
import pymongo
from steam_api_extract import get_review_updates_for_app_id
from credentials import credentials

credentials = credentials()

def get_reviews_for_each_game():

    username = urllib.parse.quote_plus(credentials['username'])
    password = urllib.parse.quote_plus(credentials['password'])

    CONNECTION_STRING = f'mongodb://{username}:{password}@localhost:27017/steam'
    client = MongoClient(CONNECTION_STRING)

    with client:
            db = client.steam
            games = db.games.find()

            for game in games:

                name = game['name']
                appid = game['appid']
                last_review_retrieved = game['last_review_retrieved']
                reviews = get_review_updates_for_app_id(appid, last_review_retrieved)
    
    return appid, reviews
