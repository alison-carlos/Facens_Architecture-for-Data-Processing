from time import sleep, time
from pymongo import MongoClient, collection
import urllib.parse
import pymongo
from steam_api_extract import extract_reviews
from credentials import credentials
import json 

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
                reviews_list = extract_reviews(appid, last_review_retrieved)
                
    return reviews_list
