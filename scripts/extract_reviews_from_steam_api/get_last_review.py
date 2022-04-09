from time import sleep, time
from pymongo import MongoClient, collection
import urllib.parse
import pymongo
from steam_api_extract import extract_reviews
from credentials import credentials
import json 

credentials = credentials()

def get_reviews_for_each_game(appid=None):

    username = urllib.parse.quote_plus(credentials['username'])
    password = urllib.parse.quote_plus(credentials['password'])

    CONNECTION_STRING = f'mongodb://{username}:{password}@localhost:27017/steam'
    client = MongoClient(CONNECTION_STRING)



    with client:
            db = client.steam

            if appid is None:
                games = db.games.find()

            else:
                print(f'Iniciando busca dos reviews do appid: {appid}')
                games = db.games.find({'appid' : "1015500"})

            for game in games:

                name = game['name']
                appid = game['appid']
                last_review_retrieved = game['last_review_retrieved']

                print(f'Iniciando busca dos reviews mais recentes do game {name}, o ultimo review na base é o {last_review_retrieved}')
                reviews_list = extract_reviews(appid, last_review_retrieved)
                
    return reviews_list