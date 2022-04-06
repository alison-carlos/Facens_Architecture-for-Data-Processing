from pymongo import MongoClient, collection
import urllib.parse
import pymongo
from credentials import credentials

credentials = credentials()

def update_last_review(game_id=None, game_name=None, last_review_retrieved=None):

    username = urllib.parse.quote_plus(credentials['username'])
    password = urllib.parse.quote_plus(credentials['password'])

    CONNECTION_STRING = f'mongodb://{username}:{password}@localhost:27017/steam'
    client = MongoClient(CONNECTION_STRING)


    with client:

        db = client.steam
        games = db.games
        
        document = {'appid' : game_id}

        if game_name is not None:
            update_query = {'$set' : {'last_review_retrieved' : last_review_retrieved, 'name' : game_name}}
            games.update_one(document, update_query)
        else:
            update_query = {'$set' : {'last_review_retrieved' : last_review_retrieved}}
            games.update_one(document, update_query)

        return 'Update completed with success!'


def addGames(game=None):

    username = urllib.parse.quote_plus(credentials['username'])
    password = urllib.parse.quote_plus(credentials['password'])

    CONNECTION_STRING = f'mongodb://{username}:{password}@localhost:27017/steam'
    client = MongoClient(CONNECTION_STRING)

    with client:

        db = client.steam
        games = db.games
        
        games.insert_one(game)
        
        return 'Inserted completed with success.'

