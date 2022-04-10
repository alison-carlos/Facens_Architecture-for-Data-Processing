import sys

sys.path.append('/home/acsantos/Documents/Facens_Architecture-for-Data-Processing/scripts/extract_reviews_from_steam_api/')
from credentials import credentials

from pymongo import MongoClient, collection
import urllib.parse

credentials = credentials()

username = urllib.parse.quote_plus(credentials['username'])
password = urllib.parse.quote_plus(credentials['password'])

CONNECTION_STRING = f'mongodb://{username}:{password}@localhost:27017/steam'

def update_path_list(appid=None, path=None):

    client = MongoClient(CONNECTION_STRING)

    with client:
        db = client.steam
        games = db.games

        games.update_one({'appid': str(appid)}, {'$push': {'silverPath': path}}, upsert = True)