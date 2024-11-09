from datetime import datetime
from pymongo import MongoClient
from RiotApi import *
import logging

# Logging config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Reading api_key from params.txt
def get_api_key(filename="params.txt"):
    with open(filename, "r") as file:
        for line in file:
            if line.startswith("API_KEY"):
                return line.strip().split("=")[1]  # get value after api_key


# Lecture de la clé API
API_KEY = get_api_key()
PUUID_URL = 'https://europe.api.riotgames.com/riot/account/v1/accounts/by-riot-id/'
MATCHSLIST_URL = "https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/"
MATCHDATA_URL = "https://europe.api.riotgames.com/lol/match/v5/matches/"

# Connection to DB
try:
    # Manage Database Connection
    client = MongoClient('localhost', 27017)
    db = client['Extia_Gaming_LoL_2024']
    players_collection = db['players']
    matchs_collection = db['matchs']

    # Vérifier la connexion
    client.server_info()
    logger.info("Connection to MongoDB succeed")

except Exception as e:
    logger.error(f"Connection failed to MongoDB: {str(e)}")
    raise

# Update Players Database
try:
    # Find all players without PUUID
    players_without_puuid = players_collection.find({
        "$or": [
            {"puuid": {"$exists": False}},
            {"puuid": None},
            {"puuid": ""}
        ]
    })

    # Check whether the player already exists (based on gameName and tagLine)
    for player in players_without_puuid:
        print(f"Processing player: {player.get('gameName', 'Unknown')}#{player.get('tagLine', 'Unknown')}")

        # Keep the original _id
        player_id = player['_id']

        update_player = {
            "gameName": player.get('gameName', 'Unknown'),
            "tagLine": player.get('tagLine', 'Unknown'),
            "team": player.get('team', 'Unknown'),
            "last_updated": datetime.utcnow()
        }

        try:
            # Get PUUID from Riot API
            update_player['puuid'] = get_puuid(PUUID_URL, API_KEY, update_player)
        except Exception as e:
            logger.error(f"Error when retrieving puuid: {str(e)}")
            continue

        # Update insertion method
        try:
            # Update the existing document
            update_result = players_collection.update_one(
                {"_id": player_id},
                {
                    "$set": update_player
                }
            )

        except Exception as e:
            logger.error(f"Fail when updating data: {str(e)}")

except Exception as e:
    logger.error(f"General Fail: {str(e)}")

# Update Matchs data
try:
    players_with_puuid = players_collection.find({
        "$and": [
            {"puuid": {"$exists": True}},
            {"puuid": {"$ne": ""}}
        ]
    })

    for player in players_with_puuid:
        print(f"processing {player['gameName']}")
        match_ids = get_matchlist(MATCHSLIST_URL, API_KEY, player['puuid'], 1727114400)  # Games from 23/09

        for match_id in match_ids:
            print(f"processing {match_id}")
            if matchs_collection.count_documents({"match_id": match_id}) == 0:
                try:
                    match_data = get_matchdata(MATCHDATA_URL, API_KEY, match_id)
                except Exception as e:
                    logger.error(f"Could not get matchdata: {str(e)}")
                    break
                try:
                    match_data['match_id'] = match_id
                    match_data['created_at'] = datetime.utcnow()
                    matchs_collection.insert_one(match_data)
                except Exception as e:
                    logger.error(f"Match not inserted: {str(e)}")

except Exception as e:
    logger.error(f"General Fail: {str(e)}")

