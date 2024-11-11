from datetime import datetime
from pymongo import MongoClient
from riotApi import *
import logging

# Logging config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Reading api_key from .env
def get_env_value(param: str, filename=".env"):
    """Reads the specified parameter's value from the .env file."""
    with open(filename, "r") as file:
        for line in file:
            if line.startswith(f"{param}="):
                return line.strip().split("=", 1)[1]  # Get value after 'param='


# Read API_KEY, MONGODB_URI, DB_NAME, and PUUID_URL from .env file
API_KEY = get_env_value("API_KEY")
MONGODB_URI = get_env_value("MONGODB_URI")
DB_NAME = get_env_value("DB_NAME")
PUUID_URL = get_env_value("PUUID_URL")
MATCHSLIST_URL = get_env_value("MATCHSLIST_URL")
MATCHDATA_URL = get_env_value("MATCHDATA_URL")
MATCHTIMELINE_URL = get_env_value("MATCHTIMELINE_URL")

# Parse MONGODB_URI to extract XXX and YYY values
mongodb_uri_parts = MONGODB_URI.split(":")
MONGODB_HOST = mongodb_uri_parts[0]
MONGODB_PORT = mongodb_uri_parts[1]

# Connection to DB
try:
    # Manage Database Connection
    client = MongoClient(MONGODB_HOST, 27017)
    db = client['Extia_Gaming_LoL_2024']
    players_collection = db['players']
    matchs_collection = db['matchs']
    timelines_collection = db['timelines']

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
            update_player['puuid'] = getPuuid(PUUID_URL, API_KEY, update_player)
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
        match_ids = getMatchlist(MATCHSLIST_URL, API_KEY, player['puuid'], 1727114400)  # Games from 23/09

        for match_id in match_ids:
            if matchs_collection.count_documents({"match_id": match_id}) == 0:
                print(f"processing {match_id}")
                try:
                    match_data = getMatchData(MATCHDATA_URL, API_KEY, match_id)
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

# Update timelines data
try:
    time.sleep(0.5)
    # Get all match_id of timelines
    existing_timeline_ids = {
        timeline['match_id'] for timeline in
        timelines_collection.find({"match_id": {"$exists": True, "$ne": ""}}, {"match_id": 1})}

    # getting matchs id in matchs collection that are not yet in timelines
    matchs_without_timeline = matchs_collection.find({
        "$and": [
            {"match_id": {"$exists": True, "$ne": ""}},
            {"match_id": {"$nin": list(existing_timeline_ids)}}
        ]
    })

    for match in matchs_without_timeline:
        match_id = match['match_id']
        print(f"Processing match: {match_id}")

        try:
            # Retrieve timeline data for the match
            timeline = getMatchTimeLine(MATCHTIMELINE_URL, API_KEY, match_id)

            # Update or insert the timeline data in timelines collection
            try:
                update_result = timelines_collection.update_one(
                    {"match_id": match_id},
                    {"$set": timeline},
                    upsert=True  # Insert if the document doesn't exist
                )
                if not update_result.upserted_id:
                    logger.info(f"Updated existing timeline for match_id: {match_id}")

            except Exception as e:
                logger.error(f"Failed to update timeline database for match_id {match_id}: {str(e)}")

        except Exception as e:
            logger.error(f"Failed to retrieve timeline for match_id {match_id}: {str(e)}")

except Exception as e:
    logger.error(f"General failure in updating timelines: {str(e)}")
