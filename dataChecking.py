from otherFunctions import *
import pandas as pd
import logging

# Logging config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Read API_KEY, MONGODB_URI, DB_NAME, and PUUID_URL from .env file
MONGODB_URI = get_env_value("MONGODB_URI")
DB_NAME = get_env_value("DB_NAME")

# Parse MONGODB_URI to extract XXX and YYY values
mongodb_uri_parts = MONGODB_URI.split(":")
MONGODB_HOST = mongodb_uri_parts[0]
MONGODB_PORT = mongodb_uri_parts[1]

# Connection to DB
db = logToDB()
players_collection = db['players']
matchs_collection = db['matchs']
timelines_collection = db['timelines']

# Étape 1 : Extraire tous les puuids existants dans la collection players
existing_puuids = set(player["puuid"] for player in players_collection.find({}, {"puuid": 1}))

# Étape 2 : Utiliser une pipeline d'agrégation pour trouver les matchs avec des puuids inconnus
pipeline_missing_players = [
    {
        "$project": {
            "match_id": "$match_id",
            "missing_puuids": {
                "$filter": {
                    "input": "$metadata.participants",
                    "as": "puuid",
                    "cond": {"$not": {"$in": ["$$puuid", list(existing_puuids)]}}
                }
            }
        }
    },
    {
        "$match": {"missing_puuids": {"$ne": []}}  # Garder seulement les matchs avec des puuids manquants
    },
    {
        "$unwind": "$missing_puuids"  # Unwrap chaque puuid manquant
    },
    {
        "$lookup": {
            "from": "players",
            "localField": "missing_puuids",
            "foreignField": "puuid",
            "as": "player_info"
        }
    },
    {
        "$unwind": {
            "path": "$player_info",
            "preserveNullAndEmptyArrays": True  # Inclure les puuids sans correspondance dans players
        }
    },
    {
        "$project": {
            "match_id": 1,
            "puuid": "$missing_puuids",
            "gameName": "$player_info.gameName",
            "tagLine": "$player_info.tagLine"
        }
    }
]

# Exécuter la requête
results = list(matchs_collection.aggregate(pipeline_missing_players))

# Afficher les résultats
for result in results:
    print(result)




