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

csAt15List = []
for match in timelines_collection.find():
    match_id = match.get("match_id", "")

    # Créer une correspondance entre participantId et puuid à partir de metadata.participants
    participant_id_to_puuid = {
        i+1: puuid for i, puuid in enumerate(match.get("metadata", {}).get("participants", []))  # id start at 1
    }

    # Accéder à la 15e frame
    frame15 = match.get("info", {}).get("frames", [])[15].get("participantFrames", {})

    # Initialiser un dictionnaire pour stocker les informations de minions pour chaque puuid
    for participant_id, data in frame15.items():
        puuid = participant_id_to_puuid.get(data.get("participantId"))
        if puuid:
            # Calculer le score CS pour chaque participant et utiliser le puuid
            csAt15List.append([match_id, puuid, data.get("minionsKilled", 0) + data.get("jungleMinionsKilled", 0)])

df = pd.DataFrame(csAt15List, columns=['match_id', 'puuid', 'csAt15'])
pipeline = [
        # Match le document avec le bon match_id
        {
            "$match": {
                "metadata.matchId": match_id
            }
        },
        # Déplie le tableau participants
        {
            "$unwind": "$info.participants"
        },
        # Match le participant avec le bon puuid
        {
            "$match": {
                "info.participants.puuid": puuid
            }
        },
        # Projette uniquement la teamPosition
        {
            "$project": {
                "_id": 0,
                "teamPosition": "$info.participants.teamPosition"
            }
        }
    ]
player_names = {
    doc["puuid"]: doc["name"]
    for doc in players_collection.find(
        {"puuid": {"$in": df["puuid"].unique().tolist()}},
        {"puuid": 1, "name": 1, "_id": 0}
    )
}

df["name"] = df["puuid"].map(player_names)
df = df.drop(['puuid'], axis=1)
df = df.reindex(columns=["match_id", "name", "csAt15"])
df.to_excel("test.xlsx", index=False, engine="openpyxl")
logger.info(f"Data exported to test.xlsx")

# Finding all matches with missing puuid in player collection
existing_puuids = set(player["puuid"] for player in players_collection.find({}, {"puuid": 1}))
pipeline = [
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
results = list(timelines_collection.aggregate(pipeline))
for result in results:
    print(result)
