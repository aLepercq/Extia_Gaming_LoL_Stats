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


