from pymongo import MongoClient
import logging

# Logging config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_env_value(param: str, filename=".env"):
    """Reads the specified parameter's value from the .env file."""
    with open(filename, "r") as file:
        for line in file:
            if line.startswith(f"{param}="):
                return line.strip().split("=", 1)[1]


def logToDB():
    try:
        mongodb_uri = get_env_value("MONGODB_URI")
        db_name = get_env_value("DB_NAME")

        # Parse MONGODB_URI to extract XXX and YYY values
        mongodb_uri_parts = mongodb_uri.split(":")
        mongodb_host = mongodb_uri_parts[0]
        mongodb_port = int(mongodb_uri_parts[1])

        # Manage Database Connection
        client = MongoClient(mongodb_host, mongodb_port)
        db = client[db_name]

        # Checking connection
        client.server_info()
        logger.info("Connection to MongoDB succeed")

    except Exception as e:
        logger.error(f"Connection failed to MongoDB: {str(e)}")
        raise

    return db
