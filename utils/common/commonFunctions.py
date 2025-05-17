from pymongo import MongoClient
from pymongo.server_api import ServerApi
from sklearn.preprocessing import MinMaxScaler
import logging

# Logging config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

position_dict = {
    "TOP": "TOP",
    "JUNGLE": "JGL",
    "MIDDLE": "MID",
    "BOTTOM": "BOT",
    "UTILITY": "SUP"
}

weighting = {
    "TOP": {"soloKills_mean": 1.5, "damageTakenOnTeamPercentage": 1.2, "damageSelfMitigated": 1.3, "turretPlatesTaken": 1.2, "goldPerMinute": 1.1, "killParticipation": 1.0},
    "JGL": {"kda": 1.3, "killParticipation": 1.2, "riftHeraldTakedowns": 1.3, "teamBaronKills": 1.4, "objectivesStolen": 1.5, "damageDealtToObjectives": 1.2},
    "MID": {"kda": 1.2, "totalDamageDealtToChampions": 1.4, "soloKills_mean": 1.3, "totalMinionsKilled": 1.2, "killParticipation": 1.1, "goldPerMinute": 1.2},
    "BOT": {"kda": 1.3, "totalDamageDealtToChampions": 1.4, "goldPerMinute": 1.3, "killParticipation": 1.1, "totalMinionsKilled": 1.2, "turretKills": 1.1},
    "SUP": {"killParticipation": 1.4, "visionScore": 1.5, "wardsPlaced": 1.3, "totalTimeCCDealt": 1.2, "assists": 1.3, "totalDamageShieldedOnTeammates": 1.2}
}


def getFileValue(param: str, filename: str):
    """Reads the specified parameter's value from the .env file."""
    with open(filename, "r") as file:
        for line in file:
            if line.startswith(f"{param}="):
                return line.strip().split("=", 1)[1]


def logToDB(tournament: str, mode="ONLINE"):
    """return connection to the requested mongo DB"""
    try:
        mongodb_uri = getFileValue("MONGODB_URI", ".env")
        db_name = tournament

        if mode == "ONLINE":
            client = MongoClient(mongodb_uri, server_api=ServerApi('1'))
        else:
            # Parse MONGODB_URI to extract PATH and HOST values
            mongodb_host = mongodb_uri.split(":")[0]
            mongodb_port = int(mongodb_uri.split(":")[1])

            # Manage Database Connection
            client = MongoClient(mongodb_host, mongodb_port)

        # Check if the database exists, if not create it
        if db_name not in client.list_database_names():
            logger.info(f"Creating new database: {db_name}")

        db = client[db_name]

        # Ensure collections exist
        collections = ["players", "matches", "timelines"]
        for collection_name in collections:
            if collection_name not in db.list_collection_names():
                db.create_collection(collection_name)
                logger.info(f"Created collection: {collection_name} in database: {db_name}")

        # Checking connection
        client.server_info()
        logger.info(f"Connection to MongoDB succeeded. Database: {db_name}")

    except Exception as e:
        logger.error(f"Connection failed to MongoDB: {str(e)}")
        raise

    return db


def get_tournament_codes(tournament: str):
    file_path = f"tournaments/{tournament}/tournamentCodes.txt"
    tournament_codes = []
    try:
        with open(file_path, 'r') as file:
            for line in file:
                # Delete spaces and line breaks
                code = line.strip()
                if code:  # Add to list only if line is not empty
                    tournament_codes.append(str(code))
        print("List of tournaments successfully loaded.")
    except FileNotFoundError:
        print(f"The {file_path} file does not exist.")
    except Exception as e:
        print(f"Error reading file : {str(e)}")

    return tournament_codes


def get_team(participants: list, puuid_to_player: dict):
    """return the team given the 5 puuid of the players"""
    # Counting team occurrences in participants
    team_counts = {}
    for puuid in participants:
        if puuid in puuid_to_player:
            team = puuid_to_player[puuid]['team']
            team_counts[team] = team_counts.get(team, 0) + 1

    # Identify the most common team
    max_count = max(team_counts.values(), default=0)
    top_teams = [team for team, count in team_counts.items() if count == max_count]

    # Return the most frequent team or all teams in the event of a tie
    return top_teams[0] if len(top_teams) == 1 else top_teams


def scoring(stats_player, weightings: dict):
    """
    Calcule les scores des joueurs basés sur les statistiques pondérées par rôle.

    :param stats_player: DataFrame contenant les statistiques des joueurs.
    :param weightings: Dictionnaire de pondération des statistiques pour chaque rôle.
    :return: DataFrame avec une colonne 'score' ajoutée.
    """
    # Initialiser le scaler
    scaler = MinMaxScaler()

    # Initialiser une colonne score
    stats_player["score"] = 0

    # Normaliser les scores pour chaque rôle
    for role, stats in weightings.items():
        role_mask = stats_player["position"] == role
        role_df = stats_player.loc[role_mask, list(stats.keys())]

        # Normaliser chaque statistique individuellement
        normalized_stats = scaler.fit_transform(role_df)

        # Calculer le score pondéré
        score = normalized_stats.dot(list(stats.values()))

        # Mettre à jour la colonne score
        stats_player.loc[role_mask, "score"] = score

    # Normaliser le score final sur 100
    stats_player["score"] = scaler.fit_transform(stats_player[["score"]]) * 100

    return stats_player

# TODO add scoring here
