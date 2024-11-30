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


# apply ranking algorithm
def scoring(row):
    if row['position'] == 'TOP':
        return row['damageDealtToObjectives'] * 0.1 + row['damageSelfMitigated'] * 0.1 + \
            row['damageTakenOnTeamPercentage'] * 0.3 + row['damagePerMinute'] * 0.3 + row['kda'] * 0.1 +\
            row['killParticipation'] * 0.1
    # add CC score, remove KP
    elif row['position'] == 'JGL':
        return row['killParticipation'] * 0.2 + row['damageDealtToObjectives'] * 0.2 + row['kda'] * 0.1 + \
            row['visionScorePerMinute'] * 0.1 + row['damageTakenOnTeamPercentage'] * 0.2 + row['damagePerMinute'] * 0.2
    # remove kda
    elif row['position'] == 'MID':
        return row['damagePerMinute'] * 0.3 + row['kda'] * 0.3 + row['killParticipation'] * 0.3 + \
            row['CSPerMinute'] * 0.1
    # add damageObjectif
    elif row['position'] == 'BOT':
        return row['CSPerMinute'] * 0.2 + row['kda'] * 0.3 + row['damagePerMinute'] * 0.3 + \
            row['killParticipation'] * 0.2
    # replace CS by gold
    elif row['position'] == 'SUP':
        return row['visionScorePerMinute'] * 0.3 + row['killParticipation'] * 0.3 + row['totalHealsOnTeammates'] * 0.2 \
            + row['totalDamageShieldedOnTeammates'] * 0.2
    # add CC score
    else:
        return 0  # default value


# Function for determining the team from the list of puuids
def get_team(participants, puuid_to_player):
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
