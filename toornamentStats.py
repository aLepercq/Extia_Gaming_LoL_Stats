from otherFunctions import *
import pandas as pd
import logging
import tarfile
import json
import re

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

# =========================================================
# PLAYERS STATS
# =========================================================

# Create a dictionary to map each puuid to name and team
puuid_to_player = {
    player['puuid']: {
        'name': player['name'],
        'team': player['team']
    }
    for player in players_collection.find({"puuid": {"$exists": True, "$ne": ""}})
}

# Dictionary for mapping positions to abbreviations
position_map = {
    "TOP": "TOP",
    "JUNGLE": "JGL",
    "MIDDLE": "MID",
    "BOTTOM": "BOT",
    "UTILITY": "SUP"
}

# Dictionary to store player statistics by individualPosition
player_stats = {}

# Iterate over matches in the collection
match_count = 0  # Tracking number of matches processed
for match in matchs_collection.find():
    match_count += 1
    participants = match.get('info', {}).get('participants', [])
    game_duration = match.get('info', {}).get('gameDuration', 0)  # Match duration in seconds

    for participant in participants:
        # Get puuid and check if it is in the player list
        puuid = participant.get('puuid')
        player_info = puuid_to_player.get(puuid)

        # If the player is in the list, proceed to update statistics
        if player_info:
            name = player_info['name']
            team = player_info['team']

            # Get player data
            original_position = participant.get('teamPosition', 'UNKNOWN')
            position = position_map.get(original_position,
                                        original_position)  # Use abbreviation or keep original if not mapped
            # Basic stats
            win = participant.get('win', False)
            kills = participant.get('kills', 0)
            deaths = participant.get('deaths', 0)
            assists = participant.get('assists', 0)
            # Objectives
            damageDealtToBuildings = participant.get('damageDealtToBuildings', 0)
            damageDealtToObjectives = participant.get('damageDealtToObjectives', 0)
            damageDealtToTurrets = participant.get('damageDealtToTurrets', 0)
            # Damage
            totalDamageDealtToChampions = participant.get('totalDamageDealtToChampions', 0)
            teamDamagePercentage = participant.get('challenges', {}).get('teamDamagePercentage', 0)
            damagePerMinute = participant.get('challenges', {}).get('damagePerMinute', 0)
            largestCriticalStrike = participant.get('largestCriticalStrike', 0)
            # Fighting
            killParticipation = participant.get('challenges', {}).get('killParticipation', 0)
            soloKills = participant.get('challenges', {}).get('soloKills', 0)
            timeCCingOthers = participant.get('timeCCingOthers', 0)
            totalHealsOnTeammates = participant.get('totalHealsOnTeammates', 0)
            totalDamageShieldedOnTeammates = participant.get('totalDamageShieldedOnTeammates', 0)
            skillshotsDodged = participant.get('challenges', {}).get('skillshotsDodged', 0)
            skillshotsHit = participant.get('challenges', {}).get('skillshotsHit', 0)
            buffsStolen = participant.get('challenges', {}).get('buffsStolen', 0)
            # Tankiness
            damageSelfMitigated = participant.get('damageSelfMitigated', 0)
            damageTakenOnTeamPercentage = participant.get('challenges', {}).get('damageTakenOnTeamPercentage', 0)
            # Vision
            visionScorePerMinute = participant.get('challenges', {}).get('visionScorePerMinute', 0)
            controlWardTimeCoverage = participant.get('challenges', {}).get('controlWardTimeCoverageInRiverOrEnemyHalf', 0)
            # Gold
            goldPerMinute = participant.get('challenges', {}).get('goldPerMinute', 0)
            # Farming
            neutralMinionsKilled = participant.get('neutralMinionsKilled', 0)
            totalMinionsKilled = participant.get('totalMinionsKilled', 0)
            laneMinionsFirst10Minutes = participant.get('challenges', {}).get('laneMinionsFirst10Minutes', 0)

            # Initialize stats if gameName and position are encountered the first time
            if (name, position) not in player_stats:
                player_stats[(name, position)] = {
                    'name': name,
                    'team': team,
                    'position': position,
                    'matches_played': 0,
                    # Basic stats
                    'winning_games': 0,
                    'totalWinDuration': 0,
                    'kills': 0,
                    'deaths': 0,
                    'assists': 0,
                    # Objectives
                    'damageDealtToObjectives': 0,
                    # Damage
                    'totalDamageDealtToChampions': 0,
                    'teamDamagePercentage': 0,
                    'damagePerMinute': 0,
                    'largestCriticalStrike': 0,
                    # Fighting
                    'killParticipation': 0,
                    'soloKills': 0,
                    'timeCCingOthers': 0,
                    'totalHealsOnTeammates': 0,
                    'totalDamageShieldedOnTeammates': 0,
                    'skillshotsDodged': 0,
                    'skillshotsHit': 0,
                    'buffsStolen': 0,
                    # Tankiness
                    'damageSelfMitigated': 0,
                    'damageTakenOnTeamPercentage': 0,
                    # Vision
                    'visionScorePerMinute': 0,
                    'controlWardTimeCoverage': 0,
                    # Gold
                    'goldPerMinute': 0,
                    # Farming
                    'totalCSPerMinute': 0,
                    'laneMinionsFirst10Minutes': 0
                }

            # Update cumulative stats
            # Basic stats
            player_stats[(name, position)]['matches_played'] += 1
            player_stats[(name, position)]['kills'] += kills
            player_stats[(name, position)]['deaths'] += deaths
            player_stats[(name, position)]['assists'] += assists
            # Objectives
            player_stats[(name, position)]['damageDealtToObjectives'] += damageDealtToBuildings + damageDealtToObjectives + damageDealtToTurrets
            # Damage
            player_stats[(name, position)]['totalDamageDealtToChampions'] += totalDamageDealtToChampions
            player_stats[(name, position)]['teamDamagePercentage'] += teamDamagePercentage
            player_stats[(name, position)]['damagePerMinute'] += damagePerMinute
            player_stats[(name, position)]['largestCriticalStrike'] += largestCriticalStrike
            # Fighting
            player_stats[(name, position)]['killParticipation'] += killParticipation
            player_stats[(name, position)]['soloKills'] += soloKills
            player_stats[(name, position)]['timeCCingOthers'] += timeCCingOthers
            player_stats[(name, position)]['totalHealsOnTeammates'] += totalHealsOnTeammates
            player_stats[(name, position)]['totalDamageShieldedOnTeammates'] += totalDamageShieldedOnTeammates
            player_stats[(name, position)]['skillshotsDodged'] += skillshotsDodged
            player_stats[(name, position)]['skillshotsHit'] += skillshotsHit
            player_stats[(name, position)]['buffsStolen'] += buffsStolen
            # Tankiness
            player_stats[(name, position)]['damageSelfMitigated'] += damageSelfMitigated
            player_stats[(name, position)]['damageTakenOnTeamPercentage'] += damageTakenOnTeamPercentage
            # Vision
            player_stats[(name, position)]['visionScorePerMinute'] += visionScorePerMinute
            player_stats[(name, position)]['controlWardTimeCoverage'] += controlWardTimeCoverage
            # Gold
            player_stats[(name, position)]['goldPerMinute'] += goldPerMinute
            # Farming
            player_stats[(name, position)]['totalCSPerMinute'] += (
                                                totalMinionsKilled + neutralMinionsKilled) / round(game_duration/60, 0)
            player_stats[(name, position)]['laneMinionsFirst10Minutes'] += laneMinionsFirst10Minutes

            # Update win-related statistics
            if win:
                player_stats[(name, position)]['winning_games'] += 1
                player_stats[(name, position)]['totalWinDuration'] += round(game_duration/60, 0)

# Compute averageKPI and prepare final data for DataFrame
data = []
for stats in player_stats.values():
    # Basic stats
    matches_played = stats['matches_played']
    winning_games = stats['winning_games']
    totalWinDuration = stats['totalWinDuration']
    kills = stats['kills']
    deaths = stats['deaths']
    assists = stats['assists']
    # Objectives
    damageDealtToObjectives = stats['damageDealtToObjectives']
    # Damage
    totalDamageDealtToChampions = stats['totalDamageDealtToChampions']
    teamDamagePercentage = stats['teamDamagePercentage']
    damagePerMinute = stats['damagePerMinute']
    largestCriticalStrike = stats['largestCriticalStrike']
    # Fighting
    killParticipation = stats['killParticipation']
    soloKills = stats['soloKills']
    timeCCingOthers = stats['timeCCingOthers']
    totalHealsOnTeammates = stats['totalHealsOnTeammates']
    totalDamageShieldedOnTeammates = stats['totalDamageShieldedOnTeammates']
    skillshotsDodged = stats['skillshotsDodged']
    skillshotsHit = stats['skillshotsHit']
    totalBuffsStolen = stats['buffsStolen']
    # Tankiness
    damageSelfMitigated = stats['damageSelfMitigated']
    damageTakenOnTeamPercentage = stats['damageTakenOnTeamPercentage']
    # Vision
    visionScorePerMinute = stats['visionScorePerMinute']
    controlWardTimeCoverage = stats['controlWardTimeCoverage']
    # Gold
    goldPerMinute = stats['goldPerMinute']
    # Farming
    totalCSPerMinute = stats['totalCSPerMinute']
    laneMinionsFirst10Minutes = stats['laneMinionsFirst10Minutes']

    # Calculate averages
    winrate = round(winning_games / matches_played, 2)
    kda = (kills + assists) / deaths if deaths > 0 else (kills + assists)
    kda = round(kda, 1)
    damageDealtToObjectives = damageDealtToObjectives / matches_played if matches_played > 0 else 0
    totalDamageDealtToChampions = totalDamageDealtToChampions / matches_played if matches_played > 0 else 0
    teamDamagePercentage = teamDamagePercentage / matches_played if matches_played > 0 else 0
    damagePerMinute = damagePerMinute / matches_played if matches_played > 0 else 0
    largestCriticalStrike = largestCriticalStrike / matches_played if matches_played > 0 else 0
    killParticipation = killParticipation / matches_played if matches_played > 0 else 0
    soloKills = soloKills / matches_played if matches_played > 0 else 0
    timeCCingOthers = timeCCingOthers / matches_played if matches_played > 0 else 0
    totalHealsOnTeammates = totalHealsOnTeammates / matches_played if matches_played > 0 else 0
    totalDamageShieldedOnTeammates = totalDamageShieldedOnTeammates / matches_played if matches_played > 0 else 0
    skillshotsDodged = skillshotsDodged / matches_played if matches_played > 0 else 0
    skillshotsHit = skillshotsHit / matches_played if matches_played > 0 else 0
    totalBuffsStolen = totalBuffsStolen / matches_played if matches_played > 0 else 0
    damageSelfMitigated = damageSelfMitigated / matches_played if matches_played > 0 else 0
    damageTakenOnTeamPercentage = damageTakenOnTeamPercentage / matches_played if matches_played > 0 else 0
    visionScorePerMinute = visionScorePerMinute / matches_played if matches_played > 0 else 0
    controlWardTimeCoverage = controlWardTimeCoverage / matches_played if matches_played > 0 else 0
    goldPerMinute = goldPerMinute / matches_played if matches_played > 0 else 0
    totalCSPerMinute = totalCSPerMinute / matches_played if matches_played > 0 else 0
    laneMinionsFirst10Minutes = laneMinionsFirst10Minutes / matches_played if matches_played > 0 else 0

    # Append statistics for each player-position combination
    data.append({
        'name': stats['name'],
        'team': stats['team'],
        'position': stats['position'],
        'winrate': winrate,
        'matches_played': matches_played,
        'kills': kills,
        'deaths': deaths,
        'assists': assists,
        'kda': kda,
        'damageDealtToObjectives': round(damageDealtToObjectives, 0),
        'damageDealtToChampions': round(totalDamageDealtToChampions, 0),
        'teamDamagePercentage': round(teamDamagePercentage, 3),
        'damagePerMinute': round(damagePerMinute, 0),
        'largestCriticalStrike': round(largestCriticalStrike, 0),
        'killParticipation': round(killParticipation, 3),
        'soloKills': round(soloKills, 2),
        'timeCCingOthers': round(timeCCingOthers, 2),
        'totalHealsOnTeammates': round(totalHealsOnTeammates, 0),
        'totalDamageShieldedOnTeammates': round(totalDamageShieldedOnTeammates, 0),
        'skillshotsDodged': round(skillshotsDodged, 1),
        'skillshotsHit': round(skillshotsHit, 1),
        'buffsStolen': round(totalBuffsStolen, 2),
        'damageSelfMitigated': round(damageSelfMitigated, 0),
        'damageTakenOnTeamPercentage': round(damageTakenOnTeamPercentage, 3),
        'visionScorePerMinute': round(visionScorePerMinute, 2),
        'controlWardTimeCoverage': round(controlWardTimeCoverage, 2),
        'goldPerMinute': round(goldPerMinute, 0),
        'damagePerGold': round(damagePerMinute / goldPerMinute, 2),
        'CSPerMinute': round(totalCSPerMinute, 2),
        'laneMinionsFirst10Minutes': round(laneMinionsFirst10Minutes, 1)
    })

# Create a DataFrame with the data
df = pd.DataFrame(data)

# Log the number of matches processed
logger.info(f"Processed {match_count} matches.")

normalized_df = df.copy()

# apply normalization techniques
for column in list(filter(lambda x: x not in ["name", "team", "position"], normalized_df.columns)):
    normalized_df[column] = (normalized_df[column] - normalized_df[column].mean()) / normalized_df[column].std()

# Normalizing dataframe
normalized_df['score'] = round(normalized_df.apply(scoring, axis=1) * normalized_df['winrate'], 3)
# identifaction of main position
normalized_df['main'] = normalized_df.groupby('name')['matches_played'].transform(lambda x: x == x.max())
# We keep merging keys and fields we want on the left
df = pd.merge(normalized_df[['name', 'team', 'position', 'main', 'score']], df,
              on=['name', 'team', 'position'], how='left')

# Export the DataFrame to an .xlsx file
output_file = "players_statistics.xlsx"
df.to_excel(output_file, index=False, engine="openpyxl")
logger.info(f"Data exported to {output_file}")

# =========================================================
# CHAMPION STATS
# =========================================================

# Getting the round number for each match_id
matches = list(matchs_collection.find({}, {"match_id": 1, "info.gameCreation": 1, "metadata.participants": 1}))
df = pd.DataFrame([
    {
        "match_id": match["match_id"],
        "participants1": match["metadata"]["participants"][:4],
        "participants2": match["metadata"]["participants"][5:],
        "gameCreation": match["info"]["gameCreation"]
    }
    for match in matches
])

# Add the team1 and team2 fields to the DataFrame
df['team1'] = df['participants1'].apply(lambda x: get_team(x, puuid_to_player))
df['team2'] = df['participants2'].apply(lambda x: get_team(x, puuid_to_player))
df['gameCreation_dt'] = pd.to_datetime(df['gameCreation'], unit='ms')  # Convert gameCreation to datetime format
df['date'] = df['gameCreation_dt'].dt.strftime('%Y-%m-%d')  # Create the ‘date’ field in YYYY-MM-DD format
df['time'] = df['gameCreation_dt'].dt.strftime('%H:%M:%S')  # Create the ‘time’ field in HH:MM:SS format
df.drop(columns=['participants1', 'participants2', 'gameCreation'], inplace=True)
df['versus'] = df.apply(
    lambda row: f"{row['team1']} vs {row['team2']}" if row['team1'] < row['team2'] else f"{row['team2']} vs {row['team1']}",
    axis=1
)
df = df.sort_values(by=['versus', 'date', 'time']).reset_index(drop=True)
df['round'] = df.groupby(['versus']).cumcount() + 1  # identify the game number
df = df[['match_id', 'round']]

# Initialise counters for champions statistics
champion_stats = {}
for match in matchs_collection.find():
    participants = match.get('info', {}).get('participants', [])
    teams = match.get('info', {}).get('teams', [])
    match_id = match.get('metadata', {}).get('matchId', "")
    # Processing picks and results
    for participant in participants:
        champion_id = participant['championId']
        win = participant['win']

        if champion_id not in champion_stats:
            champion_stats[champion_id] = {
                'picks': 0,
                'wins': 0,
                'bans': 0,
                'games': match_count  # Total games played (used for rates)
            }

        champion_stats[champion_id]['picks'] += 1
        champion_stats[champion_id]['wins'] += int(win)
    if df.query('match_id == @match_id')['round'].iloc[0] == 1:
        # Processing banns
        for team in teams:
            bans = team.get('bans', [])
            for ban in bans:
                champion_id = ban['championId']
                if champion_id not in champion_stats:
                    champion_stats[champion_id] = {
                        'picks': 0,
                        'wins': 0,
                        'bans': 0,
                        'games': match_count
                    }
                champion_stats[champion_id]['bans'] += 1
    elif df.query('match_id == @match_id')['round'].iloc[0] == 2:
        # Processing banns of round 2
        for team in teams:
            bans = team.get('bans', [])
            for ban in bans[:3]:
                champion_id = ban['championId']
                if champion_id not in champion_stats:
                    champion_stats[champion_id] = {
                        'picks': 0,
                        'wins': 0,
                        'bans': 0,
                        'games': match_count
                    }
                champion_stats[champion_id]['bans'] += 1

# Calculate the statistics for each champion
data = []
for champion_id, stats in champion_stats.items():
    games = stats['games']
    picks = stats['picks']
    bans = stats['bans']
    wins = stats['wins']

    pickrate = picks / games if games > 0 else 0
    winrate = wins / picks if picks > 0 else 0
    banrate = bans / games if games > 0 else 0
    presence = (picks + bans) / games if games > 0 else 0

    data.append({
        'championId': champion_id,
        'pickcount': picks,
        'pickrate': round(pickrate, 3),
        'winrate': round(winrate, 3),
        'banrate': round(banrate, 3),
        'presence': round(presence, 3)
    })


# Sort by attendance rate for easier reading
df = pd.DataFrame(data).sort_values(by='presence', ascending=False)

# Association between championId and Name
archive_path = get_env_value("ARCHIVE_PATH")
patch_name = re.search(r"-(.+)\.tgz", archive_path).group(1)  # get patch
file_to_extract = patch_name+"/data/en_US/champion.json"
with tarfile.open(archive_path, "r:gz") as tar:
    # Rechercher le fichier "test" dans l'archive
    json_file = tar.extractfile(file_to_extract)

    if json_file:
        # Charger le contenu JSON
        data = json.load(json_file)
        # Extraire les associations name -> id
        id_to_name = {str(champion['key']): str(champion['name']) for champion in data['data'].values()}
    else:
        print(f"Fichier '{file_to_extract}' non trouvé dans l'archive.")

id_to_name = {int(key): value for key, value in id_to_name.items()}

df['champion'] = df['championId'].map(id_to_name)
# Export the DataFrame to an .xlsx file
output_file = "champions_statistics.xlsx"
df[['champion', 'pickcount', 'pickrate', 'winrate', 'banrate', 'presence']].to_excel(output_file,
                                                                                     index=False, engine="openpyxl")
logger.info(f"Data exported to {output_file}")
