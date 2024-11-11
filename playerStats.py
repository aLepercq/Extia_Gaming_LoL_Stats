from pymongo import MongoClient
import pandas as pd
import logging

# Logging config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connection to DB
try:
    # Manage Database Connection
    client = MongoClient('localhost', 27017)
    db = client['Extia_Gaming_LoL_2024']
    players_collection = db['players']
    matchs_collection = db['matchs']

    # Vérifier la connexion
    client.server_info()
    logger.info("Connection to MongoDB succeeded")

except Exception as e:
    logger.error(f"Connection failed to MongoDB: {str(e)}")
    raise

# Create a dictionary to map each puuid to gameName and team
puuid_to_player = {
    player['puuid']: {
        'gameName': player['gameName'],
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
match_count = 0  # Keep track of number of matches processed
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
            gameName = player_info['gameName']
            team = player_info['team']

            # Get player data
            original_position = participant.get('individualPosition', 'UNKNOWN')
            position = position_map.get(original_position,
                                        original_position)  # Use abbreviation or keep original if not mapped
            win = participant.get('win', False)
            kills = participant.get('kills', 0)
            deaths = participant.get('deaths', 0)
            assists = participant.get('assists', 0)
            total_damage = participant.get('totalDamageDealtToChampions', 0)
            goldPerMinute = participant.get('challenges', {}).get('goldPerMinute', 0)
            killParticipation = participant.get('challenges', {}).get('killParticipation', 0)
            laneMinionsFirst10Minutes = participant.get('challenges', {}).get('laneMinionsFirst10Minutes', 0)
            visionScorePerMinute = participant.get('challenges', {}).get('visionScorePerMinute', 0)
            soloKills = participant.get('challenges', {}).get('soloKills', 0)
            neutralMinionsKilled = participant.get('neutralMinionsKilled', 0)
            totalMinionsKilled = participant.get('totalMinionsKilled', 0)

            # Initialize stats if gameName and position are encountered the first time
            if (gameName, position) not in player_stats:
                player_stats[(gameName, position)] = {
                    'gameName': gameName,
                    'team': team,
                    'position': position,
                    'matches_played': 0,
                    'kills': 0,
                    'deaths': 0,
                    'assists': 0,
                    'totalDamageDealtToChampions': 0,
                    'totalGoldPerMinute': 0,
                    'totalKillParticipation': 0,
                    'totalLaneMinionsFirst10Minutes': 0,
                    'totalVisionScorePerMinute': 0,
                    'winning_games': 0,
                    'totalWinDuration': 0,
                    'totalSoloKills': 0,
                    'totalCSPerMinute': 0
                }

            # Update cumulative stats
            player_stats[(gameName, position)]['kills'] += kills
            player_stats[(gameName, position)]['deaths'] += deaths
            player_stats[(gameName, position)]['assists'] += assists
            player_stats[(gameName, position)]['matches_played'] += 1
            player_stats[(gameName, position)]['totalDamageDealtToChampions'] += total_damage
            player_stats[(gameName, position)]['totalGoldPerMinute'] += goldPerMinute
            player_stats[(gameName, position)]['totalKillParticipation'] += killParticipation
            player_stats[(gameName, position)]['totalLaneMinionsFirst10Minutes'] += laneMinionsFirst10Minutes
            player_stats[(gameName, position)]['totalVisionScorePerMinute'] += visionScorePerMinute
            player_stats[(gameName, position)]['totalSoloKills'] += soloKills
            player_stats[(gameName, position)]['totalCSPerMinute'] += (
                                                totalMinionsKilled + neutralMinionsKilled) / round(game_duration/60, 0)

            # Update win-related statistics
            if win:
                player_stats[(gameName, position)]['winning_games'] += 1
                player_stats[(gameName, position)]['totalWinDuration'] += round(game_duration/60, 0)

# Compute averageKPI and prepare final data for DataFrame
data = []
for stats in player_stats.values():
    kills = stats['kills']
    deaths = stats['deaths']
    assists = stats['assists']
    matches_played = stats['matches_played']
    totalDamageDealtToChampions = stats['totalDamageDealtToChampions']
    totalGoldPerMinute = stats['totalGoldPerMinute']
    totalKillParticipation = stats['totalKillParticipation']
    totalLaneMinionsFirst10Minutes = stats['totalLaneMinionsFirst10Minutes']
    totalVisionScorePerMinute = stats['totalVisionScorePerMinute']
    winning_games = stats['winning_games']
    totalWinDuration = stats['totalWinDuration']
    totalSoloKills = stats['totalSoloKills']
    totalCSPerMinute = stats['totalCSPerMinute']

    # Calculate averages
    winrate = round(winning_games / matches_played, 2)
    kda = (kills + assists) / deaths if deaths > 0 else (kills + assists)
    kda = round(kda, 1)
    avgDamageDealtToChampions = totalDamageDealtToChampions / matches_played if matches_played > 0 else 0
    avgGoldPerMinute = totalGoldPerMinute / matches_played if matches_played > 0 else 0
    avgKillParticipation = totalKillParticipation / matches_played if matches_played > 0 else 0
    avgLaneMinionsFirst10Minutes = totalLaneMinionsFirst10Minutes / matches_played if matches_played > 0 else 0
    avgVisionScorePerMinute = totalVisionScorePerMinute / matches_played if matches_played > 0 else 0
    avgWinDuration = totalWinDuration / winning_games if winning_games > 0 else 0
    avgSoloKills = totalSoloKills / matches_played if matches_played > 0 else 0
    avgCSPerMinute = totalCSPerMinute / matches_played if matches_played > 0 else 0

    # Append statistics for each player-position combination
    data.append({
        'gameName': stats['gameName'],
        'team': stats['team'],
        'position': stats['position'],
        'winrate': winrate,
        'matches_played': matches_played,
        'kills': kills,
        'deaths': deaths,
        'assists': assists,
        'kda': kda,
        'avgDamageDealt': round(avgDamageDealtToChampions, 0),
        'avgGoldPerMinute': round(avgGoldPerMinute, 0),
        'avgKillParticipation': round(avgKillParticipation, 2),
        'avgCS@10': round(avgLaneMinionsFirst10Minutes, 0),
        'avgVisionScorePerMinute': round(avgVisionScorePerMinute, 2),
        'avgWinDuration': round(avgWinDuration, 2),
        'avgSoloKills': round(avgSoloKills, 2),
        'avgCSPerMinute': round(avgCSPerMinute, 2)
    })

# Create a DataFrame with the data
df = pd.DataFrame(data)

# Log the number of matches processed
logger.info(f"Processed {match_count} matches.")

# Export the DataFrame to an .xlsx file
output_file = "player_statistics.xlsx"
df.to_excel(output_file, index=False, engine="openpyxl")
logger.info(f"Data exported to {output_file}")
