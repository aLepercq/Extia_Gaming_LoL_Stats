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
puuid_to_info = {
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
        player_info = puuid_to_info.get(puuid)

        # If the player is in the list, proceed to update statistics
        if player_info:
            gameName = player_info['gameName']
            team = player_info['team']

            # Get the original position and map it to the abbreviation if it exists
            original_position = participant.get('individualPosition', 'UNKNOWN')
            position = position_map.get(original_position,
                                        original_position)  # Use abbreviation or keep original if not mapped

            kills = participant.get('kills', 0)
            deaths = participant.get('deaths', 0)
            assists = participant.get('assists', 0)
            total_damage = participant.get('totalDamageDealtToChampions', 0)
            win = participant.get('win', False)  # True if the player won, False if not

            # Initialize stats if gameName and position are encountered the first time
            if (gameName, position) not in player_stats:
                player_stats[(gameName, position)] = {
                    'gameName': gameName,
                    'team': team,
                    'position': position,
                    'kills': 0,
                    'deaths': 0,
                    'assists': 0,
                    'matches_played': 0,
                    'totalDamageDealtToChampions': 0,
                    'win_count': 0,  # Count of wins
                    'total_win_duration': 0  # Sum of durations of winning games
                }

            # Update cumulative stats
            player_stats[(gameName, position)]['kills'] += kills
            player_stats[(gameName, position)]['deaths'] += deaths
            player_stats[(gameName, position)]['assists'] += assists
            player_stats[(gameName, position)]['matches_played'] += 1
            player_stats[(gameName, position)]['totalDamageDealtToChampions'] += total_damage

            # Update win stats (for calculating average win duration)
            if win:
                player_stats[(gameName, position)]['win_count'] += 1
                player_stats[(gameName, position)]['total_win_duration'] += game_duration

# Compute KDA, averageKDA, and averageWinDuration and prepare final data for DataFrame
data = []
for stats in player_stats.values():
    kills = stats['kills']
    deaths = stats['deaths']
    assists = stats['assists']
    matches_played = stats['matches_played']
    total_damage = stats['totalDamageDealtToChampions']
    win_count = stats['win_count']
    total_win_duration = stats['total_win_duration']

    # Calculate KDA, handling division by zero if deaths is 0
    kda = (kills + assists) / deaths if deaths > 0 else (kills + assists)

    # Round KDA to 1 decimal place
    kda = round(kda, 1)

    # Calculate average totalDamageDealtToChampions
    avg_damage = total_damage / matches_played if matches_played > 0 else 0
    avg_damage = round(avg_damage, 0)

    # Calculate averageWinDuration (only from winning games)
    average_win_duration = total_win_duration / win_count if win_count > 0 else 0
    average_win_duration = round(average_win_duration, 0)

    # Append statistics for each player-position combination
    data.append({
        'gameName': stats['gameName'],
        'team': stats['team'],
        'position': stats['position'],
        'matches_played': matches_played,
        'averageWinDuration': average_win_duration,
        'kills': kills,
        'deaths': deaths,
        'assists': assists,
        'kda': kda,
        'averageDamageDealt': avg_damage
    })

# Create a DataFrame with the data
df = pd.DataFrame(data)

# Log the number of matches processed
logger.info(f"Processed {match_count} matches.")

# Export the DataFrame to an .xlsx file
output_file = "player_statistics.xlsx"
df.to_excel(output_file, index=False, engine="openpyxl")
logger.info(f"Data exported to {output_file}")
