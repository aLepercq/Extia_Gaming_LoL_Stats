from utils.common.commonFunctions import *
from utils.common.riotApi import *
from pymongo import UpdateOne
from datetime import datetime
import pandas as pd


def update_puuid(players_collection):
    """check for players missing puuid and add it"""
    api_key = getFileValue("API_KEY", ".env")
    puuid_url = getFileValue("PUUID_URL", ".env")

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
            update_player['puuid'] = getPuuid(puuid_url, api_key, update_player)
        except Exception as e:
            logger.error(f"Error when retrieving puuid: {str(e)}")
            continue

        # Update insertion method
        try:
            # Update the existing document
            players_collection.update_one(
                {"_id": player_id},
                {
                    "$set": update_player
                }
            )

        except Exception as e:
            logger.error(f"Fail when updating data: {str(e)}")


def update_context(db):
    puuid_to_playerdata = {p["puuid"]: p for p in db['players'].find()}
    matches = list(db['matches'].find({}, {"match_id": 1, "info.gameCreation": 1, "metadata.participants": 1}))
    df = pd.DataFrame([
        {
            "match_id": match["match_id"],
            "participants1": match["metadata"]["participants"][:4],
            "participants2": match["metadata"]["participants"][5:],
            "gamedate": match["info"]["gameCreation"]
        }
        for match in matches
    ])
    df['team1'] = df['participants1'].apply(lambda x: get_team(x, puuid_to_playerdata))
    df['team2'] = df['participants2'].apply(lambda x: get_team(x, puuid_to_playerdata))
    df['gameCreation_dt'] = pd.to_datetime(df['gamedate'], unit='ms')  # Convert gameCreation to datetime format
    df['date'] = df['gameCreation_dt'].dt.strftime('%Y-%m-%d')  # Create the ‘date’ field in YYYY-MM-DD format
    df['time'] = df['gameCreation_dt'].dt.strftime('%H:%M:%S')  # Create the ‘time’ field in HH:MM:SS format
    df['game_dt'] = df['gameCreation_dt'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['versus'] = df.apply(
        lambda row:
        f"{row['team1']} vs {row['team2']}" if row['team1'] < row['team2'] else f"{row['team2']} vs {row['team1']}",
        axis=1)
    df = df.sort_values(by=['versus', 'date', 'time']).reset_index(drop=True)
    df['round'] = df.groupby(['versus']).cumcount() + 1  # identify the game number
    df['blue'] = df['team1']
    df['red'] = df['team2']
    df.drop(columns=['participants1', 'participants2', 'gameCreation_dt', 'team1', 'team2'], inplace=True)

    for match in db['matches'].find():
        match_id = match["metadata"]["matchId"]
        round_number = match.get("round", None)

        context = {"game_dt": str(df.query('match_id == @match_id')['game_dt'].iloc[0]),
                   "versus": str(df.query('match_id == @match_id')['versus'].iloc[0]),
                   "round": int(df.query('match_id == @match_id')['round'].iloc[0]),
                   "blue": str(df.query('match_id == @match_id')['blue'].iloc[0]),
                   "red": str(df.query('match_id == @match_id')['red'].iloc[0])
                   }

        if round_number is None:
            db['matches'].update_one(
                {"match_id": match_id},
                {
                    "$set": context
                }
            )
            db['timelines'].update_one(
                {"match_id": match_id},
                {
                    "$set": context
                }
            )


def update_players(tournament: str):
    logger.info("Players Update...")

    # Update players basic info from file
    csv_file_path = f"tournaments/{tournament}/players.csv"
    try:
        players_df = pd.read_csv(csv_file_path, encoding='utf-8', header=0, sep=';')
    except FileNotFoundError:
        logger.error(f"file not found: {csv_file_path}")
        return

    db = logToDB(tournament)
    players_collection = db['players']

    updates = []
    for index, row in players_df.iterrows():
        gamename = row['gameName']
        tagline = row['tagLine']
        team = row['team']
        name = row['name']

        update_operation = UpdateOne(
            {"gameName": gamename, "tagLine": tagline},
            {"$set": {"team": team, "name": name}},
            upsert=True
        )
        updates.append(update_operation)

    if updates:
        result = players_collection.bulk_write(updates)
        logger.info(f"{result.matched_count} players updated, {result.upserted_count} inserted.")
    else:
        logger.info("No update")

    update_puuid(players_collection)

    # Update players from matches
    # TODO: when a puuid exist in matches and not in players
    # TODO : leading 0 in players.csv


def update_matches(tournament: str):
    logger.info("Update matches...")

    api_key = getFileValue("API_KEY", ".env")
    matchslist_url = getFileValue("MATCHSLIST_URL", ".env")
    matchdata_url = getFileValue("MATCHDATA_URL", ".env")
    matchtimeline_url = getFileValue("MATCHTIMELINE_URL", ".env")
    start_timestamp = getFileValue("START_TIMESTAMP", f"tournaments/{tournament}/.config")
    end_timestamp = getFileValue("END_TIMESTAMP", f"tournaments/{tournament}/.config")

    db = logToDB(tournament)
    players_collection = db['players']
    matches_collection = db['matches']
    timelines_collection = db['timelines']

    tournament_codes = get_tournament_codes(tournament)

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
            match_ids = getMatchlist(matchslist_url, api_key, player['puuid'], start_timestamp, end_timestamp)
            for match_id in match_ids:
                if matches_collection.count_documents({"match_id": match_id}) == 0:
                    print(f"processing {match_id}")
                    try:
                        match_data = getMatchData(matchdata_url, api_key, match_id)
                    except Exception as e:
                        logger.error(f"Could not get matchdata: {str(e)}")
                        break
                    try:
                        match_data['match_id'] = match_id
                        match_data['created_at'] = datetime.utcnow()
                        game_status = match_data['info']['endOfGameResult']
                        if match_data['info']['tournamentCode'] in tournament_codes and game_status == "GameComplete":
                            matches_collection.insert_one(match_data)
                    except Exception as e:
                        logger.error(f"Match not inserted: {str(e)}")

    except Exception as e:
        logger.error(f"General Fail: {str(e)}")

    # Update timeline data
    try:
        time.sleep(0.5)
        # Get all match_id of timelines
        existing_timeline_ids = {
            timeline['match_id'] for timeline in
            timelines_collection.find({"match_id": {"$exists": True, "$ne": ""}}, {"match_id": 1})}

        # getting matchs id in matchs collection that are not yet in timelines
        matchs_without_timeline = matches_collection.find({
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
                timeline = getMatchTimeLine(matchtimeline_url, api_key, match_id)

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

    update_context(db)
