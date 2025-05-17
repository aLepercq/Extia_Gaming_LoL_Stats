from utils.common.commonFunctions import *


def generate_player_match_stats(tournament: str):
    """generate the stats per match/player to allow fast statistics generation"""
    logger.info("updating stats_players table...")

    db = logToDB(tournament)
    players = db['players']
    matches = db['matches']
    timelines = db['timelines']
    stats_players = db['stats_players']

    # Récupérer les données de match
    match_data = matches.find()

    for match in match_data:
        match_id = match["match_id"]
        participants = match["info"]["participants"]

        timeline = timelines.find_one({"match_id": match_id})
        frames = timeline.get("info", {}).get("frames", [])

        # Vérifier si la 15e frame existe
        if len(frames) > 15:
            frame_15 = frames[16].get("participantFrames", {})

        else:
            frame_15 = {}  # Valeur par défaut si la frame n'existe pas

        # mapping participantId -> puuid
        participant_id_to_puuid = {
            participant["participantId"]: participant["puuid"]
            for participant in match["info"]["participants"]
        }

        # Basic stats
        for participant in participants:
            name = players.find_one({"puuid": participant["puuid"]})["name"]
            participant_id = next(
                (p_id for p_id, puuid in participant_id_to_puuid.items() if puuid == participant["puuid"]), None
            )

            # getting stats at 15min
            if participant_id:
                frame_data = frame_15.get(str(participant_id), {})
                cs_15 = frame_data.get("minionsKilled", 0) + frame_data.get("jungleMinionsKilled", 0)
                gold_15 = frame_data.get("totalGold", 0)
                xp_15 = frame_data.get("xp", 0)
            else:
                cs_15, gold_15, xp_15 = 0, 0, 0

            player_stats = {
                "match_id": match_id,
                "versus": match["versus"],
                "round": match["round"],
                "name": name,
                "team": match["blue"] if participants.index(participant) < 5 else match["red"],
                "side": "blue" if participants.index(participant) < 5 else "red",
                "teamPosition": participant["teamPosition"],
                "championId": participant["championId"],
                "championName": participant["championName"],
                "kills": participant["kills"],
                "deaths": participant["deaths"],
                "assists": participant["assists"],
                "damageDealtToBuildings": participant["damageDealtToBuildings"],
                "damageDealtToObjectives": participant["damageDealtToObjectives"],
                "damageDealtToTurrets": participant["damageDealtToTurrets"],
                "gameDuration": match["info"]["gameDuration"],
                "damagePerMinute": participant["challenges"].get("damagePerMinute", 0),
                "damageTakenOnTeamPercentage": participant["challenges"].get("damageTakenOnTeamPercentage", 0),
                "firstTurretKilled": participant["challenges"].get("firstTurretKilled", 0),
                "goldPerMinute": participant["challenges"].get("goldPerMinute", 0),
                "kda": participant["challenges"].get("kda", 0),
                "killParticipation": participant["challenges"].get("killParticipation", 0),
                "laneMinionsFirst10Minutes": participant["challenges"].get("laneMinionsFirst10Minutes", 0),
                "riftHeraldTakedowns": participant["challenges"].get("riftHeraldTakedowns", 0),
                "soloKills": participant["challenges"].get("soloKills", 0),
                "stealthWardsPlaced": participant["challenges"].get("stealthWardsPlaced", 0),
                "survivedSingleDigitHpCount": participant["challenges"].get("survivedSingleDigitHpCount", 0),
                "teamBaronKills": participant["challenges"].get("teamBaronKills", 0),
                "teamDamagePercentage": participant["challenges"].get("teamDamagePercentage", 0),
                "teamRiftHeraldKills": participant["challenges"].get("teamRiftHeraldKills", 0),
                "turretPlatesTaken": participant["challenges"].get("turretPlatesTaken ", 0),
                "voidMonsterKill": participant["challenges"].get("voidMonsterKill", 0),
                "wardTakedowns": participant["challenges"].get("wardTakedowns", 0),
                "damageSelfMitigated": participant["damageSelfMitigated"],
                "firstBloodKill": participant["firstBloodKill"],
                "largestCriticalStrike": participant["largestCriticalStrike"],
                "largestMultiKill": participant["largestMultiKill"],
                "magicDamageDealt": participant["magicDamageDealt"],
                "magicDamageDealtToChampions": participant["magicDamageDealtToChampions"],
                "magicDamageTaken": participant["magicDamageTaken"],
                "objectivesStolen": participant["objectivesStolen"],
                "pentaKills": participant["pentaKills"],
                "physicalDamageDealt": participant["physicalDamageDealt"],
                "physicalDamageDealtToChampions": participant["physicalDamageDealtToChampions"],
                "physicalDamageTaken": participant["physicalDamageTaken"],
                "timeCCingOthers": participant["timeCCingOthers"],
                "totalDamageDealt": participant["totalDamageDealt"],
                "totalDamageDealtToChampions": participant["totalDamageDealtToChampions"],
                "totalDamageShieldedOnTeammates": participant["totalDamageShieldedOnTeammates"],
                "totalHeal": participant["totalHeal"],
                "totalHealsOnTeammates": participant["totalHealsOnTeammates"],
                "totalMinionsKilled": participant["totalMinionsKilled"],
                "totalTimeCCDealt": participant["totalTimeCCDealt"],
                "totalTimeSpentDead": participant["totalTimeSpentDead"],
                "trueDamageDealt": participant["trueDamageDealt"],
                "trueDamageDealtToChampions": participant["trueDamageDealtToChampions"],
                "trueDamageTaken": participant["trueDamageTaken"],
                "turretKills": participant["turretKills"],
                "turretsLost": participant["turretsLost"],
                "visionScore": participant["visionScore"],
                "visionWardsBoughtInGame": participant["visionWardsBoughtInGame"],
                "wardsKilled": participant["wardsKilled"],
                "wardsPlaced": participant["wardsPlaced"],
                "pinksPlaced": participant["detectorWardsPlaced"],
                "win": participant["win"],
                "cs_15": cs_15,
                "gold_15": gold_15,
                "xp_15": xp_15
            }

            # Mettre à jour ou insérer les statistiques du joueur
            stats_players.update_one(
                {"match_id": match_id, "name": name},
                {"$set": player_stats},
                upsert=True
            )
