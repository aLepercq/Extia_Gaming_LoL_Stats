from utils.common.commonFunctions import *
import pandas as pd
import numpy as np

def normalize_minmax(series):
    """Normalise une série entre 0 et 100 (Min-Max Scaling)."""
    return ((series - series.min()) / (series.max() - series.min())) * 100

def normalize_inverse(series):
    """Normalise une série inverse (moins = mieux) entre 0 et 100."""
    return ((series.max() - series) / (series.max() - series.min())) * 100

def generate_players_stats(tournament: str):
    """Génère un fichier Excel avec les statistiques des joueurs, incluant une normalisation des KPIs."""
    logger.info("Generating players statistics...")
    db = logToDB(tournament)
    players = db["players"]
    stats_players = db["stats_players"]
    df = pd.DataFrame(list(stats_players.find()))
    df_players = pd.DataFrame(list(players.find()))

    if df.empty:
        print("No data found.")
        exit()

    # Déterminer la position principale de chaque joueur
    main_positions = df.groupby(["name", "teamPosition"]).size().reset_index(name="count")
    main_positions = main_positions.sort_values(["name", "count"], ascending=[True, False])
    main_positions = main_positions.drop_duplicates(subset=["name"], keep="first")
    df = df.merge(main_positions[["name", "teamPosition"]], on="name", suffixes=("", "_main"))
    df["main"] = df["teamPosition"] == df["teamPosition_main"]

    # Calcul des différences (matchup stats)
    df["cs_15_diff"] = df.groupby(["match_id", "teamPosition"])["cs_15"].transform(lambda x: 2 * (x - x.mean()))
    df["gold_15_diff"] = df.groupby(["match_id", "teamPosition"])["gold_15"].transform(lambda x: 2 * (x - x.mean()))
    df["xp_15_diff"] = df.groupby(["match_id", "teamPosition"])["xp_15"].transform(lambda x: 2 * (x - x.mean()))

    # Agrégation des statistiques par joueur
    player_stats = df.groupby(["name", "teamPosition"]).agg(
        matches_played=("match_id", "count"),
        win_count=("win", "sum"),
        main=("main", "max"),
        kills=("kills", "mean"),
        deaths=("deaths", "mean"),
        assists=("assists", "mean"),
        kda=("kda", "mean"),
        damageDealtToBuildings=("damageDealtToBuildings", "mean"),
        damageDealtToObjectives=("damageDealtToObjectives", "mean"),
        damageDealtToTurrets=("damageDealtToTurrets", "mean"),
        gameDuration=("gameDuration", "mean"),
        damagePerMinute=("damagePerMinute", "mean"),
        damageTakenOnTeamPercentage=("damageTakenOnTeamPercentage", "mean"),
        firstTurretKilled=("firstTurretKilled", "count"),
        goldPerMinute=("goldPerMinute", "mean"),
        killParticipation=("killParticipation", "mean"),
        laneMinionsFirst10Minutes=("laneMinionsFirst10Minutes", "mean"),
        riftHeraldTakedowns=("riftHeraldTakedowns", "sum"),
        soloKills_mean=("soloKills", "mean"),
        soloKills_total=("soloKills", "sum"),
        stealthWardsPlaced=("stealthWardsPlaced", "mean"),
        survivedSingleDigitHpCount=("survivedSingleDigitHpCount", "sum"),
        teamBaronKills=("teamBaronKills", "mean"),
        teamDamagePercentage=("teamDamagePercentage", "mean"),
        teamRiftHeraldKills=("teamRiftHeraldKills", "mean"),
        turretPlatesTaken=("turretPlatesTaken", "mean"),
        voidMonsterKill=("voidMonsterKill", "mean"),
        wardTakedowns=("wardTakedowns", "mean"),
        damageSelfMitigated=("damageSelfMitigated", "mean"),
        firstBloodKill=("firstBloodKill", "sum"),
        largestCriticalStrike=("largestCriticalStrike", "max"),
        largestMultiKill=("largestMultiKill", "max"),
        magicDamageDealt=("magicDamageDealt", "mean"),
        magicDamageDealtToChampions=("magicDamageDealtToChampions", "mean"),
        magicDamageTaken=("magicDamageTaken", "mean"),
        objectivesStolen=("objectivesStolen", "sum"),
        pentaKills=("pentaKills", "sum"),
        physicalDamageDealt=("physicalDamageDealt", "mean"),
        physicalDamageDealtToChampions=("physicalDamageDealtToChampions", "mean"),
        physicalDamageTaken=("physicalDamageTaken", "mean"),
        timeCCingOthers=("timeCCingOthers", "mean"),
        totalDamageDealt=("totalDamageDealt", "mean"),
        totalDamageDealtToChampions=("totalDamageDealtToChampions", "mean"),
        totalDamageShieldedOnTeammates=("totalDamageShieldedOnTeammates", "mean"),
        totalHeal=("totalHeal", "mean"),
        totalHealsOnTeammates=("totalHealsOnTeammates", "mean"),
        totalMinionsKilled=("totalMinionsKilled", "mean"),
        totalTimeCCDealt=("totalTimeCCDealt", "mean"),
        totalTimeSpentDead=("totalTimeSpentDead", "mean"),
        trueDamageDealt=("trueDamageDealt", "mean"),
        trueDamageDealtToChampions=("trueDamageDealtToChampions", "mean"),
        trueDamageTaken=("trueDamageTaken", "mean"),
        turretKills=("turretKills", "mean"),
        visionScore=("visionScore", "mean"),
        visionWardsBoughtInGame=("visionWardsBoughtInGame", "mean"),
        wardsKilled=("wardsKilled", "mean"),
        wardsPlaced=("wardsPlaced", "mean"),
        pinksPlaced=("pinksPlaced", "mean"),
        cs_15=("cs_15", "mean"),
        gold_15=("gold_15", "mean"),
        xp_15=("xp_15", "mean"),
        cs_15_diff=("cs_15_diff", "mean"),
        gold_15_diff=("gold_15_diff", "mean"),
        xp_15_diff=("xp_15_diff", "mean"),
        visionScorePerMinute=("visionScorePerMinute", "mean"),
        CSPerMinute=("CSPerMinute", "mean")
    ).round(2).reset_index()

    # Calculs finaux
    player_stats["winrate"] = round(player_stats["win_count"] / player_stats["matches_played"], 3)
    player_stats["teamPosition"] = player_stats["teamPosition"].map(position_dict)
    player_stats["FirstBlood_pct"] = round(player_stats["firstBloodKill"] / player_stats["matches_played"], 3)*100
    player_stats["FirstTurret_pct"] = round(player_stats["firstTurretKilled"] / player_stats["matches_played"], 3)*100
    player_stats = player_stats.merge(df_players[["name", "team"]], on="name", how="left")
    player_stats = player_stats.rename(columns={"teamPosition": "position"})

    # Scoring (si tu as une fonction existante)
    player_stats = scoring(player_stats, weighting)

    # --- NOUVEAU : Normalisation stricte des KPIs ---
    # Colonnes à exclure de la normalisation
    exclude_columns = [
        "name", "team", "position", "main", "matches_played", "win_count", "winrate",
        "FirstBlood_pct", "FirstTurret_pct", "score",  # Colonnes déjà calculées ou binaires
    ]

    # KPIs inverses (moins = mieux)
    inverse_kpis = [
        "deaths", "totalTimeSpentDead", "damageTakenOnTeamPercentage",
        "physicalDamageTaken", "magicDamageTaken", "trueDamageTaken"
    ]

    # Sélection des KPIs à normaliser (uniquement les colonnes numériques non exclues)
    kpis_to_normalize = [
        col for col in player_stats.select_dtypes(include=[np.number]).columns
        if col not in exclude_columns
    ]

    # Normalisation
    for kpi in kpis_to_normalize:
        if kpi in inverse_kpis:
            player_stats[f"{kpi}_norm"] = normalize_inverse(player_stats[kpi])
        else:
            player_stats[f"{kpi}_norm"] = normalize_minmax(player_stats[kpi])

    # Vérification des valeurs normalisées
    normalized_columns = [col for col in player_stats.columns if col.endswith("_norm")]
    for col in normalized_columns:
        if player_stats[col].min() < 0 or player_stats[col].max() > 100:
            print(f"Attention : {col} n'est pas normalisée correctement (min={player_stats[col].min()}, max={player_stats[col].max()})")

    # Réorganisation des colonnes pour l'export
    first_columns = ["name", "team", "position", "main", "score", "winrate", "matches_played"]
    normalized_columns = [col for col in player_stats.columns if col.endswith("_norm")]
    remaining_columns = [
        col for col in player_stats.columns
        if col not in first_columns + normalized_columns
    ]
    final_columns = first_columns + normalized_columns + remaining_columns
    player_stats = player_stats[final_columns]

    # Export
    file_name = "players_stats"
    player_stats.to_excel(f"tournaments/{tournament}/{file_name}.xlsx", index=False)
    print(f"Exported at tournaments/{tournament}/{file_name}.xlsx")

    return player_stats
