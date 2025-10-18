from utils.common.commonFunctions import *
import pandas as pd

def generate_champion_pool(tournament: str):
    """generate the championpool stats"""
    logger.info("Update champion pool...")
    db = logToDB(tournament)
    stats_players = db['stats_players']

    # Récupérer les données
    cursor = stats_players.find({}, {"_id": 0, "name": 1, "championName": 1, "win": 1})
    df = pd.DataFrame(list(cursor))

    if df.empty:
        logger.warning("No data found in stats_players.")
        return

    # Calcul des picks et wins
    champion_pool = (
        df.groupby(["name", "championName"])
        .agg(
            picks=("championName", "count"),
            wins=("win", "sum")  # True=1, False=0 → ça compte directement les wins
        )
        .reset_index()
    )

    # Sauvegarde en CSV
    file_name = "champion_pool"
    champion_pool.to_excel(f"tournaments/{tournament}/{file_name}.xlsx", index=False)
    logger.info(f"Champion pool CSV saved to {tournament}/{file_name}.xlsx")
