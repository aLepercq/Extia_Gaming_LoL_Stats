from utils.db.updateDB import update_players, update_matches
from utils.stats.prepareStats import *
from utils.stats.playerStats import *
from utils.common.commonFunctions import logger, getFileValue


def update_database(tournament: str):
    print("Choose an option to update the database :")
    print("   - a : Update players")
    print("   - b : Update matches")
    print("   - c : Update All")
    choice = input("Enter your choice (a/b/c) : ").strip().lower()

    if choice == 'a':
        update_players(tournament)
    elif choice == 'b':
        update_matches(tournament)
    elif choice == 'c':
        update_players(tournament)
        update_matches(tournament)
    else:
        logger.error("Invalid choice.")


def update_statistics(tournament: str):
    print("Choose an option to update the statistics :")
    print("a : Update player statistics")
    print("b : Update champion pool")
    print("c : Update champion statistics")
    print("d : Update All")
    print("e : Get the Head-to-Head (H2H)")
    choice = input("Enter your choice (a/b/c/d/e) : ").strip().lower()

    generate_player_match_stats(tournament)

    if choice == 'a':
        generate_players_stats(tournament)
    elif choice == 'b':
        logger.info("Update champion pool...")
        # update_champion_pool(tournament)
    elif choice == 'c':
        logger.info("Update champion statistics")
        # update_champion_stats(tournament)
    elif choice == 'd':
        logger.info("Update All...")
        # update_player_stats(tournament)
        # update_champion_pool(tournament)
        # update_champion_stats(tournament)
    elif choice == 'e':
        logger.info("Obtaining Head-to-Head (H2H)...")
        # get_H2H(tournament)
    else:
        logger.error("Invalid choice.")


def main():
    tournament = getFileValue("TOORNAMENT_NAME", ".env")
    print("Choose an option  :")
    print("1 : Update the database")
    print("2 : Update statistics")
    choice = input("Enter your choice (1/2) : ").strip()

    if choice == '1':
        update_database(tournament)
    elif choice == '2':
        update_statistics(tournament)
    else:
        logger.error("Invalid choice.")


if __name__ == "__main__":
    main()
