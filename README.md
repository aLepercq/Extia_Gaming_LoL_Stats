# Extia Gaming LoL Stats

Extia Gaming LoL Stats is a data analysis project for League of Legends tournaments, 
using match data retrieved via the Riot Games API and stored in MongoDB. 
The aim is to generate detailed player statistics, such as KDA, minions per minute, vision score, and much more.
The toornament has to be managed on play.toornament.com to get full statistics.

## Table of Contents
- [Installation](#Installation)
- [Configuration](#Configuration)
- [How to use](#How-to-use)
- [Functionalities](#Functionalities)
- [Database Structure](#Database-Structure)
- [Riot Games API](#Riot-Games-API)
- [Exporting Results](#Exporting-Results)
- [Licence](#licence)

## Installation

1. Clone Repo :
```bash
git clone https://github.com/aLepercq/Extia_Gaming_LoL_Stats.git
cd Extia_Gaming_LoL_Stats
```

2. Install dependencies: Use a virtual environment or install directly with pip :
```bash
pip install -r requirements.txt
```

3. MongoDB: Make sure MongoDB is installed and running. This project uses MongoDB to store player and match information.

## Configuration
- Environment variables :
    - Create a <span style="background-color: #9db6c9">.env</span> file to store the MongoDB parameters:
    ```dotenv
    TOORNAMENT_NAME=<your toornament name> # will be used as DB name
    MONGODB_URI=<your_mongo_adress_here>
    ```
    - Add the latest version of Data Dragon:
    ```dotenv
    DATADRAGON_VERSION=<dragontail-version>
    ``` 
    - Add your Riot Games API key:
    ```dotenv
    RIOT_API_KEY=<your_api_key_here>
    ```
    - Add the Riot API routes needed:
    ```dotenv
    PUUID_URL=https://europe.api.riotgames.com/riot/account/v1/accounts/by-riot-id/
    MATCHSLIST_URL=https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/
    MATCHDATA_URL=https://europe.api.riotgames.com/lol/match/v5/matches/
    MATCHTIMELINE_URL=https://europe.api.riotgames.com/lol/match/v5/matches/
    ```
- API Authorization: 
This project uses the Riot Games API to collect League of Legends data. 
To access match information, a valid API key is required.
- Toornament variables
    - Create a <span style="background-color: #9db6c9">.config</span> file to store the tournament parameters:
    ```dotenv
    START_TIMESTAMP=<unix timestamp in seconds>
    END_TIMESTAMP=<unix timestamp in seconds>
    ```
    - Create a <span style="background-color: #9db6c9">.tournamentCodes.txt</span> file to store the tournament codes:
    ```dotenv
    EUWXXX-XXXX-XX-XXXX # fill with all the tournament codes
    ```

## How to use
1. Run the main script :
The script will ask you if you want to update the database, update player statistics or something else
```bash
python main.py
```
2. Check the logs :
connection information and progress will be displayed in the console.
3. Data export
The results will be exported to an Excel file <span style="background-color: #9db6c9">player_statistics.xlsx</span> 
containing the player statistics.

## Functionalities
- Player statistics: Extracts and calculates statistics for each player based on several metrics 
(KDA, damage inflicted, participation in kills, minions per minute, etc.).
- Export: Automatic generation of an Excel file summarising all the data collected for easy analysis.
- Average Victory Time: Calculates the average time in minutes for games won by each player.

## Database Structure
The MongoDB collections used are :
- Players: Details of players registered for the tournament, including puuid, gameName, tagLine and team.
- Matches: Match data retrieved from the Riot API, including basic statistics.
- Timelines: Match timeline data retrieved from the Riot API, including events and data by frames (60s between frames)

## Riot Games API
Match data is retrieved using the Riot Games match-v5 API:
[Riot Games API documentation](https://developer.riotgames.com/apis#match-v5)

## Licence
This project is under MIT license.