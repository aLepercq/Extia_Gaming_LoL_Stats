import time
import json
from requests_html import HTMLSession


def getPuuid(puuid_url: str, api_key: str, player) -> str:
    """Return the puuid of a player given its gametag + tagline
       -PUUID_URL is a string of the api url
       -USER is a list containing gametag and tigline strings
    """
    time.sleep(1.2)  # sleep to stay below api limits
    session = HTMLSession()
    response = session.get(puuid_url + player['gameName'] + '/' + player['tagLine'],
                           headers={"X-Riot-Token": api_key})
    if response.status_code == 200:
        return json.loads(response.text)['puuid']
    else:
        print(f"{response.status_code}")
        return ""


def getMatchlist(matchslist_url: str, api_key: str, puuid: str, start):
    """return the last 30 tourney matches from start epoch given a puuid"""
    time.sleep(1.2)  # sleep to stay below api limits
    session = HTMLSession()
    response = session.get(f"{matchslist_url}{puuid}/ids?startTime={start}&type=tourney&start=0&count=30",
                           headers={"X-Riot-Token": api_key})
    return json.loads(response.text)


def getMatchData(matchdata_url, api_key, matchid):
    """return the match data given a match_id"""
    time.sleep(1.2)
    session = HTMLSession()
    response = session.get(f"{matchdata_url}{matchid}",
                           headers={"X-Riot-Token": api_key})
    return json.loads(response.text)


def getMatchTimeLine(matchtimeline_url, api_key, matchid):
    """return the match data given a match_id"""
    time.sleep(1.2)
    session = HTMLSession()
    response = session.get(f"{matchtimeline_url}{matchid}/timeline",
                           headers={"X-Riot-Token": api_key})
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        print(f"{response.status_code}")
        return ""
