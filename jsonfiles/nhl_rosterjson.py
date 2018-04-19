from bs4 import BeautifulSoup
import re
import pandas as pd
import requests
import json
import time
from nhl_main import get_url
from nhl_players import fix_name
from nhl_teams import fix_team


def get_pbp(game_id):
    """
    Given a game_id it returns the raw json
    Ex: http://statsapi.web.nhl.com/api/v1/game/2016020475/feed/live
    :param game_id: the game
    :return: raw json of game
    """
    url = 'http://statsapi.web.nhl.com/api/v1/game/{}/feed/live'.format(game_id)

    try:
        response = get_url(url)
        time.sleep(1)
        pbp_json = json.loads(response.text)
    except requests.exceptions.HTTPError as e:
        print('Json pbp for game {} is not there'.format(game_id), e)
        return None

    return pbp_json


def parse_player(player_list, player):
    """
    :param player_list = list of players from raw json
    :param player = player in player_list
    :return: dict of home & away playing rosters
    """

    players = dict()

    players["Player_Id"] = player_list[player]["id"]
    players["Name"] = fix_name(player_list[player]["fullName"].upper())
    # if 'currentTeam' in player_list[player]:
    #    players['Team'] = fix_team(player_list[player]['currentTeam']['triCode'])
    if 'primaryPosition' in player_list[player]:
        players['Pos'] = player_list[player]['primaryPosition']['abbreviation']
    if 'shootsCatches' in player_list[player]:
        players['Shoots'] = player_list[player]['shootsCatches']
    # if 'primaryNumber' in player_list[player]:
    #    players['Num'] = player_list[player]['primaryNumber']
    # if 'currentAge' in player_list[player]:
    #    players['Age'] = player_list[player]['currentAge']
    if 'birthDate' in player_list[player]:
        players['Birth_Date'] = player_list[player]['birthDate']
    if 'birthCity' in player_list[player]:
        players['Birth_City'] = player_list[player]['birthCity']
    if 'birthStateProvince' in player_list[player]:
        players['Birth_Region'] = player_list[player]['birthStateProvince']
    if 'birthCountry' in player_list[player]:
        players['Birth_Country'] = player_list[player]['birthCountry']
    if 'nationality' in player_list[player]:
        players['Nationality'] = player_list[player]['nationality']
    if 'height' in player_list[player]:
        players['Height'] = player_list[player]['height']
    if 'weight' in player_list[player]:
        players['Weight'] = player_list[player]['weight']

    # get draft info from player html page
    url = 'https://www.nhl.com/player/{}-{}-{}'.format(player_list[player]["firstName"],
                                                       player_list[player]["lastName"],
                                                       player_list[player]["id"])
    html = get_url(url)
    time.sleep(1)
    soup = BeautifulSoup(html.content, 'html.parser')

    spans = soup.find_all('div', {'class': 'player-overview__bio'})  # find bio section
    bio = [i.get_text() for i in spans][0].split()  # split into list
    try:
        draft = bio[bio.index('Draft:'):bio.index('Draft:') + 9]  # find index for draft info.
        players['Draft_Year'] = int(draft[1])
        players['Draft_Team'] = draft[2].strip(',')
        players['Round'] = int(re.findall("\d+", draft[3])[0])
        players['Pick'] = int(re.findall("\d+", draft[5])[0])
        players['Overall'] = int(re.findall("\d+", draft[7])[0])
    except:
        pass  # player is undrafted

    return players


def parse_json(game_json):
    """
    :param game_json: raw json
    :return: pandas df of game info, coaches, officials, goalies, three stars, and playing rosters
    """

    player_list = game_json['gameData']['players']
    rosters = [parse_player(player_list, player) for player in player_list]
    roster = pd.DataFrame(rosters)
    roster['Game_Id'] = game_json['gamePk']

    columns = ['Game_Id', 'Player_Id', 'Name', 'Pos', 'Shoots', 'Birth_Date', 'Birth_City', 'Birth_Region',
               'Birth_Country', 'Nationality', 'Height', 'Weight', 'Draft_Year', 'Draft_Team', 'Round', 'Pick',
               'Overall']
    roster = roster.reindex_axis(columns, axis=1)

    return roster


def scrape_game(game_id):
    """
    Used for debugging
    :param game_id: game to scrape
    :return: DataFrame of game info
    """
    try:
        game_json = get_pbp(game_id)
    except requests.exceptions.HTTPError as e:
        print('Json pbp for game {} is not there'.format(game_id), e)
        return None

    try:
        game_df = parse_json(game_json)
    except Exception as e:
        print('Error for Json pbp for game {}'.format(game_id), e)
        return None

    return game_df
