import re
import pandas as pd
import requests
import json
import time
import sqlite3
from bs4 import BeautifulSoup
from nhl_main import get_url
from nhl_players import fix_name
from nhl_teams import fix_team


# here we access the db to build a list of players for which bios have already been scraped
# scraping each bio requires visiting each player's individual html page, so we want to avoid repeating this process
# if their bio has been previously scraped as each game contains nearly 40 unique players and bio info is static
conn = sqlite3.connect('nhl.db')
cur = conn.cursor()
old_bios = pd.read_sql('select Player_Id from bios', conn)
old_players = old_bios['Player_Id'].tolist()


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

    players['Player_Id'] = player_list[player]['id']
    players['Name'] = fix_name(player_list[player]['fullName'].upper())
    # often attributes are missing so we need to check first
    if 'primaryPosition' in player_list[player]:
        players['Pos'] = player_list[player]['primaryPosition']['abbreviation']
    if 'shootsCatches' in player_list[player]:
        players['Shoots'] = player_list[player]['shootsCatches']
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

    # get draft info from player html page as it is not included in the json
    url = 'https://www.nhl.com/player/{}-{}-{}'.format(player_list[player]['firstName'],
                                                       player_list[player]['lastName'],
                                                       player_list[player]['id'])
    html = get_url(url)
    time.sleep(1)
    soup = BeautifulSoup(html.content, 'html.parser')

    spans = soup.find_all('div', {'class': 'player-overview__bio'})  # find bio section
    bio = [i.get_text() for i in spans][0].split()  # split into list
    try:
        draft = bio[bio.index('Draft:'):bio.index('Draft:') + 9]  # find index for draft info.
        players['Draft_Year'] = int(draft[1])
        players['Draft_Team'] = fix_team(draft[2].strip(','))
        players['Draft_Round'] = int(re.findall("\d+", draft[3])[0])
        players['Draft_Pick'] = int(re.findall("\d+", draft[5])[0])
        players['Draft_Overall'] = int(re.findall("\d+", draft[7])[0])
    except:
        pass  # if player is undrafted this section does not exist. not as error so we will just skip this step

    return players


def parse_json(game_json):
    """
    this returns a full df which includes all bios for new players who do not already exist in the db
    :param game_json: raw json
    :return: pandas df containing player bios
    """

    player_list = game_json['gameData']['players']
    rosters = [parse_player(player_list, player) for player in player_list
               if player_list[player]['id'] not in old_players]
    roster = pd.DataFrame(rosters)

    columns = ['Player_Id', 'Name', 'Pos', 'Shoots', 'Birth_Date', 'Birth_City', 'Birth_Region',
               'Birth_Country', 'Nationality', 'Height', 'Weight', 'Draft_Year', 'Draft_Team', 'Draft_Round',
               'Draft_Pick', 'Draft_Overall']
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
