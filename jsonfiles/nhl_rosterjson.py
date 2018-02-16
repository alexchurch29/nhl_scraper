import pandas as pd
import requests
import json
import time
from nhl_main import get_url
from nhl_players import fix_name


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


def parse_gameinfo(game_json):
    """
    :param game_json: raw json
    :return: dict of coaches, officials
    """

    coaches = dict()
    officials = dict()

    coaches['Away'] = game_json['liveData']['boxscore']['teams']['away']['coaches'][0]['person']['fullName']
    coaches['Home'] = game_json['liveData']['boxscore']['teams']['home']['coaches'][0]['person']['fullName']

    referees = [official['official']['fullName'] for official in game_json['liveData']['boxscore']['officials']
                if official['officialType'] == 'Referee']
    linesman = [official['official']['fullName'] for official in game_json['liveData']['boxscore']['officials']
                if official['officialType'] == 'Linesman']
    officials['Referee'] = referees
    officials['Linesman'] = linesman

    return coaches, officials


def parse_player(player_list, player):
    """
    :param player_list = list of players from raw json
    :param player = player in player_list
    :return: dict of home & away playing rosters
    """

    players = dict()

    players["Player_Id"] = player_list[player]["id"]
    players["Full_Name"] = fix_name(player_list[player]["fullName"].upper())

    # Below info is always as of current season and not as of date of game. Disclude for now.
    '''
    if 'primaryNumber' in player_list[player]:
        players['Number'] = player_list[player]['primaryNumber']
    if 'birthDate' in player_list[player]:
        players['Birth_Date'] = player_list[player]['birthDate']
    if 'height' in player_list[player]:
        players['Height'] = player_list[player]['height']
    if 'weight' in player_list[player]:
    players['Weight'] = player_list[player]['weight']
    if 'alternateCaptain' in player_list[player] and player_list[player]['alternateCaptain'] is True:
        players['Captain'] = 'A'
    elif 'captain' in player_list[player] and player_list[player]['captain'] is True:
        players['Captain'] = 'C'
    if 'rookie' in player_list[player] and player_list[player]['rookie']is True:
        players['Rookie'] = 'R'
    if 'shootsCatches' in player_list[player]:
        players['Shoots'] = player_list[player]['shootsCatches']
    # players['Team_Id'] = player_list[player]['currentTeam']['id']
    # players['Team_Name'] = player_list[player]['currentTeam']['name']
    players['Position'] = player_list[player]['primaryPosition']['abbreviation']
    '''
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
    '''
    scratches = list()
    goalies = list()
    stars = list()
    for player in player_list:
        if player_list[player]['id'] in game_json['liveData']['boxscore']['teams']['away']['scratches'] or \
                        player_list[player]['id'] in game_json['liveData']['boxscore']['teams']['home']['scratches']:
            scratches.append(True)
        else:
            scratches.append(np.NaN)
    for player in player_list:
        if player_list[player]['id'] == game_json['liveData']['decisions']['winner']['id']:
            goalies.append('W')
        elif player_list[player]['id'] == game_json['liveData']['decisions']['loser']['id']:
            goalies.append('L')
        else:
            goalies.append(np.NaN)
    for player in player_list:
        if player_list[player]['id'] == game_json['liveData']['decisions']['firstStar']['id']:
            stars.append(1)
        elif player_list[player]['id'] == game_json['liveData']['decisions']['secondStar']['id']:
            stars.append(2)
        elif player_list[player]['id'] == game_json['liveData']['decisions']['thirdStar']['id']:
            stars.append(3)
        else:
            stars.append(np.NaN)
    roster['Scratch'] = scratches
    roster['Goalie'] = goalies
    roster['Star'] = stars
    '''
    columns = ['Game_Id', 'Player_Id', 'Full_Name'] #'Number', 'Position', 'Shoots', 'Birth_Date',
               # 'Height', 'Weight', 'Captain', 'Rookie', 'Scratch', 'Goalie', 'Star'
    roster = roster.reindex_axis(columns, axis=1)

    game_data = parse_gameinfo(game_json)

    coaches = pd.DataFrame(game_data[0], index=[0])
    coaches['game_Id'] = game_json['gamePk']

    officials = pd.DataFrame(game_data[1])
    officials['game_Id'] = game_json['gamePk']

    return roster#, coaches, officials


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
