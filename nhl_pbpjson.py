import pandas as pd
import requests
import json
import time
from nhl_main import get_url, convert_to_seconds
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


def change_event_name(event):
    """
    Change event names from json style to html
    ex: BLOCKED_SHOT to BLOCK
    :param event: event type
    :return: fixed event type
    """
    event_types = {
        'PERIOD_START': 'PSTR',
        'FACEOFF': 'FAC',
        'BLOCKED_SHOT': 'BLOCK',
        'GAME_END': 'GEND',
        'GIVEAWAY': 'GIVE',
        'GOAL': 'GOAL',
        'HIT': 'HIT',
        'MISSED_SHOT': 'MISS',
        'PERIOD_END': 'PEND',
        'SHOT': 'SHOT',
        'STOP': 'STOP',
        'TAKEAWAY': 'TAKE',
        'PENALTY': 'PENL',
        'Early Intermission Start': 'EISTR',
        'Early Intermission End': 'EIEND',
        'Shootout Completed': 'SOC',
    }

    try:
        return event_types[event]
    except KeyError:
        return event


def get_teams(json):
    """
    Get teams
    :param json: pbp json
    :return: dict with home and away
    """
    return {'Home': json['gameData']['teams']['home']['name'].upper(),
            'Away': json['gameData']['teams']['away']['name'].upper()}


def parse_event(event):
    """
    Parses a single event when the info is in a json format
    :param event: json of event
    :return: dictionary with the info
    """
    play = dict()

    play['Event_Type'] = str(change_event_name(event['result']['eventTypeId']))
    play['Event_Desc'] = event['result']['description']
    # play['Event_Num'] = event['about']['eventIdx']
    # commented out as eventIdx in json does not match event num from html
    play['Period'] = event['about']['period']
    play['Time_Elapsed'] = convert_to_seconds(event['about']['periodTime'])
    play['Away_Score'] = event['about']['goals']['away']
    play['Home_Score'] = event['about']['goals']['home']

    if event['result']['eventTypeId'] == "SHOT":
        if "Snap" in event['result']['secondaryType']:
            play["Secondary_Type"] = "Snap"
        elif "Slap" in event['result']['secondaryType']:
            play["Secondary_Type"] = "Slap"
        elif "Backhand" in event['result']['secondaryType']:
            play["Secondary_Type"] = "Backhand"
        elif "Tip-In" in event['result']['secondaryType']:
            play["Secondary_Type"] = "Tip-In"
        elif "Wrap-around" in event['result']['secondaryType']:
            play["Secondary_Type"] = "Wrap-around"
        elif "Deflected" in event['result']['secondaryType']:
            play["Secondary_Type"] = "Deflected"

    if event['result']['eventTypeId'] == "MISSED_SHOT":
        play["Secondary_Type"] = event['result']['description'].split('-')[1]

    if event['result']['eventTypeId'] == "PENALTY":
        play["Secondary_Type"] = event['result']['secondaryType']
        play["Penl_Length"] = event['result']['penaltyMinutes']

    if event['result']['eventTypeId'] == "GOAL":
        play["Secondary_Type"] = event['result']['secondaryType']

    # If there's a players key that means an event occurred on the play.
    if 'players' in event.keys():
        for i in range(len(event['players'])):
            play['P{}_Name'.format(i + 1)] = fix_name(event['players'][i]['player']['fullName'].upper())
            play['P{}_Id'.format(i + 1)] = str(event['players'][i]['player']['id'])

            # Coordinates aren't always there
            try:
                play['xC'] = event['coordinates']['x']
                play['yC'] = event['coordinates']['y']
            except KeyError:
                play['xC'] = ''
                play['yC'] = ''

    return play


def parse_json(game_json):
    """
    Scrape the json for a game
    :param game_json: raw json
    :return: Either a DataFrame with info for the game
    """

    plays = game_json['liveData']['plays']['allPlays'][2:]  # All the plays/events in a game

    # Go through all events and store all the info in a list
    # 'PERIOD READY' & 'PERIOD OFFICIAL'..etc aren't found in html...so get rid of them
    event_to_ignore = ['PERIOD_READY', 'PERIOD_OFFICIAL', 'GAME_READY', 'GAME_OFFICIAL', 'GAME_END']
    events = [parse_event(play) for play in plays if play['result']['eventTypeId'] not in event_to_ignore]

    pbp = pd.DataFrame(events)
    pbp['Game_Id'] = game_json['gamePk']
    pbp['Event_Num'] = pbp.index + 1
    columns = ['Game_Id', 'Period', 'Event_Num', 'Event_Type', 'Secondary_Type', 'Penl_Length',
               'Event_Desc', 'Time_Elapsed', 'P1_Id', 'P1_Name', 'P2_Id', 'P2_Name', 'P3_Id', 'P3_Name', 'P4_Id',
               'P4_Name', 'Home_Score', 'Away_Score', 'Home_Zone', 'Away_Zone', 'Home_Strength', 'Away_Strength',
               'G_Pulled_Home', 'G_Pulled_Away', 'xC', 'yC', 'Dist', 'H1', 'H2', 'H3', 'H4', 'H5', 'H6', 'A1', 'A2',
               'A3', 'A4', 'A5', 'A6', 'Penalty_Shot']
    pbp = pbp.reindex_axis(columns, axis=1)

    return pbp


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
