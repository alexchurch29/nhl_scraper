import pandas as pd
import json
import time
from nhl_main import get_url, convert_to_seconds
from nhl_players import fix_name


def get_shifts(game_id):
    """
    Given a game_id it returns the raw json
    Ex: http://www.nhl.com/stats/rest/shiftcharts?cayenneExp=gameId=2010020001
    :param game_id: the game
    :return:
    """
    url = 'http://www.nhl.com/stats/rest/shiftcharts?cayenneExp=gameId={}'.format(game_id)
    response = get_url(url)
    time.sleep(1)

    shift_json = json.loads(response.text)
    return parse_json(shift_json, game_id)


def parse_shift(shift):
    """
    Parse shift for json
    :param shift: json for shift
    :return: dict with shift info
    """

    shift_dict = dict()
    name = fix_name(' '.join([shift['firstName'].strip(' ').upper(), shift['lastName'].strip(' ').upper()]))
    shift_dict['Period'] = shift['period']
    shift_dict['Player'] = name
    shift_dict['Team'] = shift['teamAbbrev']
    shift_dict['Shift'] = shift['shiftNumber']
    if shift['eventDescription'] is None:
        shift_dict['Start'] = convert_to_seconds(shift['startTime'])
        shift_dict['End'] = convert_to_seconds(shift['endTime'])
        shift_dict['Duration'] = convert_to_seconds(shift['duration'])
    else:
        shift_dict = dict()

    return shift_dict


def parse_json(shift_json, game_id):
    """
    Parse the json
    :param shift_json: raw json
    :param game_id: if of game
    :return: DataFrame with info
    """
    columns = ['Player', 'Period', 'Team', 'Shift', 'Start', 'End', 'Duration']
    shifts = [parse_shift(shift) for shift in shift_json['data']]     # Go through the shifts
    shifts = [shift for shift in shifts if shift != {}]               # Get rid of null shifts (which happen at end)

    df = pd.DataFrame(shifts, columns = columns)
    df.insert(0, 'Game_Id', str(game_id))
    df = df.sort_values(by=['Period', 'Start'], ascending=[True, True])  # Sort by period by time
    df = df.reset_index(drop=True)

    return df
