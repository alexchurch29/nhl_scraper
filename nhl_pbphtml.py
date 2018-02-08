import pandas as pd
import numpy as np
from bs4 import BeautifulSoup, SoupStrainer
import time
import re
import requests
from nhl_players import fix_name
from nhl_main import get_url, convert_to_seconds
from nhl_pbpjson import get_pbp as get_json


def get_pbp(game_id):
    """
    Given a game_id it returns the raw html
    Ex: http://www.nhl.com/scores/htmlreports/20162017/PL020475.HTM
    :param game_id: the game
    :return: raw html of game
    """
    game_id = str(game_id)
    url = 'http://www.nhl.com/scores/htmlreports/{}{}/PL{}.HTM'.format(game_id[:4], int(game_id[:4]) + 1, game_id[4:])

    time.sleep(1)
    return get_url(url)


def return_name_html(info):
    """
    In the PBP html the name is in a format like: 'Center - MIKE RICHARDS'
    Some also have a hyphen in their last name so can't just split by '-'
    :param info: position and name
    :return: name
    """
    s = info.index('-')  # Find first hyphen
    return info[s + 1:].strip(' ')  # The name should be after the first hyphen


def strip_html_pbp(td):
    """
    Strip html tags and such
    :param td: pbp
    :return: list of plays (which contain a list of info) stripped of html
    """

    for y in range(len(td)):
        # Get the 'br' tag for the time column...this get's us time remaining instead of elapsed and remaining combined
        if y == 3:
            td[y] = td[y].get_text()   # This gets us elapsed and remaining combined-< 3:0017:00
            index = td[y].find(':')
            td[y] = td[y][:index+3]
        elif (y == 6 or y == 7) and td[0] != '#':
            # 6 & 7-> These are the player one ice one's
            # The second statement controls for when it's just a header
            baz = td[y].find_all('td')
            bar = [baz[z] for z in range(len(baz)) if z % 4 != 0] # Because of previous step we get repeats..delete some

            # The setup in the list is now: Name/Number->Position->Blank...and repeat
            # Now strip all the html
            players = []
            for i in range(len(bar)):
                if i % 3 == 0:
                    try:
                        name = return_name_html(bar[i].find('font')['title'])
                        number = bar[i].get_text().strip('\n')  # Get number and strip leading/trailing endlines
                    except KeyError:
                        name = ''
                        number = ''
                elif i % 3 == 1:
                    if name != '':
                        position = bar[i].get_text()
                        players.append([name, number, position])

            td[y] = players
        else:
            td[y] = td[y].get_text()

    return td


def clean_html_pbp(html):
    """
    Get rid of html and format the data
    :param html: the requested url
    :return: a list with all the info
    """
    strainer = SoupStrainer('td', attrs={'class': re.compile(r'bborder')})
    soup = BeautifulSoup(html.content, "lxml", parse_only=strainer)
    soup = soup.select('td.+.bborder')

    # Create a list of lists (each length 8)...corresponds to 8 columns in html pbp
    td = [soup[i:i + 8] for i in range(0, len(soup), 8)]

    cleaned_html = [strip_html_pbp(x) for x in td]

    return cleaned_html


def get_coords(game_id):
    """
    returns X, Y coordinates for html pbp parser
    :param game_id: gameid
    :return: dict of X, Y coordinates
    """

    def parse_coords(event):
        play = dict()
        if 'players' in event.keys():
            for i in range(len(event['players'])):

                # Coordinates aren't always there
                try:
                    play['xC'] = event['coordinates']['x']
                    play['yC'] = event['coordinates']['y']
                except KeyError:
                    play['xC'] = np.NaN
                    play['yC'] = np.NaN
        return play

    try:
        game_json = get_json(game_id)
    except requests.exceptions.HTTPError as e:
        print('Json pbp for game {} is not there'.format(game_id), e)
        return None

    plays = game_json['liveData']['plays']['allPlays'][2:]  # All the plays/events in a game
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
    events = [parse_coords(play) for play in plays if play['result']['eventTypeId'] in event_types.keys()]

    coords = pd.DataFrame(events)

    return coords


def parse_pbp(gameid):
    """
    parse cleaned html and create pd Dataframe
    :param gameid: game id
    :return: df where each row is an event in pb
    """

    html = get_pbp(gameid)
    events = clean_html_pbp(html)

    teams = dict()
    teams['away'] = events[0][6][:3]
    teams['home'] = events[0][7][:3]

    score = {'home': 0, 'away': 0}

    pbp_events = []

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

    for i in events[:len(events)]:
        if i[4] in event_types.values() and i[0] != '#':
            play = dict()
            play['Home_Score'] = score['home']
            play['Away_Score'] = score['away']
            # play['Team_away'] = teams['away']
            # play['Team_home'] = teams['home']
            play['Event_Num'] = i[0]
            play['Period'] = i[1]
            # play['strength'] = i[2]
            play['Time_Elapsed'] = convert_to_seconds(i[3])
            play['Event_Type'] = i[4]
            play['Event_Desc'] = i[5]

            if 'Off. Zone' in i[5] and i[5][:3] == teams['home']:
                play['Home_Zone'] = 'OZ'
                play['Away_Zone'] = 'DZ'
            elif 'Off. Zone' in i[5] and i[5][:3] == teams['away']:
                play['Home_Zone'] = 'DZ'
                play['Away_Zone'] = 'OZ'
            elif 'Def. Zone' in i[5] and i[5][:3] == teams['home'] and i[4] == 'BLOCK':
                play['Home_Zone'] = 'OZ'
                play['Away_Zone'] = 'DZ'
            elif 'Def. Zone' in i[5] and i[5][:3] == teams['home'] and i[4] != 'BLOCK':
                play['Home_Zone'] = 'DZ'
                play['Away_Zone'] = 'OZ'
            elif 'Def. Zone' in i[5] and i[5][:3] == teams['away'] and i[4] == 'BLOCK':
                play['Home_Zone'] = 'DZ'
                play['Away_Zone'] = 'OZ'
            elif 'Def. Zone' in i[5] and i[5][:3] == teams['away'] and i[4] != 'BLOCK':
                play['Home_Zone'] = 'OZ'
                play['Away_Zone'] = 'DZ'
            elif 'Neu. Zone' in i[5]:
                play['Home_Zone'] = 'NZ'
                play['Away_Zone'] = 'NZ'

            if len(i[6]) > 0 and i[6][len(i[6])-1][2] == 'G':
                play['Away_Strength'] = len(i[6])-1
                play['G_Pulled_Away'] = False
            elif len(i[6]) > 0 and i[6][len(i[6])-1][2] != 'G':
                play['Away_Strength'] = len(i[6])
                play['G_Pulled_Away'] = True
            if len(i[7]) > 0 and i[7][len(i[7])-1][2] == 'G':
                play['Home_Strength'] = len(i[7]) - 1
                play['G_Pulled_Home'] = False
            elif len(i[7]) > 0 and i[7][len(i[7])-1][2] != 'G':
                play['Home_Strength'] = len(i[7])
                play['G_Pulled_Home'] = True

            for player in i[6]:
                n = i[6].index(player)+1
                play['A{}Name'.format(n)] = fix_name(player[0])
                play['A{}Num'.format(n)] = player[1]
                play['A{}Pos'.format(n)] = player[2]
            for player in i[7]:
                n = i[7].index(player)+1
                play['H{}Name'.format(n)] = fix_name(player[0])
                play['H{}Num'.format(n)] = player[1]
                play['H{}Pos'.format(n)] = player[2]

            if 'Penalty Shot' in i[5]:
                play['Penalty_Shot'] = True
            else:
                play['Penalty_Shot'] = False

            if i[4] == 'FAC':
                # MTL won Neu. Zone - MTL #11 GOMEZ vs TOR #37 BRENT
                play['P1_Team'] = i[5][:3]
                for value in teams.values():
                    if value != i[5][:3]:
                        play['P2_Team'] = value
                regex = re.compile(r'(.{3})\s+#(\d+)')
                desc = regex.findall(i[5])  # [[Team, num], [Team, num]]
                for player in desc:
                    if player[0] == i[5][:3]:
                        play['P1_Num'] = player[1]
                    else:
                        play['P2_Num'] = player[1]

            penl = {'Hooking': 'Hooking', 'Holding': 'Holding', 'Holding the stick': 'Holding the stick',
                    'Interference': 'Interference', 'Roughing': 'Roughing', 'Tripping': 'Tripping',
                    'Unsportsmanlike conduct': 'Unsportsmanlike conduct', 'Illegal equipment': 'Illegal equipment',
                    'Diving': 'Embellishment', 'Embellishment': 'Embellishment', 'Broken stick': 'Broken stick',
                    'Delaying Game-Puck over glass': 'Delaying Game-Puck over glass',
                    'Delay Gm - Face-off Violation': 'Delay Gm - Face-off Violation', 'Delay of game': 'Delay of game',
                    'Delaying the game': 'Delay of game', 'Delay of game - bench': 'Delay of game - bench',
                    'Delaying Game-Ill. play goalie': 'Delaying Game-Ill. play goalie',
                    'Delaying Game-Smothering puck': 'Delaying Game-Smothering puck',
                    'Goalie leave crease': 'Goalie leave crease',
                    'Face-off violation-bench': 'Face-off violation-bench', 'Bench': 'Bench',
                    'Illegal stick': 'Illegal stick', 'Closing hand on puck': 'Closing hand on puck',
                    'Throwing stick': 'Throwing stick', 'Too many men/ice - bench': 'Too many men/ice - bench',
                    'Abusive language - bench': 'Abuse of officials - bench',
                    'Abuse of officials - bench': 'Abuse of officials - bench',
                    'Interference on goalkeeper': 'Interference on goalkeeper', 'Hi-sticking': 'High-sticking',
                    'Hi stick - double minor': 'High-sticking', 'Cross checking': 'Cross checking',
                    'Cross check - double minor': 'Cross checking', 'Slashing': 'Slashing', 'Charging': 'Charging',
                    'Boarding': 'Boarding', 'Kneeing': 'Kneeing', 'Clipping': 'Clipping', 'Elbowing': 'Elbowing',
                    'Spearing': 'Spearing', 'Butt ending': 'Butt ending', 'Head butting': 'Head butting',
                    'Illegal check to head': 'Illegal check to head',  'Fighting': 'Fighting',
                    'Instigator': 'Instigator', 'Abuse of officials': 'Abuse of officials', 'Aggressor': 'Aggressor',
                    'PS-': "Penalty Shot", 'PS -': "Penalty Shot", 'Misconduct': 'Misconduct',
                    'Game misconduct': 'Game misconduct', 'Game Misconduct': 'Game misconduct',
                    'Match penalty': 'Game Misconduct', 'Checking from behind': 'Checking from behind'}

            if i[4] == 'PENL':
                for pen in penl:
                    if pen in i[5]:
                        play['Secondary_Type'] = penl[pen]
                    elif pen not in penl:
                        play['Secondary_Type'] = 'misc'

                if '2 min' in i[5]:
                    play['Penl_Length'] = 2
                elif '4 min' in i[5]:
                    play['Penl_Length'] = 4
                elif '5 min' in i[5]:
                    play['Penl_Length'] = 5
                elif '10 min' in i[5]:
                    play['Penl_Length'] = 10

                play['P1_Team'] = i[5][:3]
                for value in teams.values():
                    if value != i[5][:3]:
                        play['P2_Team'] = value
                if 'TEAM' in i[5]:
                    play['P1_Num'] = i[5][:3]
                else:
                    regex = re.compile(r'(.{3})\s+#(\d+)')
                    desc = regex.findall(i[5])  # [[Team, num], [Team, num]]
                    for player in desc:
                        if player[0] == i[5][:3]:
                            play['P1_Num'] = player[1]
                        elif player[0] != i[5][:3]:
                            play['P2_Num'] = player[1]

            if i[4] == 'HIT':
                play['P1_Team'] = i[5][:3]
                for value in teams.values():
                    if value != i[5][:3]:
                        play['P2_Team'] = value
                regex = re.compile(r'(.{3})\s+#(\d+)')
                desc = regex.findall(i[5])  # [[Team, num], [Team, num]]
                for player in desc:
                    if player[0] == i[5][:3]:
                        play['P1_Num'] = player[1]
                    elif player[0] != i[5][:3]:
                        play['P2_Num'] = player[1]

            shot_types = ['Wrist', 'Snap', 'Slap', 'Deflected', 'Tip-In', 'Backhand', 'Wrap-around']

            if i[4] == 'BLOCK':
                play['P2_Team'] = i[5][:3]
                for value in teams.values():
                    if value != i[5][:3]:
                        play['P1_Team'] = value
                for shot in shot_types:
                    if shot in i[5]:
                        play['Secondary_Type'] = shot
                regex = re.compile(r'(.{3})\s+#(\d+)')
                desc = regex.findall(i[5])  # [[Team, num], [Team, num]]
                for player in desc:
                    if player[0] == i[5][:3]:
                        play['P2_Num'] = player[1]
                    elif player[0] != i[5][:3]:
                        play['P1_Num'] = player[1]

            if i[4] == 'SHOT':
                play['P1_Team'] = i[5][:3]
                for value in teams.values():
                    if value != i[5][:3]:
                        play['P4_Team'] = value
                for shot in shot_types:
                    if shot in i[5]:
                        play['Secondary_Type'] = shot
                regex = re.compile(r'#(\d+)')
                desc = regex.search(i[5]).groups()  # num
                play['P1_Num'] = desc[0]
                for key, value in teams.items():
                    if value != i[5][:3] and key == 'away':
                        play['P4_Num'] = i[6][len(i[6])-1][1]
                    elif value != i[5][:3] and key == 'home':
                        play['P4_Num'] = i[7][len(i[7])-1][1]
                dist = i[5].split(',')
                feet = re.findall('\d+', dist[len(dist)-1][1:])
                play['Dist'] = feet[0]

            miss_types = ['Over Net', 'Wide of Net', 'Crossbar', 'Goalpost']

            if i[4] == 'MISS':
                play['P1_Team'] = i[5][:3]
                for shot in shot_types:
                    if shot in i[5]:
                        play['Secondary_Type'] = shot
                for miss in miss_types:
                    if miss in i[5]:
                        play['Tertiary_Type'] = miss
                regex = re.compile(r'#(\d+)')
                desc = regex.search(i[5]).groups()  # num
                play['P1_Num'] = desc[0]
                dist = i[5].split(',')
                feet = re.findall('\d+', dist[len(dist) - 1][1:])
                play['Dist'] = feet[0]

            if i[4] == 'GIVE' or i[4] == 'TAKE':
                play['P1_Team'] = i[5][:3]
                regex = re.compile(r'#(\d+)')
                desc = regex.search(i[5]).groups()  # num
                play['P1_Num'] = desc[0]

            if i[4] == 'GOAL':
                play['P1_Team'] = i[5][:3]
                for value in teams.values():
                    if value != i[5][:3]:
                        play['P4_Team'] = value
                for shot in shot_types:
                    if shot in i[5]:
                        play['Secondary_Type'] = shot
                for key, value in teams.items():
                    if value != i[5][:3] and key == 'away':
                        play['P4_Num'] = i[6][len(i[6])-1][1]
                    elif value != i[5][:3] and key == 'home':
                        play['P4_Num'] = i[7][len(i[7])-1][1]
                regex = re.compile(r'#(\d+)\s+')
                desc = regex.findall(i[5])  # [num] -> ranging from 1 to 3 indices
                play['P1_Num'] = desc[0]
                if len(desc) >= 2:
                    play['P2_Num'] = desc[1]
                    play['P2_Team'] = i[5][:3]
                    if len(desc) == 3:
                        play['P3_Num'] = desc[2]
                        play['P3_Team'] = i[5][:3]

                dist = i[5].split(',')
                for d in dist:
                    if 'ft' in d:
                        feet = re.findall('\d+', d[1:])
                        play['Dist'] = feet[0]

                for key, value in teams.items():
                    if value == i[5][:3] and key == 'away':
                        score['away'] += 1
                    elif value == i[5][:3] and key == 'home':
                        score['home'] += 1
                play['Home_Score'] = score['home']
                play['Away_Score'] = score['away']

            if i[4] == 'STOP':
                if 'ICING' in i[5]:
                    play['Secondary_Type'] = 'Icing'
                elif 'OFFSIDE' in i[5]:
                    play['Secondary_Type'] = 'Offside'

            pbp_events.append(play)

    pbp = pd.DataFrame(pbp_events)
    pbp['Game_Id'] = gameid

    try:
        pbp['xC'] = get_coords(gameid)['xC']
    except:
        pass
    try:
        pbp['yC'] = get_coords(gameid)['yC']
    except:
        pass

    columns = ['Game_Id', 'Period', 'Event_Num', 'Event_Type', 'Secondary_Type', 'Tertiary_Type', 'Penl_Length',
               'Event_Desc', 'Time_Elapsed', 'P1_Team', 'P1_Num', 'P2_Team', 'P2_Num', 'P3_Team', 'P3_Num', 'P4_Team',
               'P4_Num', 'Home_Score', 'Away_Score', 'Home_Zone', 'Away_Zone', 'Home_Strength', 'Away_Strength',
               'G_Pulled_Home', 'G_Pulled_Away', 'xC', 'yC', 'Dist', 'H1Name', 'H1Num', 'H1Pos', 'H2Name', 'H2Num',
               'H2Pos', 'H3Name', 'H3Num', 'H3Pos', 'H4Name', 'H4Num', 'H4Pos', 'H5Name', 'H5Num', 'H5Pos', 'H6Name',
               'H6Num', 'H6Pos', 'A1Name', 'A1Num', 'A1Pos', 'A2Name', 'A2Num', 'A2Pos', 'A3Name', 'A3Num', 'A3Pos',
               'A4Name', 'A4Num', 'A4Pos', 'A5Name', 'A5Num', 'A5Pos', 'A6Name', 'A6Num', 'A6Pos', 'Penalty_Shot']
    pbp = pbp.reindex_axis(columns, axis=1)

    return pbp
