from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
import re
from nhl_main import get_url
from nhl_players import fix_name
from nhl_teams import fix_team


def get_html(game_id):
    """
    Given a game_id it returns the raw Playing Roster html
    Ex: http://www.nhl.com/scores/htmlreports/20162017/RO020475.HTM
    :param game_id: 2016020475
    :return: raw html of game
    """
    game_id = str(game_id)
    url = 'http://www.nhl.com/scores/htmlreports/{}{}/RO{}.HTM'.format(game_id[:4], int(game_id[:4]) + 1, game_id[4:])

    return get_url(url)


def get_players(soup):
    """
    scrape roster for players, scratches, captains, teams
    :param soup: html
    :return: dict for home & away players, dict for home & away scratches, dict for home & away captains, dict for teams
    """
    players = dict()
    scratches = dict()
    captains = dict()
    team = dict()

    tables = soup.find_all('table', {'align': 'center', 'border': '0', 'cellpadding': '0',
                                     'cellspacing': '0', 'width': '100%'})
    """
    There are 5 tables which correspond to the above criteria.
    tables[0] is game info
    tables[1] is away starters
    tables[2] is home starters
    tables[3] is away scratches
    tables[4] is home scratches
    """

    del tables[0]
    player_info = [table.find_all('td') for table in tables]

    player_info = [[x.get_text() for x in group] for group in player_info]

    # Make list of list of 3 each. The three are: number, position, name (in that order)
    player_info = [[group[i:i + 3] for i in range(0, len(group), 3)] for group in player_info]

    # Get rid of header column
    player_info = [[player for player in group if player[0] != '#'] for group in player_info]

    # Create dict that records captains for the given game
    # {'Away Captain': 'AWAY CAPTAIN', 'Away Assistants': 'AWAY ASSISTANTS',
    # 'Home Captain': 'HOME CAPTAIN', 'Home Assistants': 'HOME ASSISTANTS'}
    captains['Away Captain'] = [i for i in player_info[0] if i[0] != '\xa0' and i[2].find('(C)') != -1]
    captains['Away Assistants'] = [i for i in player_info[0] if i[0] != '\xa0' and i[2].find('(A)') != -1]
    captains['Home Captain'] = [i for i in player_info[1] if i[0] != '\xa0' and i[2].find('(C)') != -1]
    captains['Home Assistants'] = [i for i in player_info[1] if i[0] != '\xa0' and i[2].find('(A)') != -1]

    away_players = player_info[0] + player_info[2]
    home_players = player_info[1] + player_info[3]
    away_scratches = player_info[2]
    home_scratches = player_info[3]

    def fix_capt(player):
        """
        Sometimes a player had a (A) or (C) attached to their name
        :param player: list of player info -> [number, position, name]
        :return: fixed list
        """
        player[2] = player[2][:player[2].find('(')]
        player[2] = player[2].strip()

        return player

    # For those with (A) or (C) in name field get rid of it
    # First condition is to control when we get whitespace as one of the indices
    players['Away'] = [fix_capt(i) if i[0] != '\xa0' and i[2].find('(') != -1 else i for i in away_players]
    players['Home'] = [fix_capt(i) if i[0] != '\xa0' and i[2].find('(') != -1 else i for i in home_players]
    scratches['Away Scratch'] = [fix_capt(i) if i[0] != '\xa0' and i[2].find('(') != -1 else i for i in away_scratches]
    scratches['Home Scratch'] = [fix_capt(i) if i[0] != '\xa0' and i[2].find('(') != -1 else i for i in home_scratches]

    # Get rid when just whitespace
    players['Away'] = [i for i in away_players if i[0] != u'\xa0']
    players['Home'] = [i for i in home_players if i[0] != u'\xa0']
    scratches['Away Scratch'] = [i for i in away_scratches if i[0] != u'\xa0']
    scratches['Home Scratch'] = [i for i in home_scratches if i[0] != u'\xa0']

    # Returns home and away team
    teams = soup.find_all(class_='teamHeading')
    teams = [i.get_text() for i in teams]
    team['Away'] = fix_team(teams[0])
    team['Home'] = fix_team(teams[1])

    return players, scratches, captains, team


def get_gameinfo(soup):
    """
    scrape game sheet for game attendance, start/end time, timezone - will be added to schedule
    :param soup: html
    :return: dict of attendance, start/end time, timezone
    """

    game_info = dict()

    tables = soup.find_all('table', {'align': 'center', 'border': '0', 'cellpadding': '0',
                                     'cellspacing': '0', 'width': '100%'})

    text = tables[0].find_all('td', text=True)
    text = [str(i.get_text()).split('\xa0') for i in text]
    for i in text:
        for v in i:
            if 'Attendance' in v:
                game_info['Attendance'] = v[11:]
            if 'Ass./Att.' in v:
                game_info['Attendance'] = v[10:]
            if 'Start' in v:
                k = i.index(v) + 1
                game_info['Start'] = i[k]
            if 'End' in v:
                k = i.index(v) + 1
                game_info['End'] = i[k]
                k = i.index(v) + 2
                game_info['TZ'] = i[k]

    return game_info


def get_coaches(soup):
    """
    scrape head coaches from Playing Roster html
    :param soup: html content
    :return: dict of coaches for game {"Away": "AWAY COACH", "Home": "HOME COACH"}
    """
    head_coaches = dict()

    coaches = soup.find_all('tr', id='HeadCoaches')
    coaches = coaches[0].find_all('td')
    head_coaches['Away'] = coaches[1].get_text()
    head_coaches['Home'] = coaches[3].get_text()

    return head_coaches


def get_officials(soup):
    """
    scrape officials from Playing Roster html
    :param soup: html content
    :return: dict of officials for game {"Referee": "Referee", "Linesman": "Linesman"}
    """
    officials = dict()

    official = soup.find_all('table', {'border': '0', 'cellpadding': '0',
                                       'cellspacing': '0', 'width': '100%'})
    official = official[5].find_all('td')
    official = [i.get_text() for i in official]
    officials['Referee'] = official[3:5]
    officials['Linesman'] = official[6:8]

    return officials


def get_goalies(soup_game_summary):
    """
    Scrape winning & losing goalie from boxscore
    :param soup_game_summary: game summary html
    :return: dict of winning & losing goalies
    """

    table = soup_game_summary.find('table', id='MainTable')
    tables = table.find_all('td', 'bborder + rborder')
    text = [i.get_text() for i in tables]
    from itertools import zip_longest
    goalies = [j for i, j in zip_longest(text[:-1], text[1:]) if i == 'G']
    goalie = [re.split(r'[, ]', i) for i in goalies]
    goalies = dict()
    for i in goalie:
        if '(W)' in i:
            goalies['winner'] = fix_name(' '.join((i[2], i[0])))
        elif '(L)' in i:
            goalies['loser'] = fix_name(' '.join((i[2], i[0])))
        elif '(OT)' in i:
            goalies['overtime'] = fix_name(' '.join((i[2], i[0])))

    return goalies


def get_stars(soup_game_summary):
    """
    Scrape game summary for three stars
    :param soup_game_summary: game summary html
    :return: list of three stars for game
    """

    three_stars = dict()

    table = soup_game_summary.find('table', id='MainTable')
    table = table.find_all('table')
    s = table[-1].get_text().split('\n')
    three_stars['First'] = [s[3], s[5].split(' ')[0]]
    three_stars['Second'] = [s[9], s[11].split(' ')[0]]
    three_stars['Third'] = [s[15], s[17].split(' ')[0]]

    return three_stars


def scrape_roster(game_id):
    """
    For a given game scrapes the roster
    :param game_id: id for game
    :return: dict of players (home and away), dict of head coaches, dict of officials
    """

    try:
        html = get_html(game_id)
        time.sleep(1)
    except Exception as e:
        print('Roster for game {} is not there'.format(game_id), e)
        raise Exception

    try:
        soup = BeautifulSoup(html.content, 'html.parser')
        players = get_players(soup)
        head_coaches = get_coaches(soup)
        officials = get_officials(soup)
    except Exception as e:
        print('Problem with playing roster for game {}'.format(game_id), e)
        raise Exception

    try:
        game_id = str(game_id)
        url_game_summary = 'http://www.nhl.com/scores/htmlreports/{}{}/GS{}.HTM'.format(game_id[:4],
                                                                                        int(game_id[:4]) + 1,
                                                                                        game_id[4:])
        html_game_summary = get_url(url_game_summary)
        time.sleep(1)
    except Exception as e:
        print('Game Summary for game {} is not there'.format(game_id), e)
        raise Exception

    try:
        soup_game_summary = BeautifulSoup(html_game_summary.content, 'html.parser')
        goalies = get_goalies(soup_game_summary)
        three_stars = get_stars(soup_game_summary)
    except Exception as e:
        print('Problem with game summary for game {}'.format(game_id), e)
        raise Exception

    return players, head_coaches, officials, goalies, three_stars


def parse_roster(game_id):
    """
    :param game_id: id for game
    :return: pandas Dataframes for players, coaches, officials
    """
    players = scrape_roster(game_id)

    # create pandas Dataframes for home & away team's roster for given game, including scratches and captains
    away_roster = pd.DataFrame(players[0][0]['Away'], columns=['Num', 'Pos', 'Name'])
    away_roster['Game_Id'] = game_id
    away_roster['Team'] = players[0][3]['Away']
    scratch = []
    for player in players[0][0]['Away']:
        if player in players[0][1]['Away Scratch']:
            scratch.append(True)
        else:
            scratch.append(np.NaN)
    away_roster['Scratch'] = scratch
    captains = []
    for player in players[0][0]['Away']:
        if player in players[0][2]['Away Captain']:
            captains.append('C')
        elif player in players[0][2]['Away Assistants']:
            captains.append('A')
        else:
            captains.append(np.NaN)
    away_roster['Captain'] = captains

    home_roster = pd.DataFrame(players[0][0]['Home'], columns=['Num', 'Pos', 'Name'])
    home_roster['Game_Id'] = game_id
    home_roster['Team'] = players[0][3]['Home']
    scratch = []
    for player in players[0][0]['Home']:
        if player in players[0][1]['Home Scratch']:
            scratch.append(True)
        else:
            scratch.append(np.NaN)
    home_roster['Scratch'] = scratch
    captains = []
    for player in players[0][0]['Home']:
        if player in players[0][2]['Home Captain']:
            captains.append('C')
        elif player in players[0][2]['Home Assistants']:
            captains.append('A')
        else:
            captains.append(np.NaN)
    home_roster['Captain'] = captains

    goalies = []
    for player in players[0][0]['Away']:
        if player[2] == players[3]['winner']:
            goalies.append('W')
        elif 'loser' in players[3].keys() and player[2] == players[3]['loser']:
            goalies.append('L')
        elif 'overtime' in players[3].keys() and player[2] == players[3]['overtime']:
            goalies.append('OT')
        else:
            goalies.append(np.NaN)
    away_roster['Goalie'] = goalies
    goalies = []
    for player in players[0][0]['Home']:
        if player[2] == players[3]['winner']:
            goalies.append('W')
        elif 'loser' in players[3].keys() and player[2] == players[3]['loser']:
            goalies.append('L')
        elif 'overtime' in players[3].keys() and player[2] == players[3]['overtime']:
            goalies.append('OT')
        else:
            goalies.append(np.NaN)
    home_roster['Goalie'] = goalies

    rosters = pd.concat([home_roster, away_roster], ignore_index=True)

    star = list()
    for player in players[0][0]['Home']:
        if players[4]['First'][0] == players[0][3]['Home'] and players[4]['First'][1] == player[0]:
            star.append(1)
        elif players[4]['Second'][0] == players[0][3]['Home'] and players[4]['Second'][1] == player[0]:
            star.append(2)
        elif players[4]['Third'][0] == players[0][3]['Home'] and players[4]['Third'][1] == player[0]:
            star.append(3)
        else:
            star.append(np.NaN)
    for player in players[0][0]['Away']:
        if players[4]['First'][0] == players[0][3]['Away'] and players[4]['First'][1] == player[0]:
            star.append(1)
        elif players[4]['Second'][0] == players[0][3]['Away'] and players[4]['Second'][1] == player[0]:
            star.append(2)
        elif players[4]['Third'][0] == players[0][3]['Away'] and players[4]['Third'][1] == player[0]:
            star.append(3)
        else:
            star.append(np.NaN)
    rosters['Star'] = star
    # fixes issues with player names
    rosters['Name'] = rosters['Name'].map(lambda x: fix_name(x))

    # create pandas Dataframe of home & away team's coach for given game
    coaches = pd.DataFrame(players[1], index=[0])
    coaches['Game_Id'] = game_id
    coaches['Away'] = players[1]['Away']
    coaches['Home'] = players[1]['Home']

    # create pandas Dataframe of officials for given game
    officials = pd.DataFrame(players[2])
    officials['Game_Id'] = game_id

    return rosters, coaches, officials
