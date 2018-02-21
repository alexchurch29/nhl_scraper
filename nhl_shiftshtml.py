import pandas as pd
from bs4 import BeautifulSoup
import time
import re
from nhl_players import fix_name
from nhl_teams import fix_team
from nhl_main import get_url, convert_to_seconds


def get_teams(soup):
    """
    Return the team for the TOI tables and the home team
    :param soup: souped up html
    :return: list with team and home team
    """

    team = soup.find('td', class_='teamHeading + border')  # Team for shifts
    team = team.get_text()

    # Get Home Team
    teams = soup.find_all('td', {'align': 'center', 'style': 'font-size: 10px;font-weight:bold'})
    regex = re.compile(r'>(.*)<br/>')
    home_team = regex.findall(str(teams[7]))

    return [team, home_team[0]]


def analyze_shifts(shift, name, team):
    """
    Analyze shifts for each player when using.
    Prior to this each player (in a dictionary) has a list with each entry being a shift.
    This function is only used for the html
    :param shift: info on shift
    :param name: player name
    :param team: given team
    :return: dict with info for shift
    """
    shifts = dict()

    shifts['Player'] = name.upper()
    shifts['Period'] = '4' if shift[1] == 'OT' else shift[1]
    shifts['Team'] = fix_team(team.strip(' '))
    shifts['Shift'] = shift[0]
    shifts['Start'] = convert_to_seconds(shift[2].split('/')[0])
    shifts['End'] = convert_to_seconds(shift[3].split('/')[0])
    shifts['Duration'] = convert_to_seconds(shift[4].split('/')[0])

    return shifts


def get_shifts(game_id):
    """
    Given a game_id it returns a DataFrame with the shifts for both teams
    Ex: http://www.nhl.com/scores/htmlreports/20162017/TV020971.HTM
    :param game_id: the game
    :return: DataFrame with all shifts, return None when an exception is thrown when parsing
    """
    game_id = str(game_id)
    home_url = 'http://www.nhl.com/scores/htmlreports/{}{}/TH{}.HTM'.format(game_id[:4], int(game_id[:4])+1, game_id[4:])
    away_url = 'http://www.nhl.com/scores/htmlreports/{}{}/TV{}.HTM'.format(game_id[:4], int(game_id[:4])+1, game_id[4:])

    home = get_url(home_url)
    time.sleep(1)

    away = get_url(away_url)
    time.sleep(1)

    return home, away


def parse_html(html, game_id):
    """
    Parse the html
    :param html: cleaned up html
    :param game_id: id for game
    :return: DataFrame with info
    """
    columns = ['Game_Id', 'Player', 'Period', 'Team', 'Shift', 'Start', 'End', 'Duration']
    df = pd.DataFrame(columns=columns)

    soup = BeautifulSoup(html.content, "lxml")

    teams = get_teams(soup)
    team = teams[0]
    # home_team = teams[1]

    td = soup.findAll(True, {'class': ['playerHeading + border', 'lborder + bborder']})

    """
    The list 'td' is laid out with player name followed by every component of each shift. Each shift contains: 
    shift #, Period, begin, end, and duration. The shift event isn't included. 
    """
    players = dict()
    for t in td:
        t = t.get_text()
        if ',' in t:     # If it has a comma in it we know it's a player's name...so add player to dict
            name = t
            # Just format the name normally...it's coded as: 'num last_name, first_name'
            name = name.split(',')
            name = ' '.join([name[1].strip(' '), name[0][2:].strip(' ')])
            name = fix_name(name)
            players[name] = dict()
            players[name]['number'] = name[0][:2].strip()
            players[name]['Shifts'] = []
        else:
            # Here we add all the shifts to whatever player we are up to
            players[name]['Shifts'].extend([t])

    for key in players.keys():
        # Create a list of lists (each length 5)...corresponds to 5 columns in html shifts
        players[key]['Shifts'] = [players[key]['Shifts'][i:i + 5] for i in range(0, len(players[key]['Shifts']), 5)]

        shifts = [analyze_shifts(shift, key, team) for shift in players[key]['Shifts']]
        df = df.append(shifts, ignore_index=True)

    df['Game_Id'] = str(game_id)
    return df


def scrape_game(game_id):
    """
    Scrape the game.
    :param game_id: id for game
    :return: DataFrame with info for the game
    """
    home_html, away_html = get_shifts(game_id)

    away_df = parse_html(away_html, game_id)
    home_df = parse_html(home_html, game_id)

    game_df = pd.concat([away_df, home_df], ignore_index=True)

    game_df = game_df.sort_values(by=['Period', 'Start'], ascending=[True, True])  # Sort by period and by time
    game_df = game_df.reset_index(drop=True)

    # create second df to record list of players on ice at every second of the game
    # this will be used to track ice time by game state
    final_period = int(game_df['Period'].iloc[-1]) # returns final period of game
    final_play = int(game_df['End'].iloc[-1]) # returns time of final play of game
    # creates dict where each key represents a second in time for the given game
    shifts_by_sec = {i: [] for i in range(1, (((final_period - 1) * 1200) + final_play + 1))}

    # loop through each shift and return which players are on the ice at any given second in time
    for index, row in game_df.iterrows():
        for k, v in shifts_by_sec.items():
            if (row['Start'] + (1200 * (int(row['Period']) - 1))+1) <= k <= (row['End'] + (1200 * (int(row['Period']) - 1))):
                v.append(row['Player'])
                v.append(row['Team'])

    players_on_ice = pd.DataFrame.from_dict(shifts_by_sec, orient='index')
    players_on_ice['Game_Id'] = str(game_id)
    players_on_ice['Time'] = players_on_ice.index
    columns=['Game_Id', 'Time', 'P1_Name', 'P1_Team', 'P2_Name', 'P2_Team', 'P3_Name', 'P3_Team', 'P4_Name', 'P4_Team',
             'P5_Name', 'P5_Team', 'P6_Name', 'P6_Team', 'P7_Name', 'P7_Team', 'P8_Name', 'P8_Team', 'P9_Name',
             'P9_Team', 'P10_Name', 'P10_Team', 'P11_Name', 'P11_Team', 'P12_Name', 'P12_Team', 'P13_Name', 'P13_Team']
    players_on_ice.reindex_axis(columns, axis=1)

    return game_df, players_on_ice
