import json
import time
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from nhl_main import get_url
from nhl_teams import fix_team
from nhl_rosterhtml import get_html, get_gameinfo

'''
Playoffs
2015030412 = game code, change this to access different games
2015       = season code, first year of the season (e.g., 2015 is for the 2015-16 seasons)
    03     = game type code; 1 = preseason, 2 = regular season; 3 = playoffs
      04   = playoff only: round number (1st round = 1, 2nd round = 2, ECF/WCF = 3, SCF = 4)
        1  = series number: 1-8 in round 1, 1-4 in round 2, 1-2 in round 3,1 in round 4
         2 = game number: 1-7 for any given series
         
Regular Season
2015020807 = game code, change this to access different games
2015       = season code, first year of the season (e.g., 2015 is for the 2015-16 seasons)
    02     = game type code; 1 = preseason, 2 = regular season; 3 = playoffs
      0807   = game ID; generally 1-1230 in a normal regular season, but sometimes games will be missing 
      (e.g., games cancelled due to weather) and sometimes games will be added on the end, starting with 1231 
      (e.g., make-up games for weather-cancelled games). Numbers are usually approx. 1-130ish in the pre-season, but it can be arbitrary.
'''


def get_schedule(date_from, date_to):
    """
    Scrapes all games in given date range
    e.g. https://statsapi.web.nhl.com/api/v1/schedule?startDate=2010-10-03&endDate=2011-06-20
    :param date_from: scrape from this date
    :param date_to: scrape up to this date
    :return: raw json of NHL schedule for given date range
    """
    url = 'https://statsapi.web.nhl.com/api/v1/schedule?startDate={a}&endDate={b}'.format(a=date_from, b=date_to)

    response = get_url(url)
    time.sleep(1)

    schedule_json = json.loads(response.text)

    return schedule_json


def scrape_schedule(date_from, date_to):
    """
    Calls getSchedule and scrapes the raw schedule JSON
    :param date_from: scrape from this date e.g. '2010-10-03'
    :param date_to: scrape until this date e.g. '2011-06-20'
    :return: pd DF with game data for given date range
    """
    games = []

    schedule_json = get_schedule(date_from, date_to)

    for day in schedule_json['dates']:
        for game in day['games']:
            if 20000 <= int(str(game['gamePk'])[5:]) < 40000: # do not include pre season or all star games
                schedule = dict()
                schedule['Date'] = day['date']
                schedule['Game_Id'] = game['gamePk']
                if game['gameType'] == 'R':
                    schedule['Game_Type'] = 'Regular Season'
                elif game['gameType'] == 'P':
                    schedule['Game_Type'] = 'Playoff'
                    schedule['Round'] = int(str(game['gamePk'])[7])
                    schedule['Series'] = int(str(game['gamePk'])[8])
                    schedule['Game'] = int(str(game['gamePk'])[9])
                schedule['Season'] = game['season']
                schedule['Game_State'] = game['status']['detailedState']

                schedule['Away_Team'] = fix_team(game['teams']['away']['team']['name'].upper())
                schedule['Away_Team_Id'] = game['teams']['away']['team']['id']
                schedule['Away_Score'] = game['teams']['away']['score']
                schedule['Away_Wins'] = game['teams']['away']['leagueRecord']['wins']
                schedule['Away_Losses'] = game['teams']['away']['leagueRecord']['losses']
                if 'ot' in game['teams']['away']['leagueRecord']:
                    schedule['Away_OT'] = game['teams']['away']['leagueRecord']['ot']

                schedule['Home_Team'] = fix_team(game['teams']['home']['team']['name'].upper())
                schedule['Home_Team_Id'] = game['teams']['home']['team']['id']
                schedule['Home_Score'] = game['teams']['home']['score']
                schedule['Home_Wins'] = game['teams']['home']['leagueRecord']['wins']
                schedule['Home_Losses'] = game['teams']['home']['leagueRecord']['losses']
                if 'ot' in game['teams']['home']['leagueRecord']:
                    schedule['Home_OT'] = game['teams']['home']['leagueRecord']['ot']

                schedule['Venue'] = game['venue']['name']

                try:
                    html = get_html(game['gamePk'])
                    time.sleep(1)
                    soup = BeautifulSoup(html.content, 'html.parser')
                    schedule['Attendance'] = get_gameinfo(soup)['Attendance']
                    schedule['Start'] = get_gameinfo(soup)['Start']
                    schedule['End'] = get_gameinfo(soup)['End']
                    schedule['Timezone'] = get_gameinfo(soup)['TZ']
                except:
                    schedule['Attendance'] = np.NaN
                    schedule['Start'] = np.NaN
                    schedule['End'] = np.NaN
                    schedule['Timezone'] = np.NaN

                games.append(schedule)

    columns = ['Game_Id', 'Season', 'Date', 'Game_Type', 'Round', 'Series', 'Game', 'Game_State', 'Home_Team_Id',
               'Home_Team', 'Away_Team_Id', 'Away_Team', 'Home_Score', 'Away_Score', 'Home_Wins', 'Home_Losses',
               'Home_OT', 'Away_Wins', 'Away_Losses', 'Away_OT', 'Venue', 'Attendance', 'Start', 'End', 'Timezone']

    games = pd.DataFrame(games, columns=columns)

    return games
