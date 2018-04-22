import pandas as pd
import numpy as np
from nhl_schedule import scrape_schedule as html_schedule
from nhl_rosterhtml import parse_roster as playing_roster
from nhl_rosterjson import scrape_game as player_bios
from nhl_shiftshtml import scrape_game as html_shifts
from nhl_pbphtml import parse_pbp as html_pbp
from nhl_pbphtml import get_coords as nhl_coordinates
from espn_coordinates import scrape_game as espn_coordinates


def scrape_games_by_date(start_date, end_date):
    """
    Scrapes all games for given date range
    :param start_date: start date
    :param end_date: end date
    :return: pd dataframe of rosters, shifts, pbp events, coaches, and officials for all games in given date range
    """

    # create list of game id's for date range provided
    schedule = html_schedule(start_date, end_date)
    games = [row['Game_Id'] for index, row in schedule.iterrows() if row['Game_State'] == 'Final']

    rosters = []
    bios = []
    coaches = []
    officials = []
    shifts = []
    pbp = []

    # track issues scraping certain games
    broken_shifts_games = []
    broken_pbp_games = []
    broken_roster_games = []
    broken_bio_games = []

    # iterate through new games to scrape and append those dataframes to appropriate list
    for game in games:
        try:
            rosters.append(playing_roster(game)[0])
            coaches.append(playing_roster(game)[1])
            officials.append(playing_roster(game)[2])
        except Exception as e:
            print('Error for rosters for game {}'.format(game), e)
            broken_roster_games.append(game)

        try:
            bios.append(player_bios(game))
        except Exception as e:
            print('Error for bios for game {}'.format(game), e)
            broken_bio_games.append(game)

        try:
            shifts.append(html_shifts(game))
        except Exception as e:
            print('Error for shifts for game {}'.format(game), e)
            broken_shifts_games.append(game)

        try:
            p = html_pbp(game)
        except Exception as e:
            print('Error for pbp for game {}'.format(game), e)
            broken_pbp_games.append(game)

        date = schedule.loc[schedule['Game_Id'] == game, 'Date'].iloc[0]
        home = schedule.loc[schedule['Game_Id'] == game, 'Home_Team'].iloc[0]
        away = schedule.loc[schedule['Game_Id'] == game, 'Away_Team'].iloc[0]
        coords = espn_coordinates(date, home, away)
        if coords is not None:
            coords['Game_Id'] = game
            p = pd.merge(left=p, right=coords, how='left', on=['Game_Id', 'Period', 'Time_Elapsed', 'Event_Type'])
            p = p.drop_duplicates(subset=['Game_Id', 'Event_Num'])
            pbp.append(p)
        elif coords is None:  # if espn coordinates are broken we use the json pbp to pick up the slack
            try:
                p['xC'] = nhl_coordinates(game)['xC']
                p['yC'] = nhl_coordinates(game)['yC']
                pbp.append(p)
            except Exception as e:
                print('Error with json coordinates for game {}'.format(game), e)
                p['xC'] = np.NaN
                p['yC'] = np.NaN
                pbp.append(p)

    # add all newly scraped games to appropriate dataframe
    rosters = pd.concat(rosters, ignore_index=True)
    shifts = pd.concat(shifts, ignore_index=True)
    pbp = pd.concat(pbp, ignore_index=True)
    bios = pd.concat(bios, ignore_index=True)
    coaches = pd.concat(coaches, ignore_index=True)
    officials = pd.concat(officials, ignore_index=True)

    broken_games = dict()
    broken_games['broken pbp'] = broken_pbp_games
    broken_games['broken bios'] = broken_bio_games
    broken_games['broken shifts'] = broken_shifts_games
    broken_games['broken roster'] = broken_roster_games
    print(broken_games)

    return rosters, shifts, pbp, coaches, officials, schedule, bios


def scrape_games_by_id(games, start_date, end_date):
    """
    Scrapes all games by id
    :param games: list of games
    :param start_date: date of earliest game
    :param end_date: date of latest game
    :return: pd dataframe of rosters, shifts, pbp events, coaches, and officials for all games in given date range
    """

    # need schedule for espn coordinates
    schedule = html_schedule(start_date, end_date)

    rosters = []
    bios = []
    coaches = []
    officials = []
    shifts = []
    pbp = []

    # track issues scraping certain games
    broken_shifts_games = []
    broken_pbp_games = []
    broken_roster_games = []
    broken_bio_games = []

    # iterate through new games to scrape and append those dataframes to appropriate list
    for game in games:
        try:
            rosters.append(playing_roster(game)[0])
            coaches.append(playing_roster(game)[1])
            officials.append(playing_roster(game)[2])
        except Exception as e:
            print('Error for rosters for game {}'.format(game), e)
            broken_roster_games.append(game)

        try:
            bios.append(player_bios(game))
        except Exception as e:
            print('Error for bios for game {}'.format(game), e)
            broken_bio_games.append(game)

        try:
            shifts.append(html_shifts(game))
        except Exception as e:
            print('Error for shifts for game {}'.format(game), e)
            broken_shifts_games.append(game)

        try:
            p = html_pbp(game)
        except Exception as e:
            print('Error for pbp for game {}'.format(game), e)
            broken_pbp_games.append(game)

        date = schedule.loc[schedule['Game_Id'] == game, 'Date'].iloc[0]
        home = schedule.loc[schedule['Game_Id'] == game, 'Home_Team'].iloc[0]
        away = schedule.loc[schedule['Game_Id'] == game, 'Away_Team'].iloc[0]
        coords = espn_coordinates(date, home, away)
        if coords is not None:
            coords['Game_Id'] = game
            p = pd.merge(left=p, right=coords, how='left', on=['Game_Id', 'Period', 'Time_Elapsed', 'Event_Type'])
            p = p.drop_duplicates(subset=['Game_Id', 'Event_Num'])
            pbp.append(p)
        elif coords is None:  # if espn coordinates are broken we use the json pbp to pick up the slack
            try:
                p['xC'] = nhl_coordinates(game)['xC']
                p['yC'] = nhl_coordinates(game)['yC']
                pbp.append(p)
            except Exception as e:
                print('Error with json coordinates for game {}'.format(game), e)
                p['xC'] = np.NaN
                p['yC'] = np.NaN
                pbp.append(p)

    # add all newly scraped games to appropriate dataframe
    rosters = pd.concat(rosters, ignore_index=True)
    shifts = pd.concat(shifts, ignore_index=True)
    pbp = pd.concat(pbp, ignore_index=True)
    bios = pd.concat(bios, ignore_index=True)
    coaches = pd.concat(coaches, ignore_index=True)
    officials = pd.concat(officials, ignore_index=True)

    broken_games = dict()
    broken_games['broken pbp'] = broken_pbp_games
    broken_games['broken bios'] = broken_bio_games
    broken_games['broken shifts'] = broken_shifts_games
    broken_games['broken roster'] = broken_roster_games
    print(broken_games)

    return rosters, shifts, pbp, coaches, officials, bios
