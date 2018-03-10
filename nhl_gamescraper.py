import pandas as pd
from nhl_rosterhtml import parse_roster as playing_roster
from nhl_shiftshtml import scrape_game as html_shifts
from nhl_pbphtml import parse_pbp as html_pbp
from nhl_schedule import scrape_schedule as html_schedule


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
    coaches = []
    officials = []
    shifts = []
    pbp = []

    # track issues scraping certain games
    broken_shifts_games = []
    broken_pbp_games = []
    broken_roster_games = []

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
            shifts.append(html_shifts(game)[0])
        except Exception as e:
            print('Error for shifts for game {}'.format(game), e)
            broken_shifts_games.append(game)

        try:
            pbp.append(html_pbp(game))
        except Exception as e:
            print('Error for pbp for game {}'.format(game), e)
            broken_pbp_games.append(game)

    # add all newly scraped games to appropriate dataframe
    rosters = pd.concat(rosters, ignore_index=True)
    shifts = pd.concat(shifts, ignore_index=True)
    pbp = pd.concat(pbp, ignore_index=True)
    coaches = pd.concat(coaches, ignore_index=True)
    officials = pd.concat(officials, ignore_index=True)

    broken_games = dict()
    broken_games['broken pbp'] = broken_pbp_games
    broken_games['broken shifts'] = broken_shifts_games
    broken_games['broken roster'] = broken_roster_games
    print(broken_games)

    return rosters, shifts, pbp, coaches, officials, schedule


def scrape_games_by_id(games):
    """
    Scrapes games based on list of individual game id's. Can be used to pickup missing games.
    :param games: list of game ids to scrape
    :return: pd dataframe of rosters, shifts, pbp events, coaches, and officials for all game id's provided
    """

    rosters = list()
    coaches = list()
    officials = list()
    shifts = list()
    pbp = list()

    broken_shifts_games = []
    broken_pbp_games = []
    broken_roster_games = []

    for game in games:
        try:
            rosters.append(playing_roster(game)[0])
            coaches.append(playing_roster(game)[1])
            officials.append(playing_roster(game)[2])
        except Exception as e:
            print('Error for rosters for game {}'.format(game), e)
            broken_roster_games.append(game)

        try:
            shifts.append(html_shifts(game)[0])
        except Exception as e:
            print('Error for shifts for game {}'.format(game), e)
            broken_shifts_games.append(game)

        try:
            pbp.append(html_pbp(game))
        except Exception as e:
            print('Error for pbp for game {}'.format(game), e)
            broken_pbp_games.append(game)

    rosters = pd.concat(rosters, ignore_index=True)
    shifts = pd.concat(shifts, ignore_index=True)
    pbp = pd.concat(pbp, ignore_index=True)
    coaches = pd.concat(coaches, ignore_index=True)
    officials = pd.concat(officials, ignore_index=True)

    broken_games = dict()
    broken_games['broken pbp'] = broken_pbp_games
    broken_games['broken shifts'] = broken_shifts_games
    broken_games['broken roster'] = broken_roster_games

    return rosters, shifts, pbp, coaches, officials
