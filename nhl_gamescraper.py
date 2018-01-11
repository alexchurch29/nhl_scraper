from nhl_rosterhtml import parse_roster as playing_roster
from nhl_shiftshtml import scrape_game as html_shifts
from nhl_pbphtml import parse_pbp as html_pbp
from nhl_schedule import scrape_schedule as html_schedule
import pandas as pd

broken_shifts_games = []
broken_pbp_games = []
broken_roster_games = []


def scrape_games(start_date, end_date):
    games = list()
    schedule = html_schedule(start_date, end_date)
    for index, row in schedule.iterrows():
        games.append(row["Game_Id"])

    rosters = list()
    coaches = list()
    officials = list()
    shifts = list()
    pbp = list()

    for game in games:
        try:
            rosters.append(playing_roster(game)[0])
            coaches.append(playing_roster(game)[1])
            officials.append(playing_roster(game)[2])
        except Exception as e:
            print('Error for rosters for game {}'.format(game), e)
            broken_roster_games.append(game)

        try:
            shifts.append(html_shifts(game))
        except Exception as e:
            print('Error for shifts for game {}'.format(game), e)
            broken_shifts_games.append(game)

        try:
            pbp.append(html_pbp(game))
        except Exception as e:
            print('Error for pbp for game {}'.format(game), e)
            broken_pbp_games.append(game)

    print(broken_pbp_games)
    print(broken_shifts_games)
    print(broken_roster_games)

    rosters = pd.concat(rosters, ignore_index=True)
    shifts = pd.concat(shifts, ignore_index=True)
    pbp = pd.concat(pbp, ignore_index=True)
    coaches = pd.concat(coaches, ignore_index=True)
    officials = pd.concat(officials, ignore_index=True)

    return schedule, rosters, shifts, pbp, coaches, officials


def games_to_csv(start_date, end_date):
    games = scrape_games(start_date, end_date)
    games[0].to_csv("schedule")
    games[1].to_csv("rosters")
    games[2].to_csv("shifts")
    games[3].to_csv("pbp")
    games[4].to_csv("coaches")
    games[5].to_csv("officials")
