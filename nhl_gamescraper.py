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

    return schedule, rosters, shifts, pbp, coaches, officials

scrape_games("2017-10-07", "2017-12-23")
