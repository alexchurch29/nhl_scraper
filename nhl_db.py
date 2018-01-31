import sqlite3
import pandas as pd

conn = sqlite3.connect('nhl.db')
cur = conn.cursor()

def update():
    # read in updated pandas dataframes from most recent scrape
    r = pd.read_pickle('rosters.pickle')
    s = pd.read_pickle('shifts.pickle')
    p = pd.read_pickle('pbp.pickle')
    c = pd.read_pickle('coaches.pickle')
    o = pd.read_pickle('officials.pickle')
    z = pd.read_pickle('schedule.pickle')

    # update existing tables in database
    r.to_sql('rosters', conn, if_exists='replace')
    s.to_sql('shifts', conn, if_exists='replace')
    p.to_sql('pbp', conn, if_exists='replace')
    c.to_sql('coaches', conn, if_exists='replace')
    o.to_sql('officials', conn, if_exists='replace')
    z.to_sql('schedule', conn, if_exists='replace')

# creates full list of skaters
# to aggregate for multi player teams add r.team to GROUP BY
skaters = pd.read_sql_query ('''
SELECT r.name, r.pos, r.team
FROM rosters r INNER JOIN shifts s 
ON r.name = s.player
WHERE pos != 'G' 
    AND s.duration > 0
GROUP BY r.pos, r.name
ORDER BY r.name;''', conn)







# goals by player all strengths
#goals = pd.read_sql_query('''
#SELECT r.NAME, COUNT(*) AS 'Goals'
#FROM pbp p INNER JOIN rosters r
#ON (r.GAME_ID = p.GAME_ID
#	AND r.NUM = p.P1_NUM
#	AND r.TEAM = p.P1_TEAM)
#WHERE (p.EVENT_TYPE = 'GOAL'
#	AND p.PERIOD != '5')
#ORDER BY goals;''', conn)


# full list of goalies
#goalies = pd.read_sql_query('''
#SELECT DISTINCT r.name, pos
#FROM rosters r INNER JOIN shifts s
#ON r.name = s.player
#WHERE r.pos = 'G'
#	AND r.scratch != 'True'
#	AND s.duration > 0
#ORDER BY r.name;''', conn)

print(skaters)