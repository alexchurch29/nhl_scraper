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
# NAME, SEASON**, TEAM*, POS, GP, TOI
# to aggregate for multi player teams add r.team to GROUP BY
# to aggregate for multiple seasons remove z.season from GROUP BY
skaters = pd.read_sql_query ('''
SELECT r.name, z.season, r.team, r.pos, COUNT(r.name) as GP, s.TOI
FROM rosters r 
INNER JOIN schedule z 
ON r.game_id = z.game_id
INNER JOIN (SELECT player, ROUND(SUM(duration)/60,2) as TOI
    FROM shifts
    GROUP BY player) as s
ON s.player = r.name
WHERE r.pos != 'G' 
    AND r.scratch is null
GROUP BY z.season, r.name, r.pos
ORDER BY r.name, z.season;''', conn)



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
