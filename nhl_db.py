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
# NAME, SEASON**, TEAM*, POS, GP, TOI, G
# to aggregate for multi player teams add r.team to GROUP BY
# to aggregate for multiple seasons remove z.season from GROUP BY
skaters_all_strengths = pd.read_sql_query ('''
SELECT r.name, z.season, r.team, r.pos, COUNT(r.name) as GP, icetime.TOI, goal.G, assist1.A1, assist2.A2, 
    (assist1.A1 + assist2.A2) as A,  (goal.G + (assist1.A1 + assist2.A2)) as P, (goal.G + assist1.A1) as P1

FROM rosters r 

INNER JOIN schedule z 
ON r.game_id = z.game_id

INNER JOIN (SELECT player, ROUND(SUM(duration)/60,2) as TOI
    FROM shifts
    GROUP BY player) as icetime
ON icetime.player = r.name

LEFT OUTER JOIN (SELECT name, count(name) as G
    FROM (SELECT r.name, p.p1_team, p.p1_num
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p1_team
        and r.num=p.p1_num)
    WHERE p.event_type = 'GOAL'
        and p.period != 5)
    GROUP BY name) as goal
ON goal.name = r.name

LEFT OUTER JOIN (SELECT name, count(name) as A1
    FROM (SELECT r.name, p.p2_team, p.p2_num
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p2_team
        and r.num=p.p2_num)
    WHERE p.event_type = 'GOAL'
        and p.period != 5)
    GROUP BY name) as assist1
ON assist1.name = r.name

LEFT OUTER JOIN (SELECT name, count(name) as A2
    FROM (SELECT r.name, p.p3_team, p.p3_num
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p3_team
        and r.num=p.p3_num)
    WHERE p.event_type = 'GOAL'
        and p.period != 5)
    GROUP BY name) as assist2
ON assist2.name = r.name
    
WHERE r.pos != 'G' 
    AND r.scratch is null
    
GROUP BY z.season, r.name, r.pos

ORDER BY P DESC, r.name;''', conn)



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
