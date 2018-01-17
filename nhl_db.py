import sqlite3
import pandas as pd

conn = sqlite3.connect('nhl.db')
cur = conn.cursor()

# full list of skaters # be mindful of sebastian aho(s)
skaters = pd.read_sql_query('''
SELECT DISTINCT name, pos
FROM rosters
WHERE pos != 'G' AND scratch != 'True';''', conn)

# full list of goalies
goalies = pd.read_sql_query('''
SELECT DISTINCT r.name, pos
FROM rosters r INNER JOIN shifts s
ON r.name = s.player
WHERE r.pos = 'G' 
	AND r.scratch != 'True'
	AND s.duration > 0;''', conn)

print(skaters)
print(goalies)

'''
# goals by player all strengths
SELECT r.NAME, COUNT(*) AS 'GOALS' 
FROM pbp p INNER JOIN rosters r
ON (r.GAME_ID = p.GAME_ID
	AND r.NUM = p.P1_NUM
	AND r.TEAM = p.P1_TEAM)
WHERE (p.EVENT_TYPE = 'GOAL'
	AND p.P1_NUM = '29'
	AND p.P1_TEAM = 'WPG'
	AND p.PERIOD != '5')
'''