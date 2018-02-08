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
    (assist1.A1 + assist2.A2) as A,  (goal.G + (assist1.A1 + assist2.A2)) as P, (goal.G + assist1.A1) as P1,
    (goal.G + shots.SH) as SH, (round(goal.G,2)  / (round(shots.SH,2) + round(goal.G,2))) as 'SH%', missed.MISS as 
    MISS, blocked.BLOCK as BLOCKED, (shots.SH + missed.MISS + blocked.BLOCK) as iCF, (shots.SH + missed.MISS) as iFF, 
    pim.PIM, penl.PENL_Taken, min.Minor, maj.Major, misc.Misconduct, pend.PENL_Drawn, give.Giveaways, take.Takeaways,
    hit.hits, hittaken.Hits_Taken, blocksfor.Shot_Blocks, faceoffwon.Faceoffs_Won, faceofflost.Faceoffs_Lost, 
    (round(faceoffwon.Faceoffs_Won, 2) / (round(faceoffwon.Faceoffs_Won, 2) + round(faceofflost.Faceoffs_Lost, 2))) 
    as 'Faceoff%'

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

LEFT OUTER JOIN (SELECT name, count(name) as SH
    FROM (SELECT r.name, p.p1_team, p.p1_num
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p1_team
        and r.num=p.p1_num)
    WHERE p.event_type = 'SHOT'
        and p.period != 5)
    GROUP BY name) as shots
ON shots.name = r.name

LEFT OUTER JOIN (SELECT name, count(name) as MISS
    FROM (SELECT r.name, p.p1_team, p.p1_num
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p1_team
        and r.num=p.p1_num)
    WHERE p.event_type = 'MISS'
        and p.period != 5)
    GROUP BY name) as missed
ON missed.name = r.name

LEFT OUTER JOIN (SELECT name, count(name) as BLOCK
    FROM (SELECT r.name, p.p2_team, p.p2_num
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p2_team
        and r.num=p.p2_num)
    WHERE p.event_type = 'BLOCK'
        and p.period != 5)
    GROUP BY name) as blocked
ON blocked.name = r.name

LEFT OUTER JOIN (SELECT name, sum(penl_length) as PIM
    FROM (SELECT r.name, p.p1_team, p.p1_num, p.penl_length
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p1_team
        and r.num=p.p1_num)
    WHERE p.event_type = 'PENL'
        and p.period != 5)
    GROUP BY name) as pim
ON pim.name = r.name

LEFT OUTER JOIN (SELECT name, count(name) as PENL_Taken
    FROM (SELECT r.name, p.p1_team, p.p1_num
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p1_team
        and r.num=p.p1_num)
    WHERE p.event_type = 'PENL'
        and p.period != 5)
    GROUP BY name) as penl
ON penl.name = r.name

LEFT OUTER JOIN (SELECT name, count(name) as Minor
    FROM (SELECT r.name, p.p1_team, p.p1_num
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p1_team
        and r.num=p.p1_num)
    WHERE p.event_type = 'PENL'
        and p.penl_length = 2
        and p.period != 5)
    GROUP BY name) as min
ON min.name = r.name

LEFT OUTER JOIN (SELECT name, count(name) as Major
    FROM (SELECT r.name, p.p1_team, p.p1_num
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p1_team
        and r.num=p.p1_num)
    WHERE p.event_type = 'PENL'
        and p.penl_length = 5
        and p.period != 5)
    GROUP BY name) as maj
ON maj.name = r.name

LEFT OUTER JOIN (SELECT name, count(name) as Misconduct
    FROM (SELECT r.name, p.p1_team, p.p1_num
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p1_team
        and r.num=p.p1_num)
    WHERE p.event_type = 'PENL'
        and p.penl_length = 10
        and p.period != 5)
    GROUP BY name) as misc
ON misc.name = r.name

LEFT OUTER JOIN (SELECT name, count(name) as PENL_Drawn
    FROM (SELECT r.name, p.p2_team, p.p2_num
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p2_team
        and r.num=p.p2_num)
    WHERE p.event_type = 'PENL'
        and p.period != 5)
    GROUP BY name) as pend
ON pend.name = r.name

LEFT OUTER JOIN (SELECT name, count(name) as Giveaways
    FROM (SELECT r.name, p.p1_team, p.p1_num
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p1_team
        and r.num=p.p1_num)
    WHERE p.event_type = 'GIVE'
        and p.period != 5)
    GROUP BY name) as give
ON give.name = r.name

LEFT OUTER JOIN (SELECT name, count(name) as Takeaways
    FROM (SELECT r.name, p.p1_team, p.p1_num
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p1_team
        and r.num=p.p1_num)
    WHERE p.event_type = 'TAKE'
        and p.period != 5)
    GROUP BY name) as take
ON take.name = r.name

LEFT OUTER JOIN (SELECT name, count(name) as Hits
    FROM (SELECT r.name, p.p1_team, p.p1_num
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p1_team
        and r.num=p.p1_num)
    WHERE p.event_type = 'HIT'
        and p.period != 5)
    GROUP BY name) as hit
ON hit.name = r.name

LEFT OUTER JOIN (SELECT name, count(name) as Hits_Taken
    FROM (SELECT r.name, p.p2_team, p.p2_num
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p2_team
        and r.num=p.p2_num)
    WHERE p.event_type = 'HIT'
        and p.period != 5)
    GROUP BY name) as hittaken
ON hittaken.name = r.name

LEFT OUTER JOIN (SELECT name, count(name) as Shot_Blocks
    FROM (SELECT r.name, p.p1_team, p.p1_num
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p1_team
        and r.num=p.p1_num)
    WHERE p.event_type = 'BLOCK'
        and p.period != 5)
    GROUP BY name) as blocksfor
ON blocksfor.name = r.name

LEFT OUTER JOIN (SELECT name, count(name) as Faceoffs_Won
    FROM (SELECT r.name, p.p1_team, p.p1_num
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p1_team
        and r.num=p.p1_num)
    WHERE p.event_type = 'FAC'
        and p.period != 5)
    GROUP BY name) as faceoffwon
ON faceoffwon.name = r.name

LEFT OUTER JOIN (SELECT name, count(name) as Faceoffs_Lost
    FROM (SELECT r.name, p.p2_team, p.p2_num
    FROM pbp p
    INNER JOIN rosters r
    ON (r.game_id = p.game_id
        and r.team=p.p2_team
        and r.num=p.p2_num)
    WHERE p.event_type = 'FAC'
        and p.period != 5)
    GROUP BY name) as faceofflost
ON faceofflost.name = r.name
    
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

print(skaters_all_strengths)

