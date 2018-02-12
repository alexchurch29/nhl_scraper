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
skaters_individual_counts = pd.read_sql_query ('''
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


skater_on_ice_counts = pd.read_sql_query('''
SELECT r.name, z.season, r.team, r.pos, COUNT(r.name) as GP, icetime.TOI, cf.CF, ca.CA, (round(cf.CF,2)/((round(cf.CF,2)
+round(ca.CA,2))) as 'CF%', ff.FF, fa.FA, (round(ff.FF,2)/((round(ff.FF,2)+round(fa.FA,2))) as 'FF%', sf.SF, sa.SA, 
(round(sf.SF,2)/((round(sf.SF,2)+round(sa.SA,2))) as 'SF%', gf.GF, ga.GA, (round(gf.GF,2)/((round(gf.GF,2)
+round(ga.GA,2))) as 'GF%'

FROM rosters r 

INNER JOIN schedule z 
ON r.game_id = z.game_id

INNER JOIN (SELECT player, ROUND(SUM(duration)/60,2) as TOI
    FROM shifts
    GROUP BY player) as icetime
ON icetime.player = r.name

LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, COUNT(p.name) as CF
    FROM
        (SELECT p.game_id, event_type, p1_team, p2_team, p.name as name, p.pos as pos, p.team as team
        
        FROM 
            (SELECT p.game_id, event_type, p1_team, p2_team, p.name, r.Team as team, p.pos
            
            FROM
                (SELECT game_id, event_type, p1_team, p2_team, H1Name as name, H1Num as num, H1Pos as pos
                
                FROM pbp
                
                WHERE H1Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p2_team, H2Name, H2Num, H2Pos
                FROM pbp
                WHERE H2Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p2_team, H3Name, H3Num, H3Pos
                FROM pbp
                WHERE H3Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p2_team, H4Name, H4Num, H4Pos
                FROM pbp
                WHERE H4Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p2_team, H5Name, H5Num, H5Pos
                FROM pbp
                WHERE H5Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p2_team, H6Name, H6Num, H6Pos
                FROM pbp
                WHERE H6Pos != 'G'
                UNION ALL
                
                SELECT game_id, event_type, p1_team, p2_team, A1Name, A1Num, A1Pos
                FROM pbp
                WHERE A1Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p2_team, A2Name, A2Num, A2Pos
                FROM pbp
                WHERE A2Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p2_team, A3Name, A3Num, A3Pos
                FROM pbp
                WHERE A3Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p2_team, A4Name, A4Num, A4Pos
                FROM pbp
                WHERE A4Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p2_team, A5Name, A5Num, A5Pos
                FROM pbp
                WHERE A5Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p2_team, A6Name, A6Num, A6Pos
                FROM pbp
                WHERE A6Pos != 'G'
                ) as p
                
            INNER JOIN rosters r 
            ON (r.game_id=p.game_id
                and r.name=p.name
                and r.num=p.num)
            ) as p
            
        WHERE (event_type='GOAL' and p1_team=p.team)
            or (event_type='MISS' and p1_team=p.team)
            or (event_type='SHOT' and p1_team=p.team)
            or (event_type='BLOCK' and p2_team=p.team)
        ) as p
        
    GROUP BY p.name) as cf
ON CF.name=r.name

LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, COUNT(p.name) as CA
    FROM
        (SELECT p.game_id, event_type, p1_team, p4_team, p.name as name, p.pos as pos, p.team as team
        
        FROM 
            (SELECT p.game_id, event_type, p1_team, p4_team, p.name, r.Team as team, p.pos
            
            FROM
                (SELECT game_id, event_type, p1_team, p4_team, H1Name as name, H1Num as num, H1Pos as pos
                
                FROM pbp
                
                WHERE H1Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p4_team, H2Name, H2Num, H2Pos
                FROM pbp
                WHERE H2Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p4_team, H3Name, H3Num, H3Pos
                FROM pbp
                WHERE H3Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p4_team, H4Name, H4Num, H4Pos
                FROM pbp
                WHERE H4Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p4_team, H5Name, H5Num, H5Pos
                FROM pbp
                WHERE H5Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p4_team, H6Name, H6Num, H6Pos
                FROM pbp
                WHERE H6Pos != 'G'
                UNION ALL
                
                SELECT game_id, event_type, p1_team, p4_team, A1Name, A1Num, A1Pos
                FROM pbp
                WHERE A1Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p4_team, A2Name, A2Num, A2Pos
                FROM pbp
                WHERE A2Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p4_team, A3Name, A3Num, A3Pos
                FROM pbp
                WHERE A3Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p4_team, A4Name, A4Num, A4Pos
                FROM pbp
                WHERE A4Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p4_team, A5Name, A5Num, A5Pos
                FROM pbp
                WHERE A5Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, p4_team, A6Name, A6Num, A6Pos
                FROM pbp
                WHERE A6Pos != 'G'
                ) as p
                
            INNER JOIN rosters r 
            ON (r.game_id=p.game_id
                and r.name=p.name
                and r.num=p.num)
            ) as p
            
        WHERE (event_type='GOAL' and p4_team=p.team)
            or (event_type='MISS' and p4_team=p.team)
            or (event_type='SHOT' and p4_team=p.team)
            or (event_type='BLOCK' and p1_team=p.team)
        ) as p
        
    GROUP BY p.name) as ca
ON CA.name=r.name

LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, COUNT(p.name) as FF
    FROM
        (SELECT p.game_id, event_type, p1_team, p.name as name, p.pos as pos, p.team as team
        
        FROM 
            (SELECT p.game_id, event_type, p1_team, p.name, r.Team as team, p.pos
            
            FROM
                (SELECT game_id, event_type, p1_team, H1Name as name, H1Num as num, H1Pos as pos
                
                FROM pbp
                
                WHERE H1Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, H2Name, H2Num, H2Pos
                FROM pbp
                WHERE H2Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, H3Name, H3Num, H3Pos
                FROM pbp
                WHERE H3Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, H4Name, H4Num, H4Pos
                FROM pbp
                WHERE H4Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, H5Name, H5Num, H5Pos
                FROM pbp
                WHERE H5Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, H6Name, H6Num, H6Pos
                FROM pbp
                WHERE H6Pos != 'G'
                UNION ALL
                
                SELECT game_id, event_type, p1_team, A1Name, A1Num, A1Pos
                FROM pbp
                WHERE A1Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, A2Name, A2Num, A2Pos
                FROM pbp
                WHERE A2Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, A3Name, A3Num, A3Pos
                FROM pbp
                WHERE A3Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, A4Name, A4Num, A4Pos
                FROM pbp
                WHERE A4Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, A5Name, A5Num, A5Pos
                FROM pbp
                WHERE A5Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, A6Name, A6Num, A6Pos
                FROM pbp
                WHERE A6Pos != 'G'
                ) as p
                
            INNER JOIN rosters r 
            ON (r.game_id=p.game_id
                and r.name=p.name
                and r.num=p.num)
            ) as p
            
        WHERE (event_type='GOAL' and p1_team=p.team)
            or (event_type='MISS' and p1_team=p.team)
            or (event_type='SHOT' and p1_team=p.team)
        ) as p
        
    GROUP BY p.name) as ff
ON FF.name=r.name

LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, COUNT(p.name) as FA
    FROM
        (SELECT p.game_id, event_type, p4_team, p.name as name, p.pos as pos, p.team as team
        
        FROM 
            (SELECT p.game_id, event_type, p4_team, p.name, r.Team as team, p.pos
            
            FROM
                (SELECT game_id, event_type, p4_team, H1Name as name, H1Num as num, H1Pos as pos
                
                FROM pbp
                
                WHERE H1Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, H2Name, H2Num, H2Pos
                FROM pbp
                WHERE H2Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, H3Name, H3Num, H3Pos
                FROM pbp
                WHERE H3Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, H4Name, H4Num, H4Pos
                FROM pbp
                WHERE H4Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, H5Name, H5Num, H5Pos
                FROM pbp
                WHERE H5Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, H6Name, H6Num, H6Pos
                FROM pbp
                WHERE H6Pos != 'G'
                UNION ALL
                
                SELECT game_id, event_type, p4_team, A1Name, A1Num, A1Pos
                FROM pbp
                WHERE A1Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, A2Name, A2Num, A2Pos
                FROM pbp
                WHERE A2Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, A3Name, A3Num, A3Pos
                FROM pbp
                WHERE A3Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, A4Name, A4Num, A4Pos
                FROM pbp
                WHERE A4Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, A5Name, A5Num, A5Pos
                FROM pbp
                WHERE A5Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, A6Name, A6Num, A6Pos
                FROM pbp
                WHERE A6Pos != 'G'
                ) as p
                
            INNER JOIN rosters r 
            ON (r.game_id=p.game_id
                and r.name=p.name
                and r.num=p.num)
            ) as p
            
        WHERE (event_type='GOAL' and p4_team=p.team)
            or (event_type='MISS' and p4_team=p.team)
            or (event_type='SHOT' and p4_team=p.team)
        ) as p
        
    GROUP BY p.name) as fa
ON FA.name=r.name

LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, COUNT(p.name) as SF
    FROM
        (SELECT p.game_id, event_type, p1_team, p.name as name, p.pos as pos, p.team as team
        
        FROM 
            (SELECT p.game_id, event_type, p1_team, p.name, r.Team as team, p.pos
            
            FROM
                (SELECT game_id, event_type, p1_team, H1Name as name, H1Num as num, H1Pos as pos
                
                FROM pbp
                
                WHERE H1Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, H2Name, H2Num, H2Pos
                FROM pbp
                WHERE H2Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, H3Name, H3Num, H3Pos
                FROM pbp
                WHERE H3Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, H4Name, H4Num, H4Pos
                FROM pbp
                WHERE H4Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, H5Name, H5Num, H5Pos
                FROM pbp
                WHERE H5Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, H6Name, H6Num, H6Pos
                FROM pbp
                WHERE H6Pos != 'G'
                UNION ALL
                
                SELECT game_id, event_type, p1_team, A1Name, A1Num, A1Pos
                FROM pbp
                WHERE A1Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, A2Name, A2Num, A2Pos
                FROM pbp
                WHERE A2Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, A3Name, A3Num, A3Pos
                FROM pbp
                WHERE A3Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, A4Name, A4Num, A4Pos
                FROM pbp
                WHERE A4Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, A5Name, A5Num, A5Pos
                FROM pbp
                WHERE A5Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, A6Name, A6Num, A6Pos
                FROM pbp
                WHERE A6Pos != 'G'
                ) as p
                
            INNER JOIN rosters r 
            ON (r.game_id=p.game_id
                and r.name=p.name
                and r.num=p.num)
            ) as p
            
        WHERE (event_type='GOAL' and p1_team=p.team)
            or (event_type='SHOT' and p1_team=p.team)
        ) as p
        
    GROUP BY p.name) as sf
ON SF.name=r.name

LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, COUNT(p.name) as SA
    FROM
        (SELECT p.game_id, event_type, p4_team, p.name as name, p.pos as pos, p.team as team
        
        FROM 
            (SELECT p.game_id, event_type, p4_team, p.name, r.Team as team, p.pos
            
            FROM
                (SELECT game_id, event_type, p4_team, H1Name as name, H1Num as num, H1Pos as pos
                
                FROM pbp
                
                WHERE H1Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, H2Name, H2Num, H2Pos
                FROM pbp
                WHERE H2Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, H3Name, H3Num, H3Pos
                FROM pbp
                WHERE H3Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, H4Name, H4Num, H4Pos
                FROM pbp
                WHERE H4Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, H5Name, H5Num, H5Pos
                FROM pbp
                WHERE H5Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, H6Name, H6Num, H6Pos
                FROM pbp
                WHERE H6Pos != 'G'
                UNION ALL
                
                SELECT game_id, event_type, p4_team, A1Name, A1Num, A1Pos
                FROM pbp
                WHERE A1Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, A2Name, A2Num, A2Pos
                FROM pbp
                WHERE A2Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, A3Name, A3Num, A3Pos
                FROM pbp
                WHERE A3Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, A4Name, A4Num, A4Pos
                FROM pbp
                WHERE A4Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, A5Name, A5Num, A5Pos
                FROM pbp
                WHERE A5Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, A6Name, A6Num, A6Pos
                FROM pbp
                WHERE A6Pos != 'G'
                ) as p
                
            INNER JOIN rosters r 
            ON (r.game_id=p.game_id
                and r.name=p.name
                and r.num=p.num)
            ) as p
            
        WHERE (event_type='GOAL' and p4_team=p.team)
            or (event_type='SHOT' and p4_team=p.team)
        ) as p
        
    GROUP BY p.name) as sa
ON SA.name=r.name

LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, COUNT(p.name) as GF
    FROM
        (SELECT p.game_id, event_type, p1_team, p.name as name, p.pos as pos, p.team as team
        
        FROM 
            (SELECT p.game_id, event_type, p1_team, p.name, r.Team as team, p.pos
            
            FROM
                (SELECT game_id, event_type, p1_team, H1Name as name, H1Num as num, H1Pos as pos
                
                FROM pbp
                
                WHERE H1Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, H2Name, H2Num, H2Pos
                FROM pbp
                WHERE H2Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, H3Name, H3Num, H3Pos
                FROM pbp
                WHERE H3Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, H4Name, H4Num, H4Pos
                FROM pbp
                WHERE H4Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, H5Name, H5Num, H5Pos
                FROM pbp
                WHERE H5Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, H6Name, H6Num, H6Pos
                FROM pbp
                WHERE H6Pos != 'G'
                UNION ALL
                
                SELECT game_id, event_type, p1_team, A1Name, A1Num, A1Pos
                FROM pbp
                WHERE A1Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, A2Name, A2Num, A2Pos
                FROM pbp
                WHERE A2Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, A3Name, A3Num, A3Pos
                FROM pbp
                WHERE A3Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, A4Name, A4Num, A4Pos
                FROM pbp
                WHERE A4Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, A5Name, A5Num, A5Pos
                FROM pbp
                WHERE A5Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p1_team, A6Name, A6Num, A6Pos
                FROM pbp
                WHERE A6Pos != 'G'
                ) as p
                
            INNER JOIN rosters r 
            ON (r.game_id=p.game_id
                and r.name=p.name
                and r.num=p.num)
            ) as p
            
        WHERE (event_type='GOAL' and p1_team=p.team)
        ) as p
        
    GROUP BY p.name) as gf
ON GF.name=r.name

LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, COUNT(p.name) as GA
    FROM
        (SELECT p.game_id, event_type, p4_team, p.name as name, p.pos as pos, p.team as team
        
        FROM 
            (SELECT p.game_id, event_type, p4_team, p.name, r.Team as team, p.pos
            
            FROM
                (SELECT game_id, event_type, p4_team, H1Name as name, H1Num as num, H1Pos as pos
                
                FROM pbp
                
                WHERE H1Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, H2Name, H2Num, H2Pos
                FROM pbp
                WHERE H2Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, H3Name, H3Num, H3Pos
                FROM pbp
                WHERE H3Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, H4Name, H4Num, H4Pos
                FROM pbp
                WHERE H4Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, H5Name, H5Num, H5Pos
                FROM pbp
                WHERE H5Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, H6Name, H6Num, H6Pos
                FROM pbp
                WHERE H6Pos != 'G'
                UNION ALL
                
                SELECT game_id, event_type, p4_team, A1Name, A1Num, A1Pos
                FROM pbp
                WHERE A1Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, A2Name, A2Num, A2Pos
                FROM pbp
                WHERE A2Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, A3Name, A3Num, A3Pos
                FROM pbp
                WHERE A3Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, A4Name, A4Num, A4Pos
                FROM pbp
                WHERE A4Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, A5Name, A5Num, A5Pos
                FROM pbp
                WHERE A5Pos != 'G'
                UNION ALL
                SELECT game_id, event_type, p4_team, A6Name, A6Num, A6Pos
                FROM pbp
                WHERE A6Pos != 'G'
                ) as p
                
            INNER JOIN rosters r 
            ON (r.game_id=p.game_id
                and r.name=p.name
                and r.num=p.num)
            ) as p
            
        WHERE (event_type='GOAL' and p4_team=p.team)
        ) as p
        
    GROUP BY p.name) as ga
ON GA.name=r.name

WHERE r.pos != 'G' 
    AND r.scratch is null
    
GROUP BY z.season, r.name, r.pos

ORDER BY cf.CF DESC, r.name
    
;''', conn)

print(skater_on_ice_counts)
