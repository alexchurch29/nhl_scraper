import sqlite3
import pandas as pd
import numpy as np
from nhl_gamescraper import scrape_games_by_date

conn = sqlite3.connect('nhl.db')
cur = conn.cursor()


def skaters_individual_counts():
    skaters_individual_counts = pd.read_sql_query ('''
    SELECT r.name, z.season, r.team, r.pos, COUNT(r.name) as GP, icetime.TOI, ifnull(goal.G,0) as G, ifnull(assist1.A1,0) 
        as A1, ifnull(assist2.A2,0) as A2, (ifnull(assist1.A1,0) + ifnull(assist2.A2,0)) as A, 
        (ifnull(goal.G,0) + (ifnull(assist1.A1,0) + ifnull(assist2.A2,0))) as P, (ifnull(goal.G,0) + ifnull(assist1.A1,0)) 
        as P1, (ifnull(goal.G,0) + ifnull(shots.SH,0)) as SH, ifnull((round(ifnull(goal.G,0),2)  / 
        (round(ifnull(shots.SH,0),2) + round(ifnull(goal.G,0),2))),0) as 'SH%', ifnull(missed.MISS,0) as MISS, 
        ifnull(blocked.BLOCK,0) as BLOCKED, (ifnull(shots.SH,0) + ifnull(missed.MISS,0) + ifnull(blocked.BLOCK,0)) as iCF, 
        (ifnull(shots.SH,0) + ifnull(missed.MISS,0)) as iFF, ifnull(pim.PIM,0) as PIM, 
        ifnull(penl.PENL_Taken,0) as PENL_Taken, ifnull(min.Minor,0) as Minor, ifnull(maj.Major,0) as Major, 
        ifnull(misc.Misconduct,0) as Misconduct, ifnull(pend.PENL_Drawn,0) as PENL_Drawn, 
        ifnull(give.Giveaways,0) as Giveaways, ifnull(take.Takeaways,0) as Takeaways, ifnull(hit.hits,0) as Hits, 
        ifnull(hittaken.Hits_Taken,0) as Hits_Taken, ifnull(blocksfor.Shot_Blocks,0) as Shot_Blocks, 
        ifnull(faceoffwon.Faceoffs_Won,0) as Faceoffs_Won, ifnull(faceofflost.Faceoffs_Lost,0) as Faceoffs_Lost, 
        ifnull((round(ifnull(faceoffwon.Faceoffs_Won,0), 2) / (round(ifnull(faceoffwon.Faceoffs_Won,0), 2) + 
        round(ifnull(faceofflost.Faceoffs_Lost,0), 2))),0) as 'Faceoff%'
    
    FROM rosters r 
    
    INNER JOIN schedule z 
    ON r.game_id = z.game_id
    
    LEFT OUTER JOIN (SELECT p.name as name, p.team as team, r.pos as pos, round(count(p.name),2)/60 as TOI

        FROM shifts_by_sec p
            
        INNER JOIN shifts_by_sec_teams s
        
        ON s.game_id=p.game_id 
        and s.time=p.time
            
        INNER JOIN rosters r
            
        ON r.name=p.name 
        and r.game_id=p.game_id
        and r.team=p.team
            
        INNER JOIN schedule z
            
        ON p.game_id=z.game_id
            
        GROUP BY p.name, r.pos) as icetime
    ON icetime.name = r.name and icetime.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, count(name) as G, pos
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
        WHERE p.event_type = 'GOAL'
            and p.period != 5)
        GROUP BY name, pos) as goal
    ON goal.name = r.name and goal.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, count(name) as A1, pos
        FROM (SELECT r.name, p.p2_team, p.p2_num, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p2_team
            and r.num=p.p2_num)
        WHERE p.event_type = 'GOAL'
            and p.period != 5)
        GROUP BY name, pos) as assist1
    ON assist1.name = r.name and assist1.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, count(name) as A2, pos
        FROM (SELECT r.name, p.p3_team, p.p3_num, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p3_team
            and r.num=p.p3_num)
        WHERE p.event_type = 'GOAL'
            and p.period != 5)
        GROUP BY name, pos) as assist2
    ON assist2.name = r.name and assist2.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, count(name) as SH, pos
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
        WHERE p.event_type = 'SHOT'
            and p.period != 5)
        GROUP BY name, pos) as shots
    ON shots.name = r.name and shots.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, count(name) as MISS, pos
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
        WHERE p.event_type = 'MISS'
            and p.period != 5)
        GROUP BY name, pos) as missed
    ON missed.name = r.name and missed.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, count(name) as BLOCK, pos
        FROM (SELECT r.name, p.p2_team, p.p2_num, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p2_team
            and r.num=p.p2_num)
        WHERE p.event_type = 'BLOCK'
            and p.period != 5)
        GROUP BY name, pos) as blocked
    ON blocked.name = r.name and blocked.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, sum(penl_length) as PIM, pos
        FROM (SELECT r.name, p.p1_team, p.p1_num, p.penl_length, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
        WHERE p.event_type = 'PENL'
            and p.period != 5)
        GROUP BY name, pos) as pim
    ON pim.name = r.name and pim.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, count(name) as PENL_Taken, pos
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
        WHERE p.event_type = 'PENL'
            and p.period != 5)
        GROUP BY name, pos) as penl
    ON penl.name = r.name and penl.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, count(name) as Minor, pos
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
        WHERE p.event_type = 'PENL'
            and p.penl_length = 2
            and p.period != 5)
        GROUP BY name, pos) as min
    ON min.name = r.name and min.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, count(name) as Major, pos
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
        WHERE p.event_type = 'PENL'
            and p.penl_length = 5
            and p.period != 5)
        GROUP BY name, pos) as maj
    ON maj.name = r.name and maj.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, count(name) as Misconduct, pos
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
        WHERE p.event_type = 'PENL'
            and p.penl_length = 10
            and p.period != 5)
        GROUP BY name, pos) as misc
    ON misc.name = r.name and misc.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, count(name) as PENL_Drawn, pos
        FROM (SELECT r.name, p.p2_team, p.p2_num, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p2_team
            and r.num=p.p2_num)
        WHERE p.event_type = 'PENL'
            and p.period != 5)
        GROUP BY name, pos) as pend
    ON pend.name = r.name and pend.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, count(name) as Giveaways, pos
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
        WHERE p.event_type = 'GIVE'
            and p.period != 5)
        GROUP BY name, pos) as give
    ON give.name = r.name and give.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, count(name) as Takeaways, pos
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
        WHERE p.event_type = 'TAKE'
            and p.period != 5)
        GROUP BY name, pos) as take
    ON take.name = r.name and take.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, count(name) as Hits, pos
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
        WHERE p.event_type = 'HIT'
            and p.period != 5)
        GROUP BY name, pos) as hit
    ON hit.name = r.name and hit.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, count(name) as Hits_Taken, pos
        FROM (SELECT r.name, p.p2_team, p.p2_num, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p2_team
            and r.num=p.p2_num)
        WHERE p.event_type = 'HIT'
            and p.period != 5)
        GROUP BY name, pos) as hittaken
    ON hittaken.name = r.name and hittaken.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, count(name) as Shot_Blocks, pos
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
        WHERE p.event_type = 'BLOCK'
            and p.period != 5)
        GROUP BY name, pos) as blocksfor
    ON blocksfor.name = r.name and blocksfor.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, count(name) as Faceoffs_Won, pos
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
        WHERE p.event_type = 'FAC'
            and p.period != 5)
        GROUP BY name, pos) as faceoffwon
    ON faceoffwon.name = r.name and faceoffwon.pos = r.pos
    
    LEFT OUTER JOIN (SELECT name, count(name) as Faceoffs_Lost, pos
        FROM (SELECT r.name, p.p2_team, p.p2_num, r.pos
        FROM pbp p
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p2_team
            and r.num=p.p2_num)
        WHERE p.event_type = 'FAC'
            and p.period != 5)
        GROUP BY name, pos) as faceofflost
    ON faceofflost.name = r.name and faceofflost.pos = r.pos
        
    WHERE r.pos != 'G' 
        AND r.scratch is null
        
    GROUP BY z.season, r.name, r.pos
    
    ORDER BY P DESC, r.name;''', conn)
    return skaters_individual_counts


def skater_on_ice_counts():
    skater_on_ice_counts = pd.read_sql_query('''
    SELECT r.name, z.season, r.team, r.pos, COUNT(r.name) as GP, icetime.TOI, ifnull(cf.CF,0) as CF, ifnull(ca.CA,0) as CA, 
    ifnull(round(ifnull(cf.CF,0),2) / (round(ifnull(cf.CF,0),2) + round(ifnull(ca.CA,0),2)),0) as 'CF%', ifnull(ff.FF,0) as FF, 
    ifnull(fa.FA,0) as FA, ifnull(round(ifnull(ff.FF,0),2) / (round(ifnull(ff.FF,0),2) + round(ifnull(fa.FA,0),2)),0) as 'FF%', 
    ifnull(sf.SF,0) as SF, ifnull(sa.SA,0) as SA, ifnull(round(ifnull(sf.SF,0),2) / (round(ifnull(sf.SF,0),2) + round(ifnull(sa.SA,0),2)),0) 
    as 'SF%', ifnull(gf.GF,0) as GF, ifnull(ga.GA,0) as GA, ifnull(round(ifnull(gf.GF,0),2) / (round(ifnull(gf.GF,0),2)
    + round(ifnull(ga.GA,0),2)),0) as 'GF%', ifnull(round(ifnull(gf.GF,0),2) / (round(ifnull(sf.SF,0),2)),0) as 'On_Ice_SH%', 
    ifnull(1 - (round(ifnull(ga.GA,0),2) / (round(ifnull(sa.SA,0),2))),1) as 'On_Ice_SV%', ifnull(round(ifnull(gf.GF,0),2) / 
    (round(ifnull(sf.SF,0),2)),0) + ifnull(1 - (round(ifnull(ga.GA,0),2) / (round(ifnull(sa.SA,0),2))),1) as PDO, 
    ifnull(oz.OZ_Faceoffs,0) as OZ_Faceoffs, ifnull(dz.DZ_Faceoffs,0) as DZ_Faceoffs, ifnull(nz.NZ_Faceoffs,0) as NZ_Faceoffs
    
    FROM rosters r 
    
    INNER JOIN schedule z 
    ON r.game_id = z.game_id
    
    LEFT OUTER JOIN (SELECT p.name as name, p.team as team, r.pos as pos, round(count(p.name),2)/60 as TOI

        FROM shifts_by_sec p
            
        INNER JOIN shifts_by_sec_teams s
        
        ON s.game_id=p.game_id 
        and s.time=p.time
            
        INNER JOIN rosters r
            
        ON r.name=p.name 
        and r.game_id=p.game_id
        and r.team=p.team
            
        INNER JOIN schedule z
            
        ON p.game_id=z.game_id
            
        GROUP BY p.name, r.pos) as icetime
    ON icetime.name = r.name and icetime.pos = r.pos
    
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
            
        GROUP BY p.name, p.pos) as cf
    ON CF.name=r.name and cf.pos=r.pos
    
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
            
        GROUP BY p.name, p.pos) as ca
    ON CA.name=r.name and CA.pos=r.pos
    
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
            
        GROUP BY p.name, p.pos) as ff
    ON FF.name=r.name and FF.pos=r.pos
    
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
            
        GROUP BY p.name, p.pos) as fa
    ON FA.name=r.name and FA.pos=r.pos
    
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
            
        GROUP BY p.name, p.pos) as sf
    ON SF.name=r.name and SF.pos=r.pos
    
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
            
        GROUP BY p.name, p.pos) as sa
    ON SA.name=r.name and SA.pos=r.pos
    
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
            
        GROUP BY p.name, p.pos) as gf
    ON GF.name=r.name and GF.pos=r.pos
    
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
            
        GROUP BY p.name, p.pos) as ga
    ON GA.name=r.name and GA.pos=r.pos
    
    LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, COUNT(p.name) as OZ_Faceoffs
        FROM
            (SELECT p.game_id, event_type, p.zone, p.name as name, p.pos as pos, p.team as team
            
            FROM 
                (SELECT p.game_id, event_type, p.zone, p.name, r.Team as team, p.pos
                
                FROM
                    (SELECT game_id, event_type, home_zone as zone, H1Name as name, H1Num as num, H1Pos as pos
                    
                    FROM pbp
                    
                    WHERE H1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H2Name, H2Num, H2Pos
                    FROM pbp
                    WHERE H2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H3Name, H3Num, H3Pos
                    FROM pbp
                    WHERE H3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H4Name, H4Num, H4Pos
                    FROM pbp
                    WHERE H4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H5Name, H5Num, H5Pos
                    FROM pbp
                    WHERE H5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H6Name, H6Num, H6Pos
                    FROM pbp
                    WHERE H6Pos != 'G'
                    UNION ALL
                    
                    SELECT game_id, event_type, away_zone, A1Name, A1Num, A1Pos
                    FROM pbp
                    WHERE A1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A2Name, A2Num, A2Pos
                    FROM pbp
                    WHERE A2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A3Name, A3Num, A3Pos
                    FROM pbp
                    WHERE A3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A4Name, A4Num, A4Pos
                    FROM pbp
                    WHERE A4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A5Name, A5Num, A5Pos
                    FROM pbp
                    WHERE A5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A6Name, A6Num, A6Pos
                    FROM pbp
                    WHERE A6Pos != 'G'
                    ) as p
                    
                INNER JOIN rosters r 
                ON (r.game_id=p.game_id
                    and r.name=p.name
                    and r.num=p.num)
                ) as p
                
            WHERE (event_type='FAC' and zone ='OZ')
            ) as p
            
        GROUP BY p.name, p.pos) as oz
    ON oz.name=r.name and oz.pos=r.pos
    
    LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, COUNT(p.name) as DZ_Faceoffs
        FROM
            (SELECT p.game_id, event_type, p.zone, p.name as name, p.pos as pos, p.team as team
            
            FROM 
                (SELECT p.game_id, event_type, p.zone, p.name, r.Team as team, p.pos
                
                FROM
                    (SELECT game_id, event_type, home_zone as zone, H1Name as name, H1Num as num, H1Pos as pos
                    
                    FROM pbp
                    
                    WHERE H1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H2Name, H2Num, H2Pos
                    FROM pbp
                    WHERE H2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H3Name, H3Num, H3Pos
                    FROM pbp
                    WHERE H3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H4Name, H4Num, H4Pos
                    FROM pbp
                    WHERE H4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H5Name, H5Num, H5Pos
                    FROM pbp
                    WHERE H5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H6Name, H6Num, H6Pos
                    FROM pbp
                    WHERE H6Pos != 'G'
                    UNION ALL
                    
                    SELECT game_id, event_type, away_zone, A1Name, A1Num, A1Pos
                    FROM pbp
                    WHERE A1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A2Name, A2Num, A2Pos
                    FROM pbp
                    WHERE A2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A3Name, A3Num, A3Pos
                    FROM pbp
                    WHERE A3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A4Name, A4Num, A4Pos
                    FROM pbp
                    WHERE A4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A5Name, A5Num, A5Pos
                    FROM pbp
                    WHERE A5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A6Name, A6Num, A6Pos
                    FROM pbp
                    WHERE A6Pos != 'G'
                    ) as p
                    
                INNER JOIN rosters r 
                ON (r.game_id=p.game_id
                    and r.name=p.name
                    and r.num=p.num)
                ) as p
                
            WHERE (event_type='FAC' and zone ='DZ')
            ) as p
            
        GROUP BY p.name, p.pos) as dz
    ON dz.name=r.name and dz.pos=r.pos
    
    LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, COUNT(p.name) as NZ_Faceoffs
        FROM
            (SELECT p.game_id, event_type, p.zone, p.name as name, p.pos as pos, p.team as team
            
            FROM 
                (SELECT p.game_id, event_type, p.zone, p.name, r.Team as team, p.pos
                
                FROM
                    (SELECT game_id, event_type, home_zone as zone, H1Name as name, H1Num as num, H1Pos as pos
                    
                    FROM pbp
                    
                    WHERE H1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H2Name, H2Num, H2Pos
                    FROM pbp
                    WHERE H2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H3Name, H3Num, H3Pos
                    FROM pbp
                    WHERE H3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H4Name, H4Num, H4Pos
                    FROM pbp
                    WHERE H4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H5Name, H5Num, H5Pos
                    FROM pbp
                    WHERE H5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H6Name, H6Num, H6Pos
                    FROM pbp
                    WHERE H6Pos != 'G'
                    UNION ALL
                    
                    SELECT game_id, event_type, away_zone, A1Name, A1Num, A1Pos
                    FROM pbp
                    WHERE A1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A2Name, A2Num, A2Pos
                    FROM pbp
                    WHERE A2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A3Name, A3Num, A3Pos
                    FROM pbp
                    WHERE A3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A4Name, A4Num, A4Pos
                    FROM pbp
                    WHERE A4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A5Name, A5Num, A5Pos
                    FROM pbp
                    WHERE A5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A6Name, A6Num, A6Pos
                    FROM pbp
                    WHERE A6Pos != 'G'
                    ) as p
                    
                INNER JOIN rosters r 
                ON (r.game_id=p.game_id
                    and r.name=p.name
                    and r.num=p.num)
                ) as p
                
            WHERE (event_type='FAC' and zone ='NZ')
            ) as p
            
        GROUP BY p.name, p.pos) as nz
    ON nz.name=r.name and nz.pos=r.pos
    
    WHERE r.pos != 'G' 
        AND r.scratch is null
        
    GROUP BY z.season, r.name, r.pos
    
    ORDER BY cf.CF DESC, r.name;''', conn)
    return skater_on_ice_counts


def shifts_by_sec_teams():
    shifts_by_sec_teams = pd.read_sql_query ('''SELECT p.*, CASE WHEN p.time = 1 THEN 0 ELSE d.home_score END home_score, 
    CASE WHEN p.time = 1 THEN 0 ELSE d.away_score END away_score
    
    FROM (SELECT p.game_id, p.time, 
    SUM(CASE WHEN z.home_team = p.team THEN 1 ELSE 0 END) as home_strength,
    SUM(CASE WHEN z.away_team = p.team THEN 1 ELSE 0 END) as away_strength
    
    FROM shifts_by_sec_temp p 
    
    INNER JOIN rosters r
    
    ON r.name=p.name 
    and r.game_id=p.game_id
    and r.team=p.team
    
    INNER JOIN schedule z
    
    ON p.game_id=z.game_id
    
    WHERE r.pos != 'G'
    
    GROUP BY z.home_team, z.away_team, p.time, p.game_id
    
    ORDER BY p.game_id, p.time) as p
    
    LEFT OUTER JOIN pbp d 
    
    ON p.game_id=d.game_id and p.time=((d.period-1)*1200+d.time_elapsed);''', conn)
    return shifts_by_sec_teams


def update(start_date, end_date):

    game_data = scrape_games_by_date(start_date, end_date)

    r = game_data[0]
    s = game_data[1]
    p = game_data[2]
    c = game_data[3]
    o = game_data[4]
    new_sched = game_data[5]

    # update schedule with games that have since gone final
    old_sched = pd.read_sql('select * from schedule', conn)
    schedule = pd.concat([old_sched, new_sched], ignore_index=True)
    schedule = schedule.drop_duplicates(subset=['Game_Id'], keep='last')
    schedule.to_sql('schedule', conn, if_exists='replace', index=False)

    # update all other existing tables in database
    r.to_sql('rosters', conn, if_exists='append', index=False)
    s.to_sql('shifts', conn, if_exists='append', index=False)
    p.to_sql('pbp', conn, if_exists='append', index=False)
    c.to_sql('coaches', conn, if_exists='append', index=False)
    o.to_sql('officials', conn, if_exists='append', index=False)

    for row in s.itertuples():
        shift = pd.DataFrame({'Time': np.arange(int(row.Start) + 1200 * ((int(row.Period)) - 1) + 1,
                                                int(row.End) + 1200 * ((int(row.Period)) - 1) + 1)})
        shift['Game_Id'] = row.Game_Id
        shift['Name'] = row.Player
        shift['Team'] = row.Team
        shift.to_sql('shifts_by_sec', conn, if_exists='append', index=False, chunksize=100000)
        shift.to_sql('shifts_by_sec_temp', conn, if_exists='append', index=False, chunksize=100000)

    t = shifts_by_sec_teams()
    col = ['home_score', 'away_score']
    t[col] = t[col].ffill()
    t = t.drop_duplicates(subset=['game_id', 'time'], keep='first')
    t.to_sql('shifts_by_sec_teams', conn, if_exists='append', index=False, chunksize=100000)

    skater_on_ice_counts().to_sql('skaters_on_ice_counts', conn, if_exists='replace', index=False)
    skaters_individual_counts().to_sql('skaters_individual_counts', conn, if_exists='replace', index=False)
    cur.executescript('DROP TABLE IF EXISTS shifts_by_sec_temp')


def convert_to_csv():
    """
    Used to convert all pickle files of pd dataframes into csv files
    :return: csv file of rosters, shifts, pbp, coaches, officials, schedule
    """

    r = pd.read_sql('rosters', conn)
    s = pd.read_sql('shifts', conn)
    p = pd.read_sql('pbp', conn)
    c = pd.read_sql('coaches', conn)
    o = pd.read_sql('officials', conn)
    z = pd.read_sql('schedule', conn)
    i = pd.read_sql('shifts_by_sec', conn)
    t = pd.read_sql('shifts_by_sec_teams', conn)

    r.to_csv('rosters.csv', index=False)
    s.to_csv('shifts.csv', index=False)
    p.to_csv('pbp.csv', index=False)
    c.to_csv('coaches.csv', index=False)
    o.to_csv('officials.csv', index=False)
    z.to_csv('schedule.csv', index=False)
    i.to_csv('shifts_by_sec.csv', index=False, chunksize=100000)
    t.to_csv('shifts_by_sec_teams.csv', index=False, chunksize=100000)
    skater_on_ice_counts().to_csv('skaters_on_ice_counts.csv', index=False)
    skaters_individual_counts().to_csv('skaters_individual_counts.csv', index=False)

# update('2018-03-28', '2018-03-28')
