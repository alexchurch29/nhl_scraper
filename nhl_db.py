import sqlite3
import pandas as pd
import numpy as np
from nhl_gamescraper import scrape_games_by_date

conn = sqlite3.connect('nhl.db')
cur = conn.cursor()


def skaters_stats():
    skaters_stats = pd.read_sql_query ('''
    SELECT z.season, z.game_type, z.game_id, z.date, CASE WHEN r.team = z.home_team THEN 'Home' ELSE 'Away' END venue,
        strength.period, strength.strength, r.name, r.team, r.pos, 1 as GP, icetime.TOI, ifnull(goal.G,0) as G,
        ifnull(assist1.A1,0) as A1, ifnull(assist2.A2,0) as A2, (ifnull(assist1.A1,0) + ifnull(assist2.A2,0)) as A,
        (ifnull(goal.G,0) + (ifnull(assist1.A1,0) + ifnull(assist2.A2,0))) as P, (ifnull(goal.G,0) + 
        ifnull(assist1.A1,0)) as P1, (ifnull(goal.G,0) + ifnull(shots.SH,0)) as SH, ifnull((round(ifnull(goal.G,0),2) /
        (round(ifnull(shots.SH,0),2) + round(ifnull(goal.G,0),2))),0.0) as 'SH%', ifnull(missed.MISS,0) as MISS,
        ifnull(blocked.BLOCK,0) as BLOCKED,
        (ifnull(goal.G,0) + ifnull(shots.SH,0) + ifnull(missed.MISS,0) + ifnull(blocked.BLOCK,0)) as iCF,
        (ifnull(goal.G,0) + ifnull(shots.SH,0) + ifnull(missed.MISS,0)) as iFF, ifnull(pim.PIM,0) as PIM,
        ifnull(penl.PENL_Taken,0) as PENL_Taken, ifnull(min.Minor,0) as Minor, ifnull(maj.Major,0) as Major,
        ifnull(misc.Misconduct,0) as Misconduct, ifnull(pend.PENL_Drawn,0) as PENL_Drawn,
        ifnull(give.Giveaways,0) as Giveaways, ifnull(take.Takeaways,0) as Takeaways, ifnull(hit.hits,0) as Hits,
        ifnull(hittaken.Hits_Taken,0) as Hits_Taken, ifnull(blocksfor.Shot_Blocks,0) as Shot_Blocks,
        ifnull(faceoffwon.Faceoffs_Won,0) as Faceoffs_Won, ifnull(faceofflost.Faceoffs_Lost,0) as Faceoffs_Lost,
        ifnull((round(ifnull(faceoffwon.Faceoffs_Won,0), 2) / (round(ifnull(faceoffwon.Faceoffs_Won,0), 2) +
        round(ifnull(faceofflost.Faceoffs_Lost,0), 2))),0.0) as 'Faceoff%', ifnull(cf.CF,0) as CF,
        ifnull(ca.CA,0) as CA, ifnull(round(ifnull(cf.CF,0),2) / (round(ifnull(cf.CF,0),2) + 
        round(ifnull(ca.CA,0),2)),0) as 'CF%', ifnull(ff.FF,0) as FF, ifnull(fa.FA,0) as FA, 
        ifnull(round(ifnull(ff.FF,0),2) / (round(ifnull(ff.FF,0),2)
        + round(ifnull(fa.FA,0),2)),0) as 'FF%', ifnull(sf.SF,0) as SF, ifnull(sa.SA,0) as SA,
        ifnull(round(ifnull(sf.SF,0),2) / (round(ifnull(sf.SF,0),2) + round(ifnull(sa.SA,0),2)),0)
        as 'SF%', ifnull(gf.GF,0) as GF, ifnull(ga.GA,0) as GA, ifnull(round(ifnull(gf.GF,0),2) / 
        (round(ifnull(gf.GF,0),2) + round(ifnull(ga.GA,0),2)),0) as 'GF%', ifnull(round(ifnull(gf.GF,0),2) / 
        (round(ifnull(sf.SF,0),2)),0) as 'On_Ice_SH%', ifnull(1 - (round(ifnull(ga.GA,0),2) / 
        (round(ifnull(sa.SA,0),2))),1) as 'On_Ice_SV%',
        ifnull(round(ifnull(gf.GF,0),2) / (round(ifnull(sf.SF,0),2)),0) + ifnull(1 - (round(ifnull(ga.GA,0),2) /
        (round(ifnull(sa.SA,0),2))),1) as PDO, ifnull(oz.OZ_Faceoffs,0) as OZ_Faceoffs, ifnull(dz.DZ_Faceoffs,0)
        as DZ_Faceoffs, ifnull(nz.NZ_Faceoffs,0) as NZ_Faceoffs, ifnull(cf.adjCF,0) as adjCF,
        ifnull(ca.adjCA,0) as adjCA, ifnull(round(ifnull(cf.adjCF,0),2) / (round(ifnull(cf.adjCF,0),2)
        + round(ifnull(ca.adjCA,0),2)),0) as 'adjCF%', ifnull(ff.adjFF,0) as adjFF, ifnull(fa.adjFA,0) as adjFA,
        ifnull(round(ifnull(ff.adjFF,0),2) / (round(ifnull(ff.adjFF,0),2) + round(ifnull(fa.adjFA,0),2)),0) as 'adjFF%',
        ifnull(sf.adjSF,0) as adjSF, ifnull(sa.adjSA,0) as adjSA, ifnull(round(ifnull(sf.adjSF,0),2) /
        (round(ifnull(sf.adjSF,0),2) + round(ifnull(sa.adjSA,0),2)),0) as 'adjSF%', ifnull(gf.adjGF,0) as adjGF,
        ifnull(ga.adjGA,0) as adjGA, ifnull(round(ifnull(gf.adjGF,0),2) / (round(ifnull(gf.adjGF,0),2) +
        round(ifnull(ga.adjGA,0),2)),0) as 'adjGF%'
    
    FROM rosters r
    
    LEFT OUTER JOIN (select game_id, period, strength
        FROM (SELECT game_id, period, home_strength || 'v' || away_strength as strength
        FROM pbp p
        UNION ALL
        SELECT game_id, period, away_strength || 'v' || home_strength as strength
        FROM pbp p)
        WHERE strength notnull) as strength
    ON r.game_id = strength.game_id
    
    INNER JOIN schedule z
    ON r.game_id = z.game_id
    
    LEFT OUTER JOIN (SELECT p.game_id, p.name, p.team, r.pos, (p.time-1)/1200 + 1 as period, case when 
        p.team = z.home_team then s.home_strength || 'v' || s.away_strength else s.away_strength || 'v' || 
        s.home_strength end strength, round(count(p.name),2)/60 as TOI
    
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
    
        GROUP BY p.game_id, p.name, r.pos, period, strength) as icetime
    ON icetime.name = r.name and icetime.pos = r.pos and icetime.strength = strength.strength
    and strength.period = icetime.period
    
    LEFT OUTER JOIN (SELECT game_id, name, count(name) as G, pos, strength, period
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos, p.game_id, p.period, case when p.p1_team = z.home_team then
        home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
        FROM pbp p
    
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
                and r.team=p.p1_team
                and r.num=p.p1_num)
    
        INNER JOIN schedule z
        ON p.game_id = z.game_id
    
        WHERE p.event_type = 'GOAL')
        GROUP BY name, pos, game_id, strength, period) as goal
    ON goal.name = r.name and goal.pos = r.pos and goal.game_id = r.game_id and goal.strength = strength.strength
    and goal.period = strength.period
    
    LEFT OUTER JOIN (SELECT game_id, name, count(name) as A1, pos, strength, period
        FROM (SELECT r.name, p.p2_team, p.p2_num, r.pos, p.game_id, p.period, case when p.p2_team = z.home_team
        then home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
        FROM pbp p
    
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p2_team
            and r.num=p.p2_num)
    
        INNER JOIN schedule z
        ON p.game_id = z.game_id
    
        WHERE p.event_type = 'GOAL')
        GROUP BY name, pos, game_id, strength, period) as assist1
    ON assist1.name = r.name and assist1.pos = r.pos and assist1.game_id = r.game_id
    and assist1.strength = strength.strength and assist1.period = strength.period
    
    LEFT OUTER JOIN (SELECT game_id, name, count(name) as A2, pos, strength, period
        FROM (SELECT r.name, p.p3_team, p.p3_num, r.pos, p.game_id, p.period, case when p.p3_team = z.home_team
        then home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
        FROM pbp p
    
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p3_team
            and r.num=p.p3_num)
    
        INNER JOIN schedule z
        ON p.game_id = z.game_id
    
        WHERE p.event_type = 'GOAL')
        GROUP BY name, pos, game_id, strength, period) as assist2
    ON assist2.name = r.name and assist2.pos = r.pos and assist2.game_id = r.game_id
    and assist2.strength = strength.strength and assist2.period = strength.period
    
    LEFT OUTER JOIN (SELECT game_id, name, count(name) as SH, pos, strength, period
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos, p.game_id, p.period, case when p.p1_team = z.home_team
        then home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
        FROM pbp p
    
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
    
        INNER JOIN schedule z
        ON p.game_id = z.game_id
    
        WHERE p.event_type = 'SHOT')
        GROUP BY name, pos, game_id, strength, period) as shots
    ON shots.name = r.name and shots.pos = r.pos and shots.game_id = r.game_id and shots.strength = strength.strength
    and shots.period = strength.period
    
    LEFT OUTER JOIN (SELECT game_id, name, count(name) as MISS, pos, strength, period
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos, p.game_id, p.period, case when p.p1_team = z.home_team
        then home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
        FROM pbp p
    
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
    
        INNER JOIN schedule z
        ON p.game_id = z.game_id
    
        WHERE p.event_type = 'MISS')
        GROUP BY name, pos, game_id, strength, period) as missed
    ON missed.name = r.name and missed.pos = r.pos and missed.game_id = r.game_id and missed.strength = 
    strength.strength and missed.period = strength.period
    
    LEFT OUTER JOIN (SELECT game_id, name, count(name) as BLOCK, pos, strength, period
        FROM (SELECT r.name, p.p2_team, p.p2_num, r.pos, p.game_id, p.period, case when p.p2_team = z.home_team
        then home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
        FROM pbp p
    
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p2_team
            and r.num=p.p2_num)
    
        INNER JOIN schedule z
        ON p.game_id = z.game_id
    
        WHERE p.event_type = 'BLOCK')
        GROUP BY name, pos, game_id, strength, period) as blocked
    ON blocked.name = r.name and blocked.pos = r.pos and blocked.game_id = r.game_id
    and blocked.strength = strength.strength and blocked.period = strength.period
    
    LEFT OUTER JOIN (SELECT game_id, name, sum(penl_length) as PIM, pos, strength, period
        FROM (SELECT r.name, p.p1_team, p.p1_num, p.penl_length, r.pos, p.game_id, p.period, case when p.p1_team =
        z.home_team then home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
        FROM pbp p
    
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
    
        INNER JOIN schedule z
        ON p.game_id = z.game_id
    
        WHERE p.event_type = 'PENL')
        GROUP BY name, pos, game_id, strength, period) as pim
    ON pim.name = r.name and pim.pos = r.pos and pim.game_id = r.game_id and pim.strength = strength.strength
    and pim.period = strength.period
    
    LEFT OUTER JOIN (SELECT game_id, name, count(name) as PENL_Taken, pos, strength, period
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos, p.game_id, p.period, case when p.p1_team = z.home_team
        then home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
        FROM pbp p
    
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
    
        INNER JOIN schedule z
        ON p.game_id = z.game_id
    
        WHERE p.event_type = 'PENL')
        GROUP BY name, pos, game_id, strength, period) as penl
    ON penl.name = r.name and penl.pos = r.pos and penl.game_id = r.game_id and penl.strength = strength.strength
    and penl.period = strength.period
    
    LEFT OUTER JOIN (SELECT game_id, name, count(name) as Minor, pos, strength, period
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos, p.game_id, p.period, case when p.p1_team = z.home_team
        then home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
        FROM pbp p
    
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
    
        INNER JOIN schedule z
        ON p.game_id = z.game_id
    
        WHERE p.event_type = 'PENL' and p.penl_length = 2)
        GROUP BY name, pos, game_id, strength, period) as min
    ON min.name = r.name and min.pos = r.pos and min.game_id = r.game_id and min.strength = strength.strength
    and min.period = strength.period
    
    LEFT OUTER JOIN (SELECT game_id, name, count(name) as Major, pos, strength, period
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos, p.game_id, p.period, case when p.p1_team = z.home_team
        then home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
        FROM pbp p
    
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
    
        INNER JOIN schedule z
        ON p.game_id = z.game_id
    
        WHERE p.event_type = 'PENL' and p.penl_length = 5)
        GROUP BY name, pos, game_id, strength, period) as maj
    ON maj.name = r.name and maj.pos = r.pos and maj.game_id = r.game_id and maj.strength = strength.strength
    and maj.period = strength.period
    
    LEFT OUTER JOIN (SELECT game_id, name, count(name) as Misconduct, pos, strength, period
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos, p.game_id, p.period, case when p.p1_team = z.home_team
        then home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
        FROM pbp p
    
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
    
        INNER JOIN schedule z
        ON p.game_id = z.game_id
    
        WHERE p.event_type = 'PENL' and p.penl_length = 10)
        GROUP BY name, pos, game_id, strength, period) as misc
    ON misc.name = r.name and misc.pos = r.pos and misc.game_id = r.game_id and misc.strength = strength.strength
    and misc.period = strength.period
    
    LEFT OUTER JOIN (SELECT game_id, name, count(name) as PENL_Drawn, pos, strength, period
        FROM (SELECT r.name, p.p2_team, p.p2_num, r.pos, p.game_id, p.period, case when p.p2_team = z.home_team
        then home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
        FROM pbp p
    
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p2_team
            and r.num=p.p2_num)
    
        INNER JOIN schedule z
        ON p.game_id = z.game_id
    
        WHERE p.event_type = 'PENL')
        GROUP BY name, pos, game_id, strength, period) as pend
    ON pend.name = r.name and pend.pos = r.pos and pend.game_id = r.game_id and pend.strength = strength.strength
    and pend.period = strength.period
    
    LEFT OUTER JOIN (SELECT game_id, name, count(name) as Giveaways, pos, strength, period
            FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos, p.game_id, p.period, case when p.p1_team = z.home_team
            then home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
            FROM pbp p
    
            INNER JOIN rosters r
            ON (r.game_id = p.game_id
                and r.team=p.p1_team
                and r.num=p.p1_num)
    
            INNER JOIN schedule z
            ON p.game_id = z.game_id
    
            WHERE p.event_type = 'GIVE')
            GROUP BY name, pos, game_id, strength, period) as give
    ON give.name = r.name and give.pos = r.pos and give.game_id = r.game_id and give.strength = strength.strength
    and give.period = strength.period
    
    LEFT OUTER JOIN (SELECT game_id, name, count(name) as Takeaways, pos, strength, period
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos, p.game_id, p.period, case when p.p1_team = z.home_team
        then home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
        FROM pbp p
    
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
    
        INNER JOIN schedule z
        ON p.game_id = z.game_id
    
        WHERE p.event_type = 'TAKE')
        GROUP BY name, pos, game_id, strength, period) as take
    ON take.name = r.name and take.pos = r.pos and take.game_id = r.game_id and take.strength = strength.strength
    and take.period = strength.period
    
    LEFT OUTER JOIN (SELECT game_id, name, count(name) as Hits, pos, strength, period
            FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos, p.game_id, p.period, case when p.p1_team = z.home_team
            then home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
            FROM pbp p
    
            INNER JOIN rosters r
            ON (r.game_id = p.game_id
                and r.team=p.p1_team
                and r.num=p.p1_num)
    
            INNER JOIN schedule z
            ON p.game_id = z.game_id
    
            WHERE p.event_type = 'HIT')
            GROUP BY name, pos, game_id, strength, period) as hit
    ON hit.name = r.name and hit.pos = r.pos and hit.game_id = r.game_id and hit.strength = strength.strength
    and hit.period = strength.period
    
    LEFT OUTER JOIN (SELECT game_id, name, count(name) as Hits_Taken, pos, strength, period
        FROM (SELECT r.name, p.p2_team, p.p2_num, r.pos, p.game_id, p.period, case when p.p2_team = z.home_team
        then home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
        FROM pbp p
    
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p2_team
            and r.num=p.p2_num)
    
        INNER JOIN schedule z
        ON p.game_id = z.game_id
    
        WHERE p.event_type = 'HIT')
        GROUP BY name, pos, game_id, strength, period) as hittaken
    ON hittaken.name = r.name and hittaken.pos = r.pos and hittaken.game_id = r.game_id
    and hittaken.strength = strength.strength and hittaken.period = strength.period
    
    LEFT OUTER JOIN (SELECT game_id, name, count(name) as Shot_Blocks, pos, strength, period
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos, p.game_id, p.period, case when p.p1_team = z.home_team
        then home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
        FROM pbp p
    
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
    
        INNER JOIN schedule z
        ON p.game_id = z.game_id
    
        WHERE p.event_type = 'BLOCK')
        GROUP BY name, pos, game_id, strength, period) as blocksfor
    ON blocksfor.name = r.name and blocksfor.pos = r.pos and blocksfor.game_id = r.game_id
    and blocksfor.strength = strength.strength and blocksfor.period = strength.period
    
    LEFT OUTER JOIN (SELECT game_id, name, count(name) as Faceoffs_Won, pos, strength, period
        FROM (SELECT r.name, p.p1_team, p.p1_num, r.pos, p.game_id, p.period, case when p.p1_team = z.home_team
        then home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
        FROM pbp p
    
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p1_team
            and r.num=p.p1_num)
    
        INNER JOIN schedule z
        ON p.game_id = z.game_id
    
        WHERE p.event_type = 'FAC')
        GROUP BY name, pos, game_id, strength, period) as faceoffwon
    ON faceoffwon.name = r.name and faceoffwon.pos = r.pos and faceoffwon.game_id = r.game_id
    and faceoffwon.strength = strength.strength and faceoffwon.period = strength.period
    
    LEFT OUTER JOIN (SELECT game_id, name, count(name) as Faceoffs_Lost, pos, strength, period
        FROM (SELECT r.name, p.p2_team, p.p2_num, r.pos, p.game_id, p.period, case when p.p2_team = z.home_team
        then home_strength || 'v' || away_strength else away_strength || 'v' ||  home_strength end strength
        FROM pbp p
    
        INNER JOIN rosters r
        ON (r.game_id = p.game_id
            and r.team=p.p2_team
            and r.num=p.p2_num)
    
        INNER JOIN schedule z
        ON p.game_id = z.game_id
    
        WHERE p.event_type = 'FAC')
        GROUP BY name, pos, game_id, strength, period) as faceofflost
    ON faceofflost.name = r.name and faceofflost.pos = r.pos and faceofflost.game_id = r.game_id
    and faceofflost.strength = strength.strength and faceofflost.period = strength.period
    
    LEFT OUTER JOIN(SELECT p.game_id, p.name as name, p.pos as pos, SUM(p.CF) as adjCF, strength, period, COUNT(p.name) 
    as CF
    
        FROM (SELECT p.game_id, event_type, p1_team, p2_team, p.name as name, p.pos as pos, p.team as team, strength, 
        period
                , CF
    
            FROM (SELECT p.game_id, event_type, p1_team, p2_team, p.name, r.Team as team, p.pos, p.period,
                    case when (event_type = 'GOAL' or event_type='MISS' or event_type='SHOT') and p.p1_team = 
                    z.home_team then home_strength || 'v' || away_strength
                    when (event_type = 'GOAL' or event_type='MISS' or event_type='SHOT') and p.p1_team = z.away_team
                    then away_strength || 'v' ||  home_strength
                    when (event_type = 'BLOCK') and p.p2_team = z.home_team then home_strength || 'v' || away_strength
                    else away_strength || 'v' ||  home_strength end strength,
                    case when (event_type = 'GOAL' or event_type='MISS' or event_type='SHOT') and p.p1_team = 
                    z.home_team then 1 * home_adjustment
                    when (event_type = 'GOAL' or event_type='MISS' or event_type='SHOT') and p.p1_team = z.away_team
                    then 1 * away_adjustment
                    when (event_type = 'BLOCK') and p.p2_team = z.home_team then 1 * home_adjustment
                    else 1 * away_adjustment end CF
    
                FROM (SELECT game_id, event_type, p1_team, p2_team, H1Name as name, H1Num as num, H1Pos as pos, period,
                        home_strength, away_strength, home_adjustment, away_adjustment
    
                    FROM pbp
    
                    WHERE H1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p2_team, H2Name, H2Num, H2Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p2_team, H3Name, H3Num, H3Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p2_team, H4Name, H4Num, H4Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p2_team, H5Name, H5Num, H5Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p2_team, H6Name, H6Num, H6Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H6Pos != 'G'
                    UNION ALL
    
                    SELECT game_id, event_type, p1_team, p2_team, A1Name, A1Num, A1Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p2_team, A2Name, A2Num, A2Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p2_team, A3Name, A3Num, A3Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p2_team, A4Name, A4Num, A4Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p2_team, A5Name, A5Num, A5Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p2_team, A6Name, A6Num, A6Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A6Pos != 'G'
                    ) as p
    
                INNER JOIN rosters r
                ON (r.game_id=p.game_id
                    and r.name=p.name
                    and r.num=p.num)
    
                INNER JOIN schedule z
                ON p.game_id = z.game_id
                ) as p
    
            WHERE (event_type='GOAL' and p1_team=p.team)
                or (event_type='MISS' and p1_team=p.team)
                or (event_type='SHOT' and p1_team=p.team)
                or (event_type='BLOCK' and p2_team=p.team)
                ) as p
    
        GROUP BY game_id, p.name, p.pos, period, strength) as cf
    ON CF.name=r.name and cf.pos=r.pos and cf.strength = strength.strength and cf.period = strength.period
    
    LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, SUM(p.CA) as adjCA, strength, period, COUNT(p.name) as C
    
        FROM (SELECT p.game_id, event_type, p1_team, p4_team, p.name as name, p.pos as pos, p.team as team, strength, 
        period, CA
    
            FROM (SELECT p.game_id, event_type, p1_team, p4_team, p.name, r.Team as team, p.pos, p.period,
                    case when (event_type = 'GOAL' or event_type='MISS' or event_type='SHOT') and p.p4_team = 
                    z.home_team then home_strength || 'v' || away_strength
                    when (event_type = 'GOAL' or event_type='MISS' or event_type='SHOT') and p.p4_team = z.away_team
                    then away_strength || 'v' ||  home_strength
                    when (event_type = 'BLOCK') and p.p1_team = z.home_team then home_strength || 'v' || away_strength
                    else away_strength || 'v' ||  home_strength end strength,
                    case when (event_type = 'GOAL' or event_type='MISS' or event_type='SHOT') and p.p4_team = 
                    z.home_team then 1 * away_adjustment
                    when (event_type = 'GOAL' or event_type='MISS' or event_type='SHOT') and p.p4_team = z.away_team
                    then 1 * home_adjustment
                    when (event_type = 'BLOCK') and p.p1_team = z.home_team then 1 * away_adjustment
                    else 1 * home_adjustment end CA
                FROM
                    (SELECT game_id, event_type, p1_team, p4_team, H1Name as name, H1Num as num, H1Pos as pos, period,
                        home_strength, away_strength, home_adjustment, away_adjustment
    
                    FROM pbp
    
                    WHERE H1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p4_team, H2Name, H2Num, H2Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p4_team, H3Name, H3Num, H3Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p4_team, H4Name, H4Num, H4Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p4_team, H5Name, H5Num, H5Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p4_team, H6Name, H6Num, H6Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H6Pos != 'G'
                    UNION ALL
    
                    SELECT game_id, event_type, p1_team, p4_team, A1Name, A1Num, A1Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p4_team, A2Name, A2Num, A2Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p4_team, A3Name, A3Num, A3Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p4_team, A4Name, A4Num, A4Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p4_team, A5Name, A5Num, A5Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, p4_team, A6Name, A6Num, A6Pos, period, home_strength, 
                    away_strength, home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A6Pos != 'G'
                    ) as p
    
                INNER JOIN rosters r
                ON (r.game_id=p.game_id
                    and r.name=p.name
                    and r.num=p.num)
    
                INNER JOIN schedule z
                ON p.game_id = z.game_id
                ) as p
    
            WHERE (event_type='GOAL' and p4_team=p.team)
                or (event_type='MISS' and p4_team=p.team)
                or (event_type='SHOT' and p4_team=p.team)
                or (event_type='BLOCK' and p1_team=p.team)
            ) as p
    
        GROUP BY p.name, p.pos, period, strength) as ca
    ON CA.name=r.name and CA.pos=r.pos and ca.strength = strength.strength and ca.period = strength.period
    
    LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, SUM(p.FF) as adjFF, strength, period, COUNT(p.name) as FF
    
        FROM (SELECT p.game_id, event_type, p1_team, p.name as name, p.pos as pos, p.team as team, strength, period, FF
    
            FROM (SELECT p.game_id, event_type, p1_team, p.name, r.Team as team, p.pos, p.period,
                    case when p.p1_team = z.home_team then home_strength || 'v' || away_strength
                    else away_strength || 'v' ||  home_strength end strength,
                    case when p.p1_team = z.home_team then 1 * home_adjustment
                    else 1 * away_adjustment end FF
    
                FROM (SELECT game_id, event_type, p1_team, H1Name as name, H1Num as num, H1Pos as pos, period,
                        home_strength, away_strength, home_adjustment, away_adjustment
    
                    FROM pbp
    
                    WHERE H1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, H2Name, H2Num, H2Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, H3Name, H3Num, H3Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, H4Name, H4Num, H4Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, H5Name, H5Num, H5Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, H6Name, H6Num, H6Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H6Pos != 'G'
                    UNION ALL
    
                    SELECT game_id, event_type, p1_team, A1Name, A1Num, A1Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, A2Name, A2Num, A2Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, A3Name, A3Num, A3Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, A4Name, A4Num, A4Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, A5Name, A5Num, A5Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, A6Name, A6Num, A6Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A6Pos != 'G'
                    ) as p
    
                INNER JOIN rosters r
                ON (r.game_id=p.game_id
                    and r.name=p.name
                    and r.num=p.num)
    
                INNER JOIN schedule z
                ON p.game_id = z.game_id
                ) as p
    
            WHERE (event_type='GOAL' and p1_team=p.team)
                or (event_type='MISS' and p1_team=p.team)
                or (event_type='SHOT' and p1_team=p.team)
            ) as p
    
        GROUP BY p.name, p.pos, period, strength) as ff
    ON FF.name=r.name and FF.pos=r.pos and ff.strength = strength.strength and ff.period = strength.period
    
    LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, SUM(p.FA) as adjFA, strength, period, COUNT(p.name) as FA
    
        FROM (SELECT p.game_id, event_type, p4_team, p.name as name, p.pos as pos, p.team as team, strength, period, FA
    
            FROM (SELECT p.game_id, event_type, p4_team, p.name, r.Team as team, p.pos, p.period,
                    case when p.p4_team = z.home_team then home_strength || 'v' || away_strength
                    else away_strength || 'v' ||  home_strength end strength,
                    case when p.p4_team = z.home_team then 1 * away_adjustment
                    else 1 * home_adjustment end FA
    
                FROM (SELECT game_id, event_type, p4_team, H1Name as name, H1Num as num, H1Pos as pos, period,
                        home_strength, away_strength, home_adjustment, away_adjustment
    
                    FROM pbp
    
                    WHERE H1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, H2Name, H2Num, H2Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, H3Name, H3Num, H3Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, H4Name, H4Num, H4Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, H5Name, H5Num, H5Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, H6Name, H6Num, H6Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H6Pos != 'G'
                    UNION ALL
    
                    SELECT game_id, event_type, p4_team, A1Name, A1Num, A1Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, A2Name, A2Num, A2Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, A3Name, A3Num, A3Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, A4Name, A4Num, A4Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, A5Name, A5Num, A5Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, A6Name, A6Num, A6Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A6Pos != 'G'
                    ) as p
    
                INNER JOIN rosters r
                ON (r.game_id=p.game_id
                    and r.name=p.name
                    and r.num=p.num)
    
                INNER JOIN schedule z
                ON p.game_id = z.game_id
                ) as p
    
            WHERE (event_type='GOAL' and p4_team=p.team)
                or (event_type='MISS' and p4_team=p.team)
                or (event_type='SHOT' and p4_team=p.team)
            ) as p
    
        GROUP BY p.name, p.pos, period, strength) as fa
    ON FA.name=r.name and FA.pos=r.pos and fa.strength = strength.strength and fa.period = strength.period
    
    LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, SUM(p.SF) as adjSF, strength, period, COUNT(p.name) as SF
    
        FROM (SELECT p.game_id, event_type, p1_team, p.name as name, p.pos as pos, p.team as team, strength, period, SF
    
            FROM (SELECT p.game_id, event_type, p1_team, p.name, r.Team as team, p.pos, p.period,
                    case when p.p1_team = z.home_team then home_strength || 'v' || away_strength
                    else away_strength || 'v' ||  home_strength end strength,
                    case when p.p1_team = z.home_team then 1 * home_adjustment
                    else 1 * away_adjustment end SF
    
                FROM (SELECT game_id, event_type, p1_team, H1Name as name, H1Num as num, H1Pos as pos, period,
                        home_strength, away_strength, home_adjustment, away_adjustment
    
                    FROM pbp
    
                    WHERE H1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, H2Name, H2Num, H2Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, H3Name, H3Num, H3Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, H4Name, H4Num, H4Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, H5Name, H5Num, H5Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, H6Name, H6Num, H6Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H6Pos != 'G'
                    UNION ALL
    
                    SELECT game_id, event_type, p1_team, A1Name, A1Num, A1Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, A2Name, A2Num, A2Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, A3Name, A3Num, A3Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, A4Name, A4Num, A4Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, A5Name, A5Num, A5Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, A6Name, A6Num, A6Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A6Pos != 'G'
                    ) as p
    
                INNER JOIN rosters r
                ON (r.game_id=p.game_id
                    and r.name=p.name
                    and r.num=p.num)
    
                INNER JOIN schedule z
                ON p.game_id = z.game_id
                ) as p
    
            WHERE (event_type='GOAL' and p1_team=p.team)
                or (event_type='SHOT' and p1_team=p.team)
            ) as p
    
        GROUP BY p.name, p.pos, period, strength) as sf
    ON SF.name=r.name and SF.pos=r.pos and sf.strength = strength.strength and sf.period = strength.period
    
    LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, SUM(p.SA) as adjSA, strength, period, COUNT(p.name) as SA
    
        FROM (SELECT p.game_id, event_type, p4_team, p.name as name, p.pos as pos, p.team as team, strength, period, SA
    
            FROM (SELECT p.game_id, event_type, p4_team, p.name, r.Team as team, p.pos, p.period,
                    case when p.p4_team = z.home_team then home_strength || 'v' || away_strength
                    else away_strength || 'v' ||  home_strength end strength,
                    case when p.p4_team = z.home_team then 1 * away_adjustment
                    else 1 * home_adjustment end SA
    
                FROM (SELECT game_id, event_type, p4_team, H1Name as name, H1Num as num, H1Pos as pos, period,
                        home_strength, away_strength, home_adjustment, away_adjustment
    
                    FROM pbp
    
                    WHERE H1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, H2Name, H2Num, H2Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, H3Name, H3Num, H3Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, H4Name, H4Num, H4Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, H5Name, H5Num, H5Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, H6Name, H6Num, H6Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H6Pos != 'G'
                    UNION ALL
    
                    SELECT game_id, event_type, p4_team, A1Name, A1Num, A1Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, A2Name, A2Num, A2Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, A3Name, A3Num, A3Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, A4Name, A4Num, A4Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, A5Name, A5Num, A5Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, A6Name, A6Num, A6Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A6Pos != 'G'
                    ) as p
    
                INNER JOIN rosters r
                ON (r.game_id=p.game_id
                    and r.name=p.name
                    and r.num=p.num)
                INNER JOIN schedule z
                ON p.game_id = z.game_id
                ) as p
    
            WHERE (event_type='GOAL' and p4_team=p.team)
                or (event_type='SHOT' and p4_team=p.team)
            ) as p
    
        GROUP BY p.name, p.pos, period, strength) as sa
    ON SA.name=r.name and SA.pos=r.pos and sa.strength = strength.strength and sa.period = strength.period
    
    LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, SUM(p.GF) as adjGF, strength, period, COUNT(p.name) as GF
    
        FROM (SELECT p.game_id, event_type, p1_team, p.name as name, p.pos as pos, p.team as team, strength, period, GF
    
            FROM (SELECT p.game_id, event_type, p1_team, p.name, r.Team as team, p.pos, p.period,
                    case when p.p1_team = z.home_team then home_strength || 'v' || away_strength
                    else away_strength || 'v' ||  home_strength end strength,
                    case when p.p1_team = z.home_team then 1 * home_adjustment
                    else 1 * away_adjustment end GF
    
                FROM (SELECT game_id, event_type, p1_team, H1Name as name, H1Num as num, H1Pos as pos, period,
                        home_strength, away_strength, home_adjustment, away_adjustment
    
                    FROM pbp
    
                    WHERE H1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, H2Name, H2Num, H2Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, H3Name, H3Num, H3Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, H4Name, H4Num, H4Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, H5Name, H5Num, H5Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, H6Name, H6Num, H6Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H6Pos != 'G'
                    UNION ALL
    
                    SELECT game_id, event_type, p1_team, A1Name, A1Num, A1Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, A2Name, A2Num, A2Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, A3Name, A3Num, A3Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, A4Name, A4Num, A4Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, A5Name, A5Num, A5Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p1_team, A6Name, A6Num, A6Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A6Pos != 'G'
                    ) as p
    
                INNER JOIN rosters r
                ON (r.game_id=p.game_id
                    and r.name=p.name
                    and r.num=p.num)
    
                INNER JOIN schedule z
                ON p.game_id = z.game_id
                ) as p
    
            WHERE (event_type='GOAL' and p1_team=p.team)
            ) as p
    
        GROUP BY p.name, p.pos, period, strength) as gf
    ON GF.name=r.name and GF.pos=r.pos and gf.strength = strength.strength and gf.period = strength.period
    
    LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, SUM(p.GA) as adjGA, strength, period, COUNT(p.name) as GA
    
        FROM (SELECT p.game_id, event_type, p4_team, p.name as name, p.pos as pos, p.team as team, strength, period, GA
    
            FROM (SELECT p.game_id, event_type, p4_team, p.name, r.Team as team, p.pos, p.period,
                    case when p.p4_team = z.home_team then home_strength || 'v' || away_strength
                    else away_strength || 'v' ||  home_strength end strength,
                    case when p.p4_team = z.home_team then 1 * away_adjustment
                    else 1 * home_adjustment end GA
    
                FROM (SELECT game_id, event_type, p4_team, H1Name as name, H1Num as num, H1Pos as pos, period,
                        home_strength, away_strength, home_adjustment, away_adjustment
    
                    FROM pbp
    
                    WHERE H1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, H2Name, H2Num, H2Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, H3Name, H3Num, H3Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, H4Name, H4Num, H4Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, H5Name, H5Num, H5Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, H6Name, H6Num, H6Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE H6Pos != 'G'
                    UNION ALL
    
                    SELECT game_id, event_type, p4_team, A1Name, A1Num, A1Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, A2Name, A2Num, A2Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, A3Name, A3Num, A3Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, A4Name, A4Num, A4Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, A5Name, A5Num, A5Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, p4_team, A6Name, A6Num, A6Pos, period, home_strength, away_strength,
                    home_adjustment, away_adjustment
                    FROM pbp
                    WHERE A6Pos != 'G'
                    ) as p
    
                INNER JOIN rosters r
                ON (r.game_id=p.game_id
                    and r.name=p.name
                    and r.num=p.num)
    
                INNER JOIN schedule z
                ON p.game_id = z.game_id
                ) as p
    
            WHERE (event_type='GOAL' and p4_team=p.team)
            ) as p
    
        GROUP BY p.name, p.pos, period, strength) as ga
    ON GA.name=r.name and GA.pos=r.pos and ga.strength = strength.strength and ga.period = strength.period
    
    LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, SUM(p.name) as OZ_Faceoffs, strength, period
    
        FROM (SELECT p.game_id, event_type, p.zone, p.name as name, p.pos as pos, p.team as team, strength, period
    
            FROM (SELECT p.game_id, event_type, p.zone, p.name, r.Team as team, p.pos, p.period,
                    case when r.Team = z.home_team then home_strength || 'v' || away_strength
                    else away_strength || 'v' ||  home_strength end strength
    
                FROM (SELECT game_id, event_type, home_zone as zone, H1Name as name, H1Num as num, H1Pos as pos, period,
                        home_strength, away_strength
    
                    FROM pbp
    
                    WHERE H1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H2Name, H2Num, H2Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE H2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H3Name, H3Num, H3Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE H3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H4Name, H4Num, H4Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE H4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H5Name, H5Num, H5Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE H5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H6Name, H6Num, H6Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE H6Pos != 'G'
                    UNION ALL
    
                    SELECT game_id, event_type, away_zone, A1Name, A1Num, A1Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE A1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A2Name, A2Num, A2Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE A2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A3Name, A3Num, A3Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE A3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A4Name, A4Num, A4Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE A4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A5Name, A5Num, A5Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE A5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A6Name, A6Num, A6Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE A6Pos != 'G'
                    ) as p
    
                INNER JOIN rosters r
                ON (r.game_id=p.game_id
                    and r.name=p.name
                    and r.num=p.num)
    
                INNER JOIN schedule z
                ON p.game_id = z.game_id
                ) as p
    
            WHERE (event_type='FAC' and zone ='OZ')
            ) as p
    
        GROUP BY p.name, p.pos, period, strength) as oz
    ON oz.name=r.name and oz.pos=r.pos and oz.strength = strength.strength and oz.period = strength.period
    
    LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, COUNT(p.name) as DZ_Faceoffs, strength, period
    
        FROM (SELECT p.game_id, event_type, p.zone, p.name as name, p.pos as pos, p.team as team, strength, period
    
            FROM (SELECT p.game_id, event_type, p.zone, p.name, r.Team as team, p.pos, p.period,
                    case when r.Team = z.home_team then home_strength || 'v' || away_strength
                    else away_strength || 'v' ||  home_strength end strength
    
                FROM (SELECT game_id, event_type, home_zone as zone, H1Name as name, H1Num as num, H1Pos as pos, period,
                        home_strength, away_strength
    
                    FROM pbp
    
                    WHERE H1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H2Name, H2Num, H2Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE H2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H3Name, H3Num, H3Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE H3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H4Name, H4Num, H4Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE H4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H5Name, H5Num, H5Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE H5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H6Name, H6Num, H6Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE H6Pos != 'G'
                    UNION ALL
    
                    SELECT game_id, event_type, away_zone, A1Name, A1Num, A1Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE A1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A2Name, A2Num, A2Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE A2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A3Name, A3Num, A3Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE A3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A4Name, A4Num, A4Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE A4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A5Name, A5Num, A5Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE A5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A6Name, A6Num, A6Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE A6Pos != 'G'
                    ) as p
    
                INNER JOIN rosters r
                ON (r.game_id=p.game_id
                    and r.name=p.name
                    and r.num=p.num)
    
                INNER JOIN schedule z
                ON p.game_id = z.game_id
                ) as p
    
            WHERE (event_type='FAC' and zone ='DZ')
            ) as p
    
        GROUP BY p.name, p.pos, period, strength) as dz
    ON dz.name=r.name and dz.pos=r.pos and dz.strength = strength.strength and dz.period = strength.period
    
    LEFT OUTER JOIN(SELECT p.name as name, p.pos as pos, COUNT(p.name) as NZ_Faceoffs, strength, period
    
        FROM (SELECT p.game_id, event_type, p.zone, p.name as name, p.pos as pos, p.team as team, strength, period
    
            FROM (SELECT p.game_id, event_type, p.zone, p.name, r.Team as team, p.pos, p.period,
                    case when r.Team = z.home_team then home_strength || 'v' || away_strength
                    else away_strength || 'v' ||  home_strength end strength
    
                FROM (SELECT game_id, event_type, home_zone as zone, H1Name as name, H1Num as num, H1Pos as pos, period,
                        home_strength, away_strength
    
                    FROM pbp
    
                    WHERE H1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H2Name, H2Num, H2Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE H2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H3Name, H3Num, H3Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE H3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H4Name, H4Num, H4Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE H4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H5Name, H5Num, H5Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE H5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, home_zone, H6Name, H6Num, H6Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE H6Pos != 'G'
                    UNION ALL
    
                    SELECT game_id, event_type, away_zone, A1Name, A1Num, A1Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE A1Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A2Name, A2Num, A2Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE A2Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A3Name, A3Num, A3Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE A3Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A4Name, A4Num, A4Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE A4Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A5Name, A5Num, A5Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE A5Pos != 'G'
                    UNION ALL
                    SELECT game_id, event_type, away_zone, A6Name, A6Num, A6Pos, period, home_strength, away_strength
                    FROM pbp
                    WHERE A6Pos != 'G'
                    ) as p
    
                INNER JOIN rosters r
                ON (r.game_id=p.game_id
                    and r.name=p.name
                    and r.num=p.num)
    
                INNER JOIN schedule z
                ON p.game_id = z.game_id
                ) as p
    
            WHERE (event_type='FAC' and zone ='NZ')
            ) as p
    
        GROUP BY p.name, p.pos, period, strength) as nz
    ON nz.name=r.name and nz.pos=r.pos and nz.strength = strength.strength and nz.period = strength.period
    
    WHERE r.pos != 'G' AND r.scratch is null AND icetime.TOI !=0
    
    GROUP BY z.game_id, r.name, r.pos, strength.strength, strength.period;
    ''', conn)
    return skaters_stats


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
    cur.executescript('DROP TABLE IF EXISTS shifts_by_sec_temp')

    # find way to update stats table with only newly scraped games
    # skater_stats().to_sql('skaters_on_ice_counts', conn, if_exists='replace', index=False)


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
    skaters_stats().to_csv('skaters_on_ice_counts.csv', index=False)

# update('2018-04-08', '2018-04-08')
skaters_stats().to_sql('skaters', conn, if_exists='append', index=False)
