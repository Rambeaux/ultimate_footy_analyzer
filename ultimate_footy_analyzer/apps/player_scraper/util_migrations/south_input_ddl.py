from south.db import  dbs
from django.db import connection, connections, transaction


class South_Input_DDL:
    
    """
        Insert these methods into forward data migrations like so: 
        
            from decisionscience.apps.input.util_migrations.south_input_ddl import South_Input_DDL
            
            objSP = South_Input_DDL()
            objSP.migrate_0002_views()           

    """
    
    
    
    #######################
    #     Migration Controller
    
    def migrate_0002_views(self):
        
        objdb = dbs['default']
        objdb.start_transaction() 
        self.view_observationtime(objdb)

        self.view_team_summary(objdb)
        self.view_team_player_summary_curr(objdb)
        self.view_player_performance_summary_current(objdb)
        self.view_player_projections_all(objdb) 
        self.view_player_performance_all(objdb)
        self.view_player_status_all(objdb)        
        self.view_player_status_current(objdb)   
        self.view_current_round(objdb)
        
        objdb.commit_transaction() 


        
    def migrate_0002_staging_indexes(self):
        objdb = dbs['default']    
        #drop FK contraints
        self.drop_staging_foreignkeys(objdb,'tbl_playerstatus_stg')
        self.drop_staging_foreignkeys(objdb,'tbl_playerperformance_stg')
        self.drop_staging_foreignkeys(objdb,'tbl_team_stg')
        self.drop_staging_foreignkeys(objdb,'tbl_teamplayer_stg')
        self.drop_staging_foreignkeys(objdb,'tbl_teamplayerposition_stg')
        #drop FK indices
        self.drop_staging_nonpkindexes(objdb,'tbl_playerstatus_stg')
        self.drop_staging_nonpkindexes(objdb,'tbl_playerperformance_stg')
        self.drop_staging_nonpkindexes(objdb,'tbl_team_stg')
        self.drop_staging_nonpkindexes(objdb,'tbl_teamplayer_stg')
        self.drop_staging_nonpkindexes(objdb,'tbl_teamplayerposition_stg')
        #drop primarykeys
        self.drop_staging_primarykey(objdb,'tbl_playerstatus_stg')
        self.drop_staging_primarykey(objdb,'tbl_playerperformance_stg')
        self.drop_staging_primarykey(objdb,'tbl_team_stg')
        self.drop_staging_primarykey(objdb,'tbl_teamplayer_stg')
        self.drop_staging_primarykey(objdb,'tbl_teamplayerposition_stg')

   
    """ ====================================
        Create Views - Non Lookup
        ====================================
    """   
       
    def view_current_round(self, objdb):   
        
        objdb.execute(
"""     
DROP VIEW IF EXISTS view_current_round CASCADE;

CREATE VIEW
view_current_round as

select season, league_id, max(performance_round)+1 current_round
from tbl_playerperformance
where performance_type = 'performance'
group by season, league_id;     
"""      
                    )
        
        
        
    def view_player_status_current(self, objdb):   
        
        objdb.execute(      
      
"""      
DROP VIEW IF EXISTS view_player_status_current CASCADE;

CREATE VIEW
view_player_status_current as
select 
pl.player_id, pl.player_siteid, pl.player_name

,pls.league_id, pls.season
,pls.aflteam
,pls.position, pls.position_B, pls.position_c, pls.position_r, pls.position_f
,pls.observation_round
,pls.named_to_play, pls.named_emergency
,pls.named_interchange, pls.not_playing, pls.injured_suspended, pls.hurt, pls.expected_return_round, pls.out_for_season

from
    tbl_player pl

left join tbl_playerstatus pls
    on pl.player_id = pls.player_id
left join view_current_round cr
    on 1=1 
where cr.current_round = pls.observation_round;
"""      )        
        


    def view_player_status_all(self, objdb):   
        
        objdb.execute(      
      
"""      
DROP VIEW IF EXISTS view_player_status_all CASCADE;

CREATE VIEW
view_player_status_all as
select 
pl.player_id, pl.player_siteid, pl.player_name

,pls.league_id, pls.season
,pls.aflteam
,pls.position, pls.position_B, pls.position_c, pls.position_r, pls.position_f
,pls.observation_round
,pls.named_to_play, pls.named_emergency
,pls.named_interchange, pls.not_playing, pls.injured_suspended, pls.hurt, pls.expected_return_round, pls.out_for_season

from
    tbl_player pl

left join tbl_playerstatus pls
    on pl.player_id = pls.player_id
; 
"""      )   
        
       
      
      
    def view_player_performance_all(self, objdb):   
        
        objdb.execute(      
      
"""      
DROP VIEW IF EXISTS view_player_performance_all CASCADE;

CREATE VIEW view_player_performance_all as
select pl.player_id
,plp.season
,MAX( CASE WHEN plp.performance_round = 1 then points ELSE null END) as points_R1
,MAX( CASE WHEN plp.performance_round = 2 then points ELSE null END) as points_R2
,MAX( CASE WHEN plp.performance_round = 3 then points ELSE null END) as points_R3
,MAX( CASE WHEN plp.performance_round = 4 then points ELSE null END) as points_R4
,MAX( CASE WHEN plp.performance_round = 5 then points ELSE null END) as points_R5
,MAX( CASE WHEN plp.performance_round = 6 then points ELSE null END) as points_R6
,MAX( CASE WHEN plp.performance_round = 7 then points ELSE null END) as points_R7
,MAX( CASE WHEN plp.performance_round = 8 then points ELSE null END) as points_R8
,MAX( CASE WHEN plp.performance_round = 9 then points ELSE null END) as points_R9
,MAX( CASE WHEN plp.performance_round = 10 then points ELSE null END) as points_R10

,MAX( CASE WHEN plp.performance_round = 11 then points ELSE null END) as points_R11
,MAX( CASE WHEN plp.performance_round = 12 then points ELSE null END) as points_R12
,MAX( CASE WHEN plp.performance_round = 13 then points ELSE null END) as points_R13
,MAX( CASE WHEN plp.performance_round = 14 then points ELSE null END) as points_R14
,MAX( CASE WHEN plp.performance_round = 15 then points ELSE null END) as points_R15
,MAX( CASE WHEN plp.performance_round = 16 then points ELSE null END) as points_R16
,MAX( CASE WHEN plp.performance_round = 17 then points ELSE null END) as points_R17
,MAX( CASE WHEN plp.performance_round = 18 then points ELSE null END) as points_R18
,MAX( CASE WHEN plp.performance_round = 19 then points ELSE null END) as points_R19
,MAX( CASE WHEN plp.performance_round = 20 then points ELSE null END) as points_R20
,MAX( CASE WHEN plp.performance_round = 21 then points ELSE null END) as points_R21
,MAX( CASE WHEN plp.performance_round = 22 then points ELSE null END) as points_R22
,MAX( CASE WHEN plp.performance_round = 23 then points ELSE null END) as points_R23

,MAX( CASE WHEN plp.performance_round = 1 and played = true then 1 ELSE 0 END) as played_R1
,MAX( CASE WHEN plp.performance_round = 2 and played = true then 1 ELSE 0 END) as played_R2
,MAX( CASE WHEN plp.performance_round = 3 and played = true then 1 ELSE 0 END) as played_R3
,MAX( CASE WHEN plp.performance_round = 4 and played = true then 1 ELSE 0 END) as played_R4
,MAX( CASE WHEN plp.performance_round = 5 and played = true then 1 ELSE 0 END) as played_R5
,MAX( CASE WHEN plp.performance_round = 6 and played = true then 1 ELSE 0 END) as played_R6
,MAX( CASE WHEN plp.performance_round = 7 and played = true then 1 ELSE 0 END) as played_R7
,MAX( CASE WHEN plp.performance_round = 8 and played = true then 1 ELSE 0 END) as played_R8
,MAX( CASE WHEN plp.performance_round = 9 and played = true then 1 ELSE 0 END) as played_R9
,MAX( CASE WHEN plp.performance_round = 10 and played = true then 1 ELSE 0 END) as played_R10

,MAX( CASE WHEN plp.performance_round = 11 and played = true then 1 ELSE 0 END) as played_R11
,MAX( CASE WHEN plp.performance_round = 12 and played = true then 1 ELSE 0 END) as played_R12
,MAX( CASE WHEN plp.performance_round = 13 and played = true then 1 ELSE 0 END) as played_R13
,MAX( CASE WHEN plp.performance_round = 14 and played = true then 1 ELSE 0 END) as played_R14
,MAX( CASE WHEN plp.performance_round = 15 and played = true then 1 ELSE 0 END) as played_R15
,MAX( CASE WHEN plp.performance_round = 16 and played = true then 1 ELSE 0 END) as played_R16
,MAX( CASE WHEN plp.performance_round = 17 and played = true then 1 ELSE 0 END) as played_R17
,MAX( CASE WHEN plp.performance_round = 18 and played = true then 1 ELSE 0 END) as played_R18
,MAX( CASE WHEN plp.performance_round = 19 and played = true then 1 ELSE 0 END) as played_R19
,MAX( CASE WHEN plp.performance_round = 20 and played = true then 1 ELSE 0 END) as played_R20
,MAX( CASE WHEN plp.performance_round = 21 and played = true then 1 ELSE 0 END) as played_R21
,MAX( CASE WHEN plp.performance_round = 22 and played = true then 1 ELSE 0 END) as played_R22
,MAX( CASE WHEN plp.performance_round = 23 and played = true then 1 ELSE 0 END) as played_R23



from
    view_player_status_all pl

left join tbl_playerperformance plp
    on pl.player_id = plp.player_id
    and pl.observation_round = plp.performance_round
    and plp.performance_type = 'performance'

group by pl.player_id
,plp.season; 
"""      )   
               
     
    def view_player_projections_all(self, objdb):   
        
        objdb.execute(         
     """
     
DROP VIEW IF EXISTS view_player_projections_all CASCADE;
     
CREATE VIEW  view_player_projections_all as
select pl.player_id
,plp.season
,plp.prediction_round
,MAX( CASE WHEN plp.performance_round = 12 and plp.prediction_round = plp.performance_round  then points ELSE null END) as predpoints_R12_1
,MAX( CASE WHEN plp.performance_round = 13 and plp.prediction_round = plp.performance_round  then points ELSE null END) as predpoints_R13_1
,MAX( CASE WHEN plp.performance_round = 14 and plp.prediction_round = plp.performance_round  then points ELSE null END) as predpoints_R14_1
,MAX( CASE WHEN plp.performance_round = 15 and plp.prediction_round = plp.performance_round  then points ELSE null END) as predpoints_R15_1
,MAX( CASE WHEN plp.performance_round = 16 and plp.prediction_round = plp.performance_round  then points ELSE null END) as predpoints_R16_1
,MAX( CASE WHEN plp.performance_round = 17 and plp.prediction_round = plp.performance_round  then points ELSE null END) as predpoints_R17_1
,MAX( CASE WHEN plp.performance_round = 18 and plp.prediction_round = plp.performance_round  then points ELSE null END) as predpoints_R18_1
,MAX( CASE WHEN plp.performance_round = 19 and plp.prediction_round = plp.performance_round  then points ELSE null END) as predpoints_R19_1
,MAX( CASE WHEN plp.performance_round = 20 and plp.prediction_round = plp.performance_round  then points ELSE null END) as predpoints_R20_1
,MAX( CASE WHEN plp.performance_round = 21 and plp.prediction_round = plp.performance_round  then points ELSE null END) as predpoints_R21_1
,MAX( CASE WHEN plp.performance_round = 22 and plp.prediction_round = plp.performance_round  then points ELSE null END) as predpoints_R22v
,MAX( CASE WHEN plp.performance_round = 23 and plp.prediction_round = plp.performance_round  then points ELSE null END) as predpoints_R23_1

-- 1 weeks in advance
,MAX( CASE WHEN plp.performance_round = 13 and plp.prediction_round+1 = plp.performance_round  then points ELSE null END) as predpoints_R13_1
,MAX( CASE WHEN plp.performance_round = 14 and plp.prediction_round+1 = plp.performance_round  then points ELSE null END) as predpoints_R14_1
,MAX( CASE WHEN plp.performance_round = 15 and plp.prediction_round+1 = plp.performance_round  then points ELSE null END) as predpoints_R15_1
,MAX( CASE WHEN plp.performance_round = 16 and plp.prediction_round+1 = plp.performance_round  then points ELSE null END) as predpoints_R16_1
,MAX( CASE WHEN plp.performance_round = 17 and plp.prediction_round+1 = plp.performance_round  then points ELSE null END) as predpoints_R17_1
,MAX( CASE WHEN plp.performance_round = 18 and plp.prediction_round+1 = plp.performance_round  then points ELSE null END) as predpoints_R18_1
,MAX( CASE WHEN plp.performance_round = 19 and plp.prediction_round+1 = plp.performance_round  then points ELSE null END) as predpoints_R19_1
,MAX( CASE WHEN plp.performance_round = 20 and plp.prediction_round+1 = plp.performance_round  then points ELSE null END) as predpoints_R20_1
,MAX( CASE WHEN plp.performance_round = 21 and plp.prediction_round+1 = plp.performance_round  then points ELSE null END) as predpoints_R21_1
,MAX( CASE WHEN plp.performance_round = 22 and plp.prediction_round+1 = plp.performance_round  then points ELSE null END) as predpoints_R22_1
,MAX( CASE WHEN plp.performance_round = 23 and plp.prediction_round+1 = plp.performance_round  then points ELSE null END) as predpoints_R23_1

-- 2 weeks in advance
,MAX( CASE WHEN plp.performance_round = 13 and plp.prediction_round+2 = plp.performance_round  then points ELSE null END) as predpoints_R13_2
,MAX( CASE WHEN plp.performance_round = 14 and plp.prediction_round+2 = plp.performance_round  then points ELSE null END) as predpoints_R14_2
,MAX( CASE WHEN plp.performance_round = 15 and plp.prediction_round+2 = plp.performance_round  then points ELSE null END) as predpoints_R15_2
,MAX( CASE WHEN plp.performance_round = 16 and plp.prediction_round+2 = plp.performance_round  then points ELSE null END) as predpoints_R16_2
,MAX( CASE WHEN plp.performance_round = 17 and plp.prediction_round+2 = plp.performance_round  then points ELSE null END) as predpoints_R17_2
,MAX( CASE WHEN plp.performance_round = 18 and plp.prediction_round+2 = plp.performance_round  then points ELSE null END) as predpoints_R18_2
,MAX( CASE WHEN plp.performance_round = 19 and plp.prediction_round+2 = plp.performance_round  then points ELSE null END) as predpoints_R19_2
,MAX( CASE WHEN plp.performance_round = 20 and plp.prediction_round+2 = plp.performance_round  then points ELSE null END) as predpoints_R20_2
,MAX( CASE WHEN plp.performance_round = 21 and plp.prediction_round+2 = plp.performance_round  then points ELSE null END) as predpoints_R21_2
,MAX( CASE WHEN plp.performance_round = 22 and plp.prediction_round+2 = plp.performance_round  then points ELSE null END) as predpoints_R22_2
,MAX( CASE WHEN plp.performance_round = 23 and plp.prediction_round+2 = plp.performance_round  then points ELSE null END) as predpoints_R23_2

-- 3 weeks in advance
,MAX( CASE WHEN plp.performance_round = 13 and plp.prediction_round+3 = plp.performance_round  then points ELSE null END) as predpoints_R13_3
,MAX( CASE WHEN plp.performance_round = 14 and plp.prediction_round+3 = plp.performance_round  then points ELSE null END) as predpoints_R14_3
,MAX( CASE WHEN plp.performance_round = 15 and plp.prediction_round+3 = plp.performance_round  then points ELSE null END) as predpoints_R15_3
,MAX( CASE WHEN plp.performance_round = 16 and plp.prediction_round+3 = plp.performance_round  then points ELSE null END) as predpoints_R16_3
,MAX( CASE WHEN plp.performance_round = 17 and plp.prediction_round+3 = plp.performance_round  then points ELSE null END) as predpoints_R17_3
,MAX( CASE WHEN plp.performance_round = 18 and plp.prediction_round+3 = plp.performance_round  then points ELSE null END) as predpoints_R18_3
,MAX( CASE WHEN plp.performance_round = 19 and plp.prediction_round+3 = plp.performance_round  then points ELSE null END) as predpoints_R19_3
,MAX( CASE WHEN plp.performance_round = 20 and plp.prediction_round+3 = plp.performance_round  then points ELSE null END) as predpoints_R20_3
,MAX( CASE WHEN plp.performance_round = 21 and plp.prediction_round+3 = plp.performance_round  then points ELSE null END) as predpoints_R21_3
,MAX( CASE WHEN plp.performance_round = 22 and plp.prediction_round+3 = plp.performance_round  then points ELSE null END) as predpoints_R22_3
,MAX( CASE WHEN plp.performance_round = 23 and plp.prediction_round+3 = plp.performance_round  then points ELSE null END) as predpoints_R23_3


from
    view_player_status_all pl

left join tbl_playerperformance plp
    on pl.player_id = plp.player_id
    and plp.performance_type = 'prediction'

group by plp.season
,pl.player_id
,plp.prediction_round
order by 
pl.player_id
,plp.prediction_round
;
""" 
   )  

     
    def view_player_performance_summary_current(self, objdb):   
        
        objdb.execute(       
    """
    
DROP VIEW IF EXISTS view_player_performance_summary_current  CASCADE;
    
CREATE VIEW 
view_player_performance_summary_current as

select pls.player_siteid, pls.player_id, pls.league_id, pls.player_name
--pls.aflteam, pls.league_id, pls.season, pls.observation_round
--,pls.position, pls.position_B, pls.position_c, pls.position_r, pls.position_f
, pls.position_b, pls.position_c, pls.position_r, pls.position_f

, plp.sum_points, plp.avg_points, plp.avg_points_l3, plp.avg_points_l5, plp.avg_points_l7 
, plp.games_played, plp.max_points, plp.min_points 
 
, pred.avg_predpoints_n1, pred.avg_predpoints_n3, pred.avg_predpoints_n5 
 

from
view_player_status_current pls

left join
(
select pls1.player_siteid, pls1.league_id,
    ,SUM( CASE WHEN plp1.performance_round <= pls1.observation_round                                     then plp1.points ELSE null END) as sum_points
    ,AVG( CASE WHEN plp1.performance_round <= pls1.observation_round      and plp1.played = true         then plp1.points ELSE null END) as avg_points
    ,AVG( CASE WHEN plp1.performance_round >= pls1.observation_round - 3  and plp1.played = true         then plp1.points ELSE null END) as avg_points_l3
    ,AVG( CASE WHEN plp1.performance_round >= pls1.observation_round - 5  and plp1.played = true         then plp1.points ELSE null END) as avg_points_l5
    ,AVG( CASE WHEN plp1.performance_round >= pls1.observation_round - 7  and plp1.played = true         then plp1.points ELSE null END) as avg_points_l7



    ,SUM( CASE WHEN plp1.performance_round <= pls1.observation_round        and plp1.played = true     then 1             ELSE null END) as games_played
    ,MAX( CASE WHEN plp1.performance_round <= pls1.observation_round                                     then plp1.points ELSE null END) as max_points
    ,MIN( CASE WHEN plp1.performance_round <= pls1.observation_round        and plp1.played = true     then plp1.points ELSE null END) as min_points
--,STDDEV( CASE WHEN plp.performance_round <= pls.observation_round  and plp.played = true     then plp.points ELSE null END) as sd_points
--,STDDEV( CASE WHEN plp.performance_round <= pls.observation_round  and plp.played = true     then plp.points ELSE null END)
-- / AVG( CASE WHEN plp.performance_round <= pls.observation_round   and plp.played = true     then plp.points ELSE null END) as sdpct_points



--,AVG( CASE WHEN plp.performance_round >= pls.observation_round - 3  and plp.played = true then plp.points ELSE null END) 
--/ AVG( CASE WHEN plp.performance_round < pls.observation_round - 3  and plp.played = true then plp.points ELSE null END) as avg_points_L3_vs_PREV
--,AVG( CASE WHEN plp.performance_round >= pls.observation_round - 5  and plp.played = true then plp.points ELSE null END)  
--/ AVG( CASE WHEN plp.performance_round < pls.observation_round - 5  and plp.played = true then plp.points ELSE null END) as avg_points_L5_vs_PREV
--,AVG( CASE WHEN plp.performance_round >= pls.observation_round - 7  and plp.played = true then plp.points ELSE null END) 
--/ AVG( CASE WHEN plp.performance_round < pls.observation_round - 7  and plp.played = true then plp.points ELSE null END) as avg_points_L7_vs_PREV


--,AVG( CASE WHEN  pred.performance_round = pls.observation_round and pred.prediction_round = pls.observation_round then pred.points ELSE null END) as curr_predpoints_N1
--,AVG( CASE WHEN  pred.performance_round = pls.observation_round and pred.prediction_round < pls.observation_round then pred.points ELSE null END) as trend_predpoints_N1



from
view_player_status_current pls1

left join tbl_playerperformance plp1
    on pls1.player_id = plp1.player_id
    and pls1.season = plp1.season
    and pls1.observation_round >= plp1.performance_round
    and plp1.performance_type = 'performance'

    group by pls1.player_siteid

) plp
on plp.player_siteid = pls.player_siteid

left join
(
select pls2.player_siteid

,AVG( CASE WHEN  pred2.performance_round = pls2.observation_round and pred2.prediction_round = pls2.observation_round then pred2.points ELSE null END) as avg_predpoints_n1
,AVG( CASE WHEN pred2.performance_round >= pls2.observation_round and pred2.performance_round < pls2.observation_round + 3   and pred2.prediction_round = pls2.observation_round  then pred2.points ELSE null END) as avg_predpoints_n3
,AVG( CASE WHEN pred2.performance_round >= pls2.observation_round and pred2.performance_round < pls2.observation_round + 5   and pred2.prediction_round = pls2.observation_round  then pred2.points ELSE null END) as avg_predpoints_n5

from
view_player_status_current pls2

left join tbl_playerperformance pred2
    on pred2.player_id = pls2.player_id 
    and pred2.season = pls2.season 
    and pred2.performance_type = 'prediction'
    and pred2.prediction_round = pls2.observation_round

    group by pls2.player_siteid
) pred
on pred.player_siteid = pls.player_siteid;
    
        
    
""")     

     
    def view_team_summary(self, objdb):   
        
        objdb.execute(       
    """
    
DROP VIEW IF EXISTS view_team_summary  CASCADE;
    
CREATE VIEW 
view_team_summary as
select team_name, named_position, named_18, named_emergency, a.observation_round
, count(a.*) as num_players
, sum(avg_points) as tot_avg_points
, sum(avg_points_l3) as tot_avg_points_l3
, avg(avg_points) as avg_points
, avg(avg_points_l3) as avg_points_l3
, avg(avg_points_l3_vs_prev) as avg_points_l3_vs_prev
, avg(avg_points_l5_vs_prev) as avg_points_l5_vs_prev




from (
select 
t.team_name, tpp.named_position, tpp.named_18, tpp.named_emergency, ppsc.*

from
    tbl_team t

--left join tbl_teamplayer tp
--    on t.team_id = t.team_id
    
left join tbl_teamplayerposition tpp
    on t.team_id = tpp.team_id


left join view_player_performance_summary_current ppsc
    on ppsc.player_id = tpp.player_id
    and ppsc.observation_round = tpp.observation_round

) a
group by team_name, named_position, named_18, named_emergency, a.observation_round
order by observation_round, team_name, named_position;
    
""")     
     
    def view_team_player_summary_curr(self, objdb):   
        
        objdb.execute(       
    """
    
DROP VIEW IF EXISTS view_team_player_summary_curr  CASCADE;
    
CREATE VIEW view_team_player_summary_curr as
select 
t.team_name, ppsc.player_name, ppsc.position, tpp.named_position, tpp.named_18, tpp.named_emergency, ppsc.games_played
, ppsc.avg_points
, ppsc.avg_points_l3
, ppsc.avg_points_l5
, ppsc.avg_points_l7
, ppsc.avg_points_l3_vs_prev
, ppsc.avg_points_l5_vs_prev
, ppsc.avg_points_l7_vs_prev

, ppsc.curr_predpoints_N1
, ppsc.trend_predpoints_N1

, ppsc.avg_predpoints_N3
, ppsc.avg_predpoints_N5


from  view_player_performance_summary_current ppsc

left outer join tbl_teamplayerposition tpp
    on ppsc.player_id = tpp.player_id
    and ppsc.observation_round = tpp.observation_round
    
left outer join tbl_team t
    on t.team_id = tpp.team_id 
   
    
""")     
     
        
        
####################
#    ManageStaging Table Indices
        
    #Drop Foreign Keys
    def drop_staging_foreignkeys(self, objdb, tablename):   
        
        objdb.execute(
"""        
        SELECT * FROM usp_drop_fkconstraints('tablename');
""".format(tablename=tablename)
        )   


    #Drop Foreign Key Indexes
    def drop_staging_nonpkindexes(self, objdb, tablename):   
        
        objdb.execute(
"""        
        SELECT * FROM usp_drop_nonpkindexes('tablename');
""".format(tablename=tablename)
        )   
        

    #Drop Primary Key Indexes
    def drop_staging_primarykey(self, objdb, tablename):   
        
        objdb.execute(
"""        
        SELECT * FROM usp_drop_primarykey('tablename');
""".format(tablename=tablename)
        )   
        




        
        

