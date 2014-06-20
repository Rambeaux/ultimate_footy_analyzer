from django.db import connection, connections, transaction

import sys, traceback
import numpy as np

import math
from scipy import stats


#models
from ultimate_footy_analyzer.apps.player_scraper.models import PlayerPerformanceSummary, Team, TeamPlayer, TeamPlayer_stg, League, Player, Player_stg, PlayerStatus, PlayerStatus_stg, PlayerPerformance, PlayerPerformance_stg, TeamPlayerPosition_stg


class UF_TeamOptimiser(object):
    
    #base player object caches
    team_TeamObjects = {}
    team_TeamPlayerObjects = {}
    
    team_playerlists = {}
    
    #positions caches
    team_player_positions = {}
    team_selection_best = {}
    team_selection_temp = {}
    team_selection_curr = {}
    
    #player performances
    player_performance_summary = {}
    
    #league definition
    league_num_in_position = {'B': 5, 'C': 7, 'R': 1, 'F':5}

    #player performance data format
    player_datatype = [('player_id', 'int'), ('player_siteid', 'int'), ('avg', 'float'), ('last5', 'float'), ('next5', 'float'), ('next1', 'float')]


    def initialise_league(self, league_id):
    
        #load all teams for a given league.
        pass
    
    

    def initialise_team(self, team_id):


        self.team_TeamObjects[team_id] = Team.objects.get(team_id=team_id)
        
        self.team_TeamPlayerObjects[team_id] = TeamPlayer.objects.filter(team=self.team_TeamObjects[team_id], active=True)

        self.team_playerlists[team_id] = ''
        
        self.player_cache_siteid = {}

        self.team_player_positions[team_id] = {'B': [], 'C': [], 'R': [], 'F': [],
                                              'CF': [], 'CB': [], 'BF': [],
                                              'RF': [], 'RC': [], 'RB': []}

        
        self.team_selection_best[team_id] = {'B': [], 'C': [], 'R': [], 'F': [], 'BN':[]}
        self.team_selection_temp[team_id] = {'B': [], 'C': [], 'R': [], 'F': [], 'BN':[]}
        self.team_selection_curr[team_id] = {'B': [], 'C': [], 'R': [], 'F': [], 'BN':[]}

    
    
    def get_player_performance(self, team_id):
        
        
        """objTeam = Team.objects.get(team_id=team_id)
        #select_players by team_id
        team_teamplayerobjects = TeamPlayer.objects.filter(team=objTeam, active=True)

        team_players = []
        player_list = ''
        
        league_num_in_position = {'B': 5, 'C': 7, 'R': 1, 'F':5}
        player_datatype = [('player_id', 'int'), ('player_siteid', 'int'), ('avg', 'float'), ('last5', 'float'), ('next5', 'float'), ('next1', 'float')]
        #player_performance_measures = {'avg': {}, 'last5': {}, 'next5': {} , 'next1': {}}

        team_player_positions = {'B': [], 'C': [], 'R': [], 'F': [],
                                  'CF': [], 'CB': [], 'BF': [],
                                  'RF': [], 'RC': [], 'RB': []}

        
        selected_team = {'B': [], 'C': [], 'R': [], 'F': [], 'BN':[]}
        """

        #Build Player Population quesry for the given squad/team.        
        for objTeamPlayer in self.team_TeamPlayerObjects[team_id]:
            
            objPlayer = Player.objects.get(player_id=objTeamPlayer.player_id)
            
            self.player_cache_siteid[objPlayer.player_siteid] = objPlayer
            
            self.team_playerlists[team_id] = self.team_playerlists[team_id] + str(objPlayer.player_siteid) + ','
            
        #clean up trailing comma
        self.team_playerlists[team_id] = self.team_playerlists[team_id][:-1]
                       
        #player peformance query               
        player_perfs = PlayerPerformanceSummary.objects.raw("""select player_siteid, player_id, player_name, sum_points, avg_points, avg_points_l3, avg_points_l5, avg_points_l7,
         avg_predpoints_n1, avg_predpoints_n3, avg_predpoints_n5, games_played, max_points, min_points, position_b, position_c, position_r, position_f
         from player_performance_summary_current_b where player_siteid in ({player_list})""".format(player_list=self.team_playerlists[team_id]))


        #TODO: Get player statuses - named to play, injured etc.

        
        team_player_positions = {}
        
        for objplayerperf in player_perfs:

            player_perf_array = (objplayerperf.player_id, objplayerperf.player_siteid,
                                           objplayerperf.avg_points, objplayerperf.avg_points_l5,
                                           objplayerperf.avg_predpoints_n5, objplayerperf.avg_predpoints_n1 )
            




            self.player_performance_summary[objplayerperf.player_id] = player_perf_array

            #all players eligible for a given position - dual positions listed twice.
            if objplayerperf.position_b == 1:
                self.team_player_positions[team_id]['B'].append(player_perf_array) 
                    
            if objplayerperf.position_f == 1:
                self.team_player_positions[team_id]['F'].append(player_perf_array) 
                    
            if objplayerperf.position_c == 1:
                self.team_player_positions[team_id]['C'].append(player_perf_array) 

            if objplayerperf.position_r == 1:
                self.team_player_positions[team_id]['R'].append(player_perf_array) 

            #dual positions indicators
            if objplayerperf.position_b == 1 and objplayerperf.position_c == 1:
                self.team_player_positions[team_id]['CB'].append(player_perf_array) 

            if objplayerperf.position_b == 1 and objplayerperf.position_f == 1:
                self.team_player_positions[team_id]['BF'].append(player_perf_array) 

            if objplayerperf.position_b == 1 and objplayerperf.position_r == 1:
                self.team_player_positions[team_id]['RB'].append(player_perf_array) 
                
            if objplayerperf.position_c == 1 and objplayerperf.position_f == 1:
                self.team_player_positions[team_id]['CF'].append(player_perf_array) 

            if objplayerperf.position_c == 1 and objplayerperf.position_r == 1:
                self.team_player_positions[team_id]['RC'].append(player_perf_array) 

            if objplayerperf.position_f == 1 and objplayerperf.position_r == 1:
                self.team_player_positions[team_id]['RF'].append(player_perf_array) 
                
        #convert each array to a numpy array        
        for positionkey in self.team_player_positions[team_id].keys():        
            self.team_player_positions[team_id][positionkey] = np.array(self.team_player_positions[team_id][positionkey], self.player_datatype)
            
                
    def select_team_positions(self, team_id, sort_measure):
                
        #initial team selection - by position order
        position_order = ['B','F','C','R']

        #temp team value
        team_value = {}
        team_value['avg'] = 0        
        team_value['last5'] = 0        
        team_value['next5'] = 0        
        team_value['next1'] = 0        

        #sort each position by avg
        for position in self.team_player_positions[team_id].keys():
            
            if len(self.team_player_positions[team_id][position]) > 0:
                np.sort(self.team_player_positions[team_id][position],  order=sort_measure)
                
                


        #initialise team selections by             
        for position in position_order:
            
            for player in self.team_player_positions[team_id][position]:
                #print  player             
                if len(self.team_selection_temp[team_id][position]) < self.league_num_in_position[position]: 
                    self.team_selection_temp[team_id][position].append(player)


            for row in self.team_selection_temp[team_id][position]:
                team_value['avg'] += row[2]
                team_value['last5'] += row[3]
                team_value['next5'] += row[4]
                team_value['next1'] += row[5]


            print  "======={0}=========".format(position)
            print  self.team_selection_temp[team_id][position]



            #TODO: manage the bench.    
            #TODO: manage dual positions    
        
        
        print team_value
        
    
    #def place_place_in_team

