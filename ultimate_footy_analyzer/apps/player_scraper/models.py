#django
from django.db import models

#project
from ultimate_footy_analyzer.libs.Util import *

#system
import sys, traceback
import numpy as np
from datetime import datetime
from struct import pack


"""

"""

class League(models.Model):

    league_id       = models.AutoField(primary_key=True)
    league_siteid   = models.IntegerField()
    
    #updated upon scrape
    league_name     = models.CharField(max_length=10)
    numteams        = models.IntegerField()
    #numbacks        = models.IntegerField(default=5)
    #numcentres      = models.IntegerField(default=7)
    #numrucks        = models.IntegerField(default=1)
    #numforwards     = models.IntegerField(default=5)


    def __unicode__(self):
        return self.league_name

    class Meta:
        db_table = "tbl_league"


class Player_base(models.Model):
    player_id       = models.AutoField(primary_key=True)
    player_siteid   = models.IntegerField(max_length=200)
    player_name     = models.CharField(max_length=100)


    def __unicode__(self):
        return self.player_name

    class Meta:
        abstract = True


class Player(Player_base):

    class Meta:
        db_table = "tbl_player"
        
@BulkLoadable
class Player_stg(Player_base):

    class Meta:
        db_table = "tbl_player_stg"



class PlayerStatus_base(models.Model):
    playerstatus_id = models.AutoField(primary_key=True)
    player = models.ForeignKey(Player)
    league = models.ForeignKey(League)
    season = models.IntegerField()
    observation_round = models.IntegerField()

    #status    
    named_to_play       = models.NullBooleanField(null=True)
    named_emergency     = models.NullBooleanField(null=True)
    named_interchange   = models.NullBooleanField(null=True)
    not_playing         = models.NullBooleanField(null=True)
    injured_suspended   = models.NullBooleanField(null=True)
    hurt                = models.NullBooleanField(null=True)
    expected_return_round = models.IntegerField(null=True)
    out_for_season      = models.NullBooleanField(null=True)
    
    position        = models.CharField(max_length=10)
    position_b        = models.BooleanField()
    position_c        = models.BooleanField()
    position_r        = models.BooleanField()
    position_f        = models.BooleanField()
    aflteam         = models.CharField(max_length=10)
    
    

    def __unicode__(self):
        return self.observation_round

    class Meta:
        abstract = True
        index_together = [["player", "league", "season", "observation_round"],]
        unique_together = ("player", "league", "season", "observation_round")        


class PlayerStatus(PlayerStatus_base):

    class Meta:
        db_table = "tbl_playerstatus"
        
@BulkLoadable
class PlayerStatus_stg(PlayerStatus_base):

    class Meta:
        db_table = "tbl_playerstatus_stg"




class PlayerPerformance_base(models.Model):
    playerperf_id       = models.AutoField(primary_key=True)
    player              = models.ForeignKey(Player)
    league              = models.ForeignKey(League)
    season              = models.IntegerField()
    performance_type    = models.CharField(max_length=20)
    performance_round   = models.IntegerField(max_length=10)

    #data    
    prediction_round = models.IntegerField(max_length=10, null=True)
    points = models.IntegerField(max_length=10, null=True)
    played = models.NullBooleanField(max_length=10, null=True)


    def __unicode__(self):
        return self.performance_round

    class Meta:
        abstract = True
        index_together = [["player", "league", "season", "performance_type", "performance_round"],]
        unique_together = ("player", "league", "season", "performance_type", "performance_round")        
        

class PlayerPerformance(PlayerPerformance_base):

    class Meta:
        db_table = "tbl_playerperformance"


@BulkLoadable
class PlayerPerformance_stg(PlayerPerformance_base):

    class Meta:
        db_table = "tbl_playerperformance_stg"



class PlayerPerformanceSummary(models.Model):

    player_siteid = models.IntegerField(primary_key=True)
    player_id = models.IntegerField()
    player_name = models.CharField(max_length=50)
    sum_points = models.FloatField()
    avg_points = models.FloatField()
    avg_points_l3 = models.FloatField()
    avg_points_l5 = models.FloatField()
    avg_points_l7 = models.FloatField()
    
    avg_predpoints_n1 = models.FloatField()
    avg_predpoints_n3 = models.FloatField()
    avg_predpoints_n5 = models.FloatField()
    games_played = models.FloatField()
    max_points = models.FloatField()
    min_points = models.FloatField()
    
    position_b = models.IntegerField()
    position_c = models.IntegerField()
    position_r = models.IntegerField()
    position_f = models.IntegerField()
    
    
    

    class Meta:
        db_table = "player_performance_summary_current"
        managed= False

    def __unicode__(self):
        return self.player_name


class Team_base(models.Model):
    team_id         = models.AutoField(primary_key=True)
    team_siteid     = models.IntegerField(max_length=200)
    league          = models.ForeignKey(League)
    season          = models.IntegerField()
    team_name       = models.CharField(max_length=50)


    def __unicode__(self):
        return self.team_name

    class Meta:
        abstract = True


class Team(Team_base):

    class Meta:
        db_table = "tbl_team"
        
@BulkLoadable
class Team_stg(Team_base):

    class Meta:
        db_table = "tbl_team_stg"



class TeamPlayer_base(models.Model):
    teamplayer_id   = models.AutoField(primary_key=True)
    team            = models.ForeignKey(Team)
    player          = models.ForeignKey(Player)
    start_date      = models.DateField()
    end_date        = models.DateField()
    active          = models.BooleanField(League)

    def __unicode__(self):
        return self.player

    class Meta:
        abstract = True


class TeamPlayer(TeamPlayer_base):

    class Meta:
        db_table = "tbl_teamplayer"
        
@BulkLoadable
class TeamPlayer_stg(TeamPlayer_base):

    class Meta:
        db_table = "tbl_teamplayer_stg"


class TeamPlayerPosition_base(models.Model):
    teamplayerposition_id   = models.AutoField(primary_key=True)
    team            = models.ForeignKey(Team)
    player          = models.ForeignKey(Player)
    observation_round      = models.IntegerField()
    #data
    named_position  = models.CharField(max_length=2)
    named_18     = models.BooleanField()
    named_emergency     = models.BooleanField()
    named_bench     = models.BooleanField()

    def __unicode__(self):
        return self.player

    class Meta:
        abstract = True


class TeamPlayerPosition(TeamPlayerPosition_base):

    class Meta:
        db_table = "tbl_teamplayerposition"
        
@BulkLoadable
class TeamPlayerPosition_stg(TeamPlayerPosition_base):

    class Meta:
        db_table = "tbl_teamplayerposition_stg"




