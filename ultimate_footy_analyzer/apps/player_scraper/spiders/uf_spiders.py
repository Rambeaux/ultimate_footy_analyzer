from scrapy.spider import Spider
from scrapy.selector import Selector, HtmlXPathSelector



from ultimate_footy_analyzer.apps.player_scraper.models import Team, TeamPlayer, TeamPlayer_stg, League, Player, Player_stg, PlayerStatus, PlayerStatus_stg, PlayerPerformance, PlayerPerformance_stg, TeamPlayerPosition_stg

from django.db import connection, connections, transaction
import time
import datetime
from scrapy import log, signals
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request, FormRequest
import urllib
import sys, traceback
from io import BytesIO

from selenium import selenium
from selenium import webdriver 

       
class ParentSpider(Spider):   
    
    @transaction.commit_on_success          
    def upsert(self, con, key_fields, data_fields, prod_tablename, stg_tablename, stagingModelObject):

        #prod_tablename = 'tbl_player'
        #stg_tablename = 'tbl_player_stg'
        #key_fields = {'player_siteid'}
        #data_fields = {'player_name', 'aflteam', 'position', 'league'}

        #update set
        update_set = ' SET '
        for data_field in data_fields:
            update_set = update_set + " {data_field} = {stg_tablename}.{data_field},".format(data_field=data_field, stg_tablename=stg_tablename)

        update_set = update_set[:-1]
        
        #update where
        update_where = ' WHERE '
        for key_field in key_fields:
            update_where = update_where + " {prod_tablename}.{key_field} = {stg_tablename}.{key_field} AND".format(key_field=key_field, stg_tablename=stg_tablename, prod_tablename=prod_tablename)

        update_where = update_where[:-3]


        #delete where
        delete_where = ' WHERE '
        for key_field in key_fields:
            delete_where = delete_where + " prod.{key_field} = stg.{key_field} AND".format(key_field=key_field, stg_tablename=stg_tablename, prod_tablename=prod_tablename)

        delete_where = delete_where[:-3]
        
        field_list = stagingModelObject.get_column_names(con)     
            
            
        _upsert = """ UPDATE {prod_tablename}
        
        {update_set}
        FROM {stg_tablename}
        {update_where}
         ;
        
        DELETE
        FROM {stg_tablename} AS stg
        USING {prod_tablename} AS prod
        {delete_where}
        ;
                   
        INSERT into {prod_tablename}
        ({field_list}) 
        SELECT {field_list}

        FROM {stg_tablename};
        """.format(prod_tablename=prod_tablename,
                   stg_tablename=stg_tablename,
                   update_set=update_set,
                   update_where=update_where,
                   delete_where=delete_where,
                   field_list=field_list)
              
        #log.msg(_upsert)      
        try:
                        
            #Only Upsert if there have been records loaded
            cursor = connection.cursor()  
            cursor.execute(_upsert)
            con.commit() 
                
        except:
            log.msg("Error: upsert_from_staging {0}".format(prod_tablename) )  
           
    @transaction.commit_on_success      
    def clear_staging(self, con, table_name):

        try:            

            cursor = con.cursor()
            sql = "DELETE FROM {table_name};".format(table_name=table_name)
            #log.msg(sql)          
            cursor.execute( sql )
                
                
        except Exception, e:
            log.msg("Error: get_prod_player_cache(): {0}, {1}".format(e, sql) )    


    def bulk_insert_bystream_text(self, objects_list, con):
        # Bulk create helper method. Unpack the dictionary
        # that has model refs.

        column_names = None
        table_name = None
                
        #Only run if there are records to be loaded
        if len(objects_list) > 0:
            
            try:
                
                # Make a text file stream object for COPY FROM
                cpy = BytesIO()

                #push cached values into 
                for object in objects_list:
                    
                    if column_names == None:
                        column_names = [str(object.get_column_names(con))]

                    if table_name == None:
                        table_name = object._meta.db_table

                    #log.msg(str(column_names))
                    
                    valuelist = object.get_bulkinsert_valuelist(con)
                    
                    fmtstr = '\t'.join([ fldval for fldval in valuelist]) + '\n'
                    cpy.write(fmtstr)

                # Copy data to database
                cpy.seek(0)
                cursor = con.cursor()
                cursor.copy_from(cpy, table_name, columns=column_names)
                con.commit()

                #self.numrecordsinserted += len(objects_list)
                
            
            except Exception, e:
                log.msg ("ERROR: Bulk Load Failed Load for {0}: {1}".format(table_name, str(e)))   
                #self.reset_records_cache()  
                print '-'*60
                traceback.print_exc(file=sys.stdout)
                print '-'*60           



""" ==============================
    Team Player Spider...
    ==============================
    
     
manage.py migrate player_scraper zero
manage.py schemamigration player_scraper --initial
manage.py migrate player_scraper    
    

#player performance
manage.py scraper --df "file://localhost/Users/nathanielramm/Dropbox/dev/python/ultimate_footy_analyzer/fixtures/2014_playerperf_R13.html" --sn 2014 --or 14 --l 390252 --pt performance --action playerperf

#player projections
manage.py scraper --df "file://localhost/Users/nathanielramm/Dropbox/dev/python/ultimate_footy_analyzer/fixtures/2014_playerproj_R12.html" --sn 2014 --or 12 --l 390252 --pt prediction --action playerperf

#team updates    
manage.py scraper --sn 2014 --l 390252 --action teamplayer --nt 12 --or 13  

    
"""

class PlayerTeamSpider(ParentSpider):
    name = "teamplayer"
        
    start_urls = [
    ]

    leagueid = None
    season = None
    obsround = None
    
    numteams = None
    
    objLeague = None
    
    teamplayerlist_indatabase = {}
    teamplayerlist_all = {}
    teamplayerlist_add = {}
    teamplayerlist_remove = {}
    
    teamplayer_upsert_cache = {}
    teamplayer_position_cache = {}

    webbrowser = {}

    def __init__(self):
        
        self.verificationErrors = []


    def init(self, season, obsround, numteams):
        
        #League Object
        """
        if leagueid == None:
            raise Exception("leagueid not specified")
        else:
            objLeague, created = League.objects.get_or_create(league_siteid = leagueid)
            self.objLeague = objLeague
            self.leagueid = leagueid
        """

        if obsround == None:
            raise Exception("observation round not specified")
        else:
            self.obsround = obsround


        if season == None:
            self.season = 2014
        else:
            self.season = season
            
        if numteams == None:
            self.numteams = 12
        else:
            self.numteams = numteams
   
   
    def set_league_id(self, leagueid):
        
        objLeague, created = League.objects.get_or_create(league_siteid = leagueid)
        self.objLeague = objLeague
        self.leagueid = leagueid        
            

    def add_datafile(self, datafile):
        
        self.start_urls.append(datafile)

    def reset_urls(self):
        
        self.start_urls = []



    def parse(self, response):
        """
        Playerperformance Scraper
        http://ultimate-footy.theage.com.au/390252/players_print?type=condensed&l=390252&status=ALL&pos=P&club=ALL&stats=2014_RS_L23        
        """
            
        try:
            
            
            xpath_strings = {
                             'lineup_table':        '//table[@id = "lineup_tbl"]/tr',
                             'player_id':           'td/a/@href',
                             'player_namedposition': 'td/text()',
                             'teamname' :           '//title',
                             'lineup_squad':        'td/strong',
                             }
            
            #Upload players        
            con = connections['default']
            
            
            """=========
                Selenium
            """
            
            #item = Item()
    
            #hxs = HtmlXPathSelector(response)
            #Do some XPath selection with Scrapy
            #hxs.select('//div').extract()
            
            """=========
                Scrapy
            """
            sel = Selector(response)
    

    
            """ ======================================
                gather and update the team name""" 
            
            #teamname
            teamname_data = str(sel.xpath(xpath_strings['teamname']).extract())
            teamname = teamname_data[teamname_data.find('Ultimate Footy - Ultimate Footy - ')+len('Ultimate Footy - Ultimate Footy - '):teamname_data.find('</title>')]

            base_url = "http://ultimate-footy.theage.com.au/"
            team_url = base_url+str(self.leagueid)+"/"
            log.msg(team_url)
            log.msg(response.url)

            #team_siteid = int( response.url[response.url.find(team_url)+len(team_url) : len(response.url)] )
            
            #get the current league number
            league_siteid = int( response.url[  response.url.find(base_url)+len(base_url)  : len(base_url)+6] )
            self.set_league_id(league_siteid)
            log.msg("league_siteid: {0}".format(league_siteid))

            #find the team num...            
            team_siteid = int( response.url[  response.url.find(base_url)+len(base_url)+7  : len(response.url)] )
            log.msg("team_siteid: {0}".format(team_siteid))
            
            
            #self.webbrowser[team_siteid] = webdriver.Chrome()        
            #self.webbrowser[team_siteid].get(response.url)
            #time.sleep(10)

            #Do some crawling of javascript created content with Selenium
            
            #hxs = HtmlXPathSelector(self.webbrowser[team_siteid].page_source)
            #lineup_table = hxs.select(xpath_strings['lineup_table']).extract()
            #log.msg(lineup_table)
            
            
            
            
            
            
            objTeam, found = Team.objects.get_or_create(team_siteid=team_siteid, league=self.objLeague, season=self.season)
            objTeam.team_name = teamname
            objTeam.save()

            """ ======================================
                gather and update the team lineup""" 

            #build a cache of current players in the team...
            self.teamplayerlist_indatabase[team_siteid] = self.get_current_playerids(con, objTeam)

            #initialise the playerlist to be filled via the scrape
            self.teamplayerlist_all[team_siteid] = []
            self.teamplayerlist_add[team_siteid] = []
            self.teamplayerlist_remove[team_siteid] = []
            self.teamplayer_upsert_cache[team_siteid] = []
                
            self.teamplayer_position_cache[team_siteid] = []    
                
            """Build The Team Player Position Data"""    
            #teamlineup    
            lineuptable_data = sel.xpath(xpath_strings['lineup_table'])
            
            #log.msg(str(lineuptable_data))
            team_position_row = 1
            
            for lineuprow_data in lineuptable_data:

                #log.msg(str(lineuprow_data.extract()))
                
                player_id_temp = lineuprow_data.xpath(xpath_strings['player_id']).extract()

                player_namedpos_temp = lineuprow_data.xpath(xpath_strings['player_namedposition']).extract()

                #where was the player named to play by the manager
                named_18 = False 
                named_emergency = False 
                named_bench = False 
                named_position = None 

                if len(player_namedpos_temp) > 0:
                    if player_namedpos_temp[0] in ('B','C','R','F','BN'):  
                        #get the named position
                        named_position = player_namedpos_temp[0]

                        #starting 18 players in the list                        
                        if team_position_row <= 18:
                            named_18 = True 

                        if team_position_row > 18 and team_position_row <= 22:
                            named_emergency = True 

                        if team_position_row > 22:
                            named_bench = True 
                            
                    #increment named players
                    team_position_row+=1

                
                #create the Player Position Record
                if len(player_id_temp)>0:
                    #get the player siteid
                    player_id = str(player_id_temp[0])[len('/players/'):len(player_id_temp[0])]

                    objPlayer = Player.objects.get(player_siteid=player_id)    
                    
                    objTeamPlayerPos = TeamPlayerPosition_stg(team=objTeam,
                                           player=objPlayer,
                                           observation_round=self.obsround,
                                           named_position=named_position,
                                           named_18=named_18,
                                           named_emergency=named_emergency,
                                           named_bench=named_bench )

                    self.teamplayer_position_cache[team_siteid].append(objTeamPlayerPos)

                    #keep track of the players just sscraped from the true current team                                        
                    self.teamplayerlist_all[team_siteid].append(player_id)

                    #if the player is not in the team in the db, we need to add them
                    if player_id not in self.teamplayerlist_indatabase[team_siteid]:
                        self.teamplayerlist_add[team_siteid].append(player_id)
                        
                
            #review the list to remove
            if len(self.teamplayerlist_indatabase[team_siteid]) > 0:
                
                for test_player_id in self.teamplayerlist_indatabase[team_siteid]:
                    
                    if test_player_id in self.teamplayerlist_all[team_siteid] == False:
                        self.teamplayerlist_remove[team_siteid].append(test_player_id)

            #process the add and remove lists
            #add list
            if len(self.teamplayerlist_add[team_siteid]) > 0:
                
                for add_player_id in self.teamplayerlist_add[team_siteid]:
                    
                    objPlayer = Player.objects.get(player_siteid=add_player_id)    
                                
                    objTeamPlayer_stg = TeamPlayer_stg(player=objPlayer, team=objTeam, active=True,
                                               start_date=datetime.datetime.now(), end_date=datetime.datetime.now()+datetime.timedelta(days=365))
    
                    self.teamplayer_upsert_cache[team_siteid].append(objTeamPlayer_stg)

            #remove list
            if len(self.teamplayerlist_remove[team_siteid]) > 0:
                
                for remove_player_id in self.teamplayerlist_remove[team_siteid]:
                    
                    objPlayer = Player.objects.get(player_siteid=remove_player_id)                
                    
                    #existing record
                    objTeamPlayer = TeamPlayer.objects.get(player=objPlayer, team=objTeam)
    
                    #'removal' record
                    objTeamPlayer_stg = TeamPlayer_stg(player=objPlayer, team=objTeam, active=False,
                                               start_date=objTeamPlayer.start_date, end_date=datetime.datetime.now())
                                               
    
                    self.teamplayer_upsert_cache[team_siteid].append(objTeamPlayer_stg)
            
    
            #Upsert to prod        
            teamplayer_prod_tablename = 'tbl_teamplayer'
            teamplayer_stg_tablename = 'tbl_teamplayer_stg'
            teamplayer_key_fields = {'team_id', 'player_id'}
            teamplayer_data_fields = {'start_date', 'end_date', 'active'}        

            self.clear_staging(con, teamplayer_stg_tablename)     
            self.bulk_insert_bystream_text(self.teamplayer_upsert_cache[team_siteid], con) 
            self.upsert(con, teamplayer_key_fields, teamplayer_data_fields, teamplayer_prod_tablename, teamplayer_stg_tablename, TeamPlayer_stg())


            #Upsert to prod        
            teamplayerpos_prod_tablename = 'tbl_teamplayerposition'
            teamplayerpos_stg_tablename = 'tbl_teamplayerposition_stg'
            teamplayerpos_key_fields = {'team_id', 'player_id', 'observation_round'}
            teamplayerpos_data_fields = {'named_position', 'named_18', 'named_emergency', 'named_bench'}        

            self.clear_staging(con, teamplayerpos_stg_tablename)     
            self.bulk_insert_bystream_text(self.teamplayer_position_cache[team_siteid], con) 
            self.upsert(con, teamplayerpos_key_fields, teamplayerpos_data_fields, teamplayerpos_prod_tablename, teamplayerpos_stg_tablename, TeamPlayerPosition_stg())
            
            #log.msg("Upsert"+str(self.teamplayer_upsert_cache[team_siteid]))

            #log.msg("teamplayerlist_indatabase: "+str(self.teamplayerlist_indatabase[team_siteid]))
            #log.msg("teamplayerlist_all: "+str(self.teamplayerlist_all[team_siteid]))
            #log.msg("teamplayerlist_add: "+str(self.teamplayerlist_add[team_siteid]))
            #log.msg("teamplayerlist_remove: "+str(self.teamplayerlist_remove[team_siteid]))

    
            #end
        except Exception, e:
            log.msg("Exception scraping data: {0}".format(e))
            log.msg(str(traceback.print_exc(file=sys.stdout)))







    @transaction.commit_on_success      
    def get_current_playerids(self, con, objTeam):

        try:            
            cache = []
            teamplayers = TeamPlayer.objects.raw( "SELECT * FROM tbl_teamplayer WHERE team_id = {0} and active='true';".format(objTeam.pk)) 
            
            log.msg(str(teamplayers))
            
            for objTeamPlayer in teamplayers:
                cache.append(objTeamPlayer.player.player_siteid)

            return cache
                
        except Exception, e:
            log.msg("Error: get_prod_player_cache(): {0}".format(e) ) 



""" ==============================
    Player Performance Spider...
    ==============================
"""

class PlayerPerformanceSpider(ParentSpider):
    name = "player"
    #allowed_domains = ["ultimate-footy.theage.com.au", "ultimatefooty.myfairfax.com.au"]

    #"file://localhost/Users/nathanielramm/Dropbox/players_print.html"
    #"http://ultimate-footy.theage.com.au/390252/players_print?type=condensed&l=390252&status=ALL&pos=P&club=ALL&stats=2014_RS_L23",
    #"http://ultimate-footy.theage.com.au/390252/players_print?type=condensed&l=390252&status=ALL&pos=P&club=ALL&stats=2014_PS_L23",
        
    start_urls = [
    ]

    leagueid = None
    season = None
    obsround = None
    performance_type = None
    
    objLeague = None
    
    playerperf_cache = []
    player_cache = []
    playerstatus_cache = []
    prod_player_cache = {}

    def __init__(self):
        pass    

    def init(self, leagueid, season, obsround, performance_type):
        
        #League Object
        if leagueid == None:
            raise Exception("leagueid not specified")
        else:
            objLeague, created = League.objects.get_or_create(league_siteid = leagueid)
            self.objLeague = objLeague
            self.leagueid = leagueid
            
        if obsround == None:
            raise Exception("observation round not specified")
        else:
            self.obsround = obsround

        if performance_type == None:
            raise Exception("performance type round not specified")
        else:
            self.performance_type = performance_type
            
        if season == None:
            self.season = 2014
        else:
            self.season = season
            

    def add_datafile(self, datafile):
        
        self.start_urls.append(datafile)


    def parse(self, response):
        """
        Playerperformance Scraper
        http://ultimate-footy.theage.com.au/390252/players_print?type=condensed&l=390252&status=ALL&pos=P&club=ALL&stats=2014_RS_L23 
        
        
               
        """
            
        try:
            sel = Selector(response)
    
            xpath_strings = {
                             'roundheaders_data':   '//html/body/table/tbody/tr/td/table/tbody/tr/td/table/thead/tr',#???
                             'performance_data':    '//html/body/table/tbody/tr/td/table/tbody/tr/td/table/tbody/tr',
                             'player_id':           'td/a/@href',
                             'playername':          'td/a/text()',
                             'player_performance':  'td/text()',
                             'prediction_data':     '//html/body/table/tbody/tr/td/table/tbody/tr/td/table/tbody/tr',
                             'player_prediction':   'td/text()',
                             'roundheaders':        'td/strong/a/text()',
                             'player_prediction':   'td/text()',
                             'player_status':       'td/img/@alt',
                             'player_status_return': 'td/img/@title',
                             }
            
    
    
            """ ======================================
                Loop through player performance column heaaders""" 
                
            roundheaders_data = sel.xpath(xpath_strings['roundheaders_data'])[1]
    
            header_cache = {}
            
            header_rounds = roundheaders_data.xpath(xpath_strings['roundheaders']).extract()
            

            
            #User this to do some error checking...     
            
    
            """ ======================================
                Loop through player performance data
                ======================================"""        
            allplayer_perfdata = sel.xpath(xpath_strings['performance_data'])
           
           
           
            if self.performance_type in ("performance", "prediction"):

                #loop through every player                
                for player_perfdata in allplayer_perfdata:
                    
                    
                    """Player Details"""
                    #playerid link
                    player_id_temp = player_perfdata.xpath(xpath_strings['player_id']).extract()[0]
                    player_id = str(player_id_temp)[len('http://ultimate-footy.theage.com.au/players/'):len(player_id_temp)]

                    #create player object for upsert                    
                    objPlayer = Player_stg(player_siteid=player_id)
                                        
                    #playername
                    objPlayer.player_name = player_perfdata.xpath(xpath_strings['playername']).extract()[0]

        
                    #append to cache for upload
                    self.player_cache.append(objPlayer)
        
                
                #Upload players        
                con = connections['default']
                
        
                #Upsert to prod        
                player_prod_tablename = 'tbl_player'
                player_stg_tablename = 'tbl_player_stg'
                player_key_fields = {'player_siteid'}
                player_data_fields = {'player_name'}        

                self.clear_staging(con, player_stg_tablename)     
                self.bulk_insert_bystream_text(self.player_cache, con) 
                
                self.upsert(con, player_key_fields, player_data_fields, player_prod_tablename, player_stg_tablename, Player_stg())

                #Build Player Cache                
                self.prod_player_cache[self.objLeague.pk] = self.get_prod_player_cache(con)
                
        
                """ ==============================================
                    Player Status
                    ==============================================
                    """        
                for player_perfdata in allplayer_perfdata:
        
                    """ Player Status"""
                    #player_id
                    player_id_temp = player_perfdata.xpath(xpath_strings['player_id']).extract()[0]
                    player_id = str(player_id_temp)[len('http://ultimate-footy.theage.com.au/players/'):len(player_id_temp)]
                    
                    #get status images                    
                    player_status_data = player_perfdata.xpath(xpath_strings['player_status']).extract()
                    player_statusreturn_data = player_perfdata.xpath(xpath_strings['player_status_return']).extract()
                        

                    #get existing player Record                    
                    objPlayer = None                    
                    
                    if self.prod_player_cache[self.objLeague.pk].has_key(int(player_id)):
                        objPlayer = self.prod_player_cache[self.objLeague.pk][int(player_id)]
                    else:
                        log.msg("Player not found in cache: {0}".format(player_id))
                        if len(self.prod_player_cache[self.objLeague.pk]) == 0:
                            log.msg("Player cache is empty: {0}".format(player_id))
                            

                    #build base player status for upload to staging
                    objPlayerStatus = PlayerStatus_stg(player=objPlayer,
                                                       league=self.objLeague,
                                                       season=self.season,
                                                       observation_round=self.obsround
                                                       )

                    named_to_play = False
                    not_playing = False
                    injured_suspended = False
                    named_emergency = False
                    named_interchange = False
                    hurt = False

                    #loop through statuses
                    for status in player_status_data:
                        if status == 'Named to Play':
                            named_to_play = True
                        if status == 'Not Playing':
                            not_playing = True
                        if status == 'Injury/Suspension Note':
                            injured_suspended = True
                        if status == 'Named as an Emergency':
                            named_emergency = True
                        if status == 'Named on Extended Interchange':
                            named_interchange = True
                        if status == 'Hurt':
                            hurt = True
                            
                    objPlayerStatus.named_to_play = named_to_play
                    objPlayerStatus.not_playing = not_playing
                    objPlayerStatus.named_emergency = named_emergency
                    objPlayerStatus.named_interchange = named_interchange
                    objPlayerStatus.injured_suspended = injured_suspended
                    objPlayerStatus.hurt = hurt
                            
                            
                    #position and club
                    club_pos_data = player_perfdata.xpath(xpath_strings['player_performance']).extract()[0]
                    
                    aflteam = club_pos_data[club_pos_data.find("(")+1  : (club_pos_data.find("-")-1)]
                    position = club_pos_data[ club_pos_data.find("-")+1: (club_pos_data.find(")"))]                            

                    position_B = ('B' in position)
                    position_C = ('C' in position)
                    position_R = ('R' in position)
                    position_F = ('F' in position)
                            
                    objPlayerStatus.aflteam = aflteam                            
                    objPlayerStatus.position = position                            
                    objPlayerStatus.position_B = position_B                            
                    objPlayerStatus.position_C = position_C                            
                    objPlayerStatus.position_R = position_R                            
                    objPlayerStatus.position_F = position_F                            
                            
                    #injuries and suspensions        
                    if injured_suspended == True:
                        #get from player_statusreturn_data
                        
                        #round of return
                        round_start = str(player_statusreturn_data[0]).find('Round')+6
                        round_end = len(str(player_statusreturn_data[0]))
                        expected_return_round = 0
                        
                        if (round_start <> -1):                    
                            expected_return_round = str(player_statusreturn_data[0])[round_start:round_end]

                        objPlayerStatus.expected_return_round = expected_return_round


                        #Out for season
                        out_for_season = False                    
                        outforseason_ind = str(player_statusreturn_data[0]).find('Indefinite/Season')
                        
                        if outforseason_ind <> -1:
                            out_for_season = True

                        objPlayerStatus.out_for_season = out_for_season
        

        
                    self.playerstatus_cache.append(objPlayerStatus)
                    
                    
                    
                    """======================================
                        Player Performance
                        ======================================""" 
                        
                        
                    #round by round performance...
                    perf_round = 1
                    round_column_offset = 2

                    
                    perf = player_perfdata.xpath(xpath_strings['player_performance']).extract()
                    numrounds_in_data = len(perf)-4

                    #log.msg(str(perf))
                    
                    
                    
                    #loop until current_round
                    #TODO: This appears to be happening 5 times for performances...
                    while perf_round <= numrounds_in_data:             
                    
                        col = perf_round+round_column_offset

                        performance = perf[col]

                        roundno_start = str(header_rounds[col-1]).find("R")+1
                        roundno_end = len(header_rounds[col-1])
                                                                    
                        true_round = str(header_rounds[col-1])[roundno_start:roundno_end]
                         

                        if self.performance_type in ("prediction"):
        
                            objPerf = PlayerPerformance_stg(player=objPlayer, 
                                                            league=self.objLeague,
                                                            performance_round = true_round,
                                                            performance_type = self.performance_type,
                                                            season=self.season,
                                                            prediction_round=self.obsround, #is only required for predicted performances
                                                            )

                            #update values
                            objPerf.points = perf[col]
                            #objPerf.save()

                        if self.performance_type in ("performance"):
        
                            objPerf = PlayerPerformance_stg(player=objPlayer, 
                                                            league=self.objLeague,
                                                            performance_round = true_round,
                                                            performance_type = self.performance_type,
                                                            season=self.season,
                                                            prediction_round=999
                                                            )


                            #did the player play the game?
                            played = False
                            if int(performance) <> 0 :
                                played = True


                            #update values
                            objPerf.points = perf[col]
                            objPerf.played=played
                            #objPerf.save()


            
                        self.playerperf_cache.append(objPerf)
                        #increment the round
                        perf_round += 1
                    #end loop...
    
                    #todo: PlayerPerformance.get_or_create(player_id, season, round, performance, performance_type, observation_round)
        
        except Exception, e:
            log.msg("Exception scraping data: {0}".format(e))
            log.msg(str(traceback.print_exc(file=sys.stdout)))
          
        con = connections['default']
          

        #Upload playerperformance   

        #Upsert to prod        
        playerperf_prod_tablename = 'tbl_playerperformance'
        playerperf_stg_tablename = 'tbl_playerperformance_stg'
        playerperf_key_fields = {'player_id', 'league_id', 'season', 'performance_type', 'performance_round', 'prediction_round'}
        playerperf_data_fields = {'points', 'played'}        

        self.clear_staging(con, playerperf_stg_tablename)     
        self.bulk_insert_bystream_text(self.playerperf_cache, con) 
        
        self.upsert(con, playerperf_key_fields, playerperf_data_fields, playerperf_prod_tablename, playerperf_stg_tablename, PlayerPerformance_stg())

          
        #Upload player status        

        #Upsert to prod        
        playerstatus_prod_tablename = 'tbl_playerstatus'
        playerstatus_stg_tablename = 'tbl_playerstatus_stg'
        playerstatus_key_fields = {'player_id', 'league_id', 'season', 'observation_round'}
        playerstatus_data_fields = {'named_to_play', 'named_emergency', 'named_interchange', 'not_playing',
                                   'injured_suspended', 'hurt', 'expected_return_round', 'out_for_season'}        

        self.clear_staging(con, playerstatus_stg_tablename)     
        self.bulk_insert_bystream_text(self.playerstatus_cache, con) 
        
        
        self.upsert(con, playerstatus_key_fields, playerstatus_data_fields, playerstatus_prod_tablename, playerstatus_stg_tablename, PlayerStatus_stg())
           
           
        """--current round;

create or replace view 
view_current_round as
select season, league_id, max(performance_round)+1 current_round
from tbl_playerperformance
where performance_type = 'performance'
group by season, league_id;

--player status;

create or replace view player_status_current as
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


create view player_status_all as
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




create view player_performance_all as
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
    player_status_all pl

left join tbl_playerperformance plp
    on pl.player_id = plp.player_id
    and pl.observation_round = plp.performance_round
    and plp.performance_type = 'performance'

group by pl.player_id
,plp.season;
        


create view player_projections_all as
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
    player_status_all pl

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

        
        
---Not ready yet...
create view player_performance_summary as
select pl.player_id, pl.player_siteid, pl.player_name, pl.aflteam, pl.position, pl.league_id
,plp.season
,cr.current_round

,SUM( CASE WHEN plp.performance_round <= cr.current_round  then points ELSE null END) as sum_points
,AVG( CASE WHEN plp.performance_round <= cr.current_round  and played = true then points ELSE null END) as avg_points
,SUM( CASE WHEN plp.performance_round <= cr.current_round and played = true then 1 ELSE null END) as games_played
,MAX( CASE WHEN plp.performance_round <= cr.current_round  then points ELSE null END) as max_points
,MIN( CASE WHEN plp.performance_round <= cr.current_round  and played = true then points ELSE null END) as min_points
,STDDEV( CASE WHEN plp.performance_round <= cr.current_round  and played = true then points ELSE null END) as sd_points
,STDDEV( CASE WHEN plp.performance_round <= cr.current_round  and played = true then points ELSE null END)
 / AVG( CASE WHEN plp.performance_round <= cr.current_round   and played = true then points ELSE null END) as sdpct_points


,AVG( CASE WHEN plp.performance_round > cr.current_round - 3  and played = true then points ELSE null END) as avg_points_L3
,AVG( CASE WHEN plp.performance_round > cr.current_round - 5  and played = true then points ELSE null END) as avg_points_L5
,AVG( CASE WHEN plp.performance_round > cr.current_round - 7  and played = true then points ELSE null END) as avg_points_L7

,AVG( CASE WHEN plp.performance_round > cr.current_round - 3  and played = true then points ELSE null END) 
/ AVG( CASE WHEN plp.performance_round <= cr.current_round - 3  and played = true then points ELSE null END) as avg_points_L3_vs_PREV
,AVG( CASE WHEN plp.performance_round > cr.current_round - 5  and played = true then points ELSE null END)  
/ AVG( CASE WHEN plp.performance_round <= cr.current_round -5  and played = true then points ELSE null END)as avg_points_L5_vs_PREV
,AVG( CASE WHEN plp.performance_round > cr.current_round - 7  and played = true then points ELSE null END) 
/ AVG( CASE WHEN plp.performance_round <= cr.current_round -7  and played = true then points ELSE null END) as avg_points_L7_vs_PREV



from
    tbl_player pl

left join tbl_playerperformance plp
    on pl.player_id = plp.player_id
    and pl.league_id = plp.league_id
    and plp.performance_type = 'performance'

left join view_current_round cr
    on 1=1
    

group by pl.player_id, pl.player_siteid, pl.player_name, pl.aflteam, pl.position, pl.league_id
,plp.season, cr.current_round;



create or replace view player_performance_summary_current as

select pls.player_siteid, pls.league_id, pls.player_id, pls.player_name, 
--pls.aflteam, pls.league_id, pls.season, pls.observation_round
--,pls.position, pls.position_b, pls.position_c, pls.position_r, pls.position_f

,SUM( CASE WHEN plp.performance_round <= pls.observation_round                             then plp.points ELSE null END) as sum_points
,AVG( CASE WHEN plp.performance_round <= pls.observation_round        and plp.played = true     then plp.points ELSE null END) as avg_points
,SUM( CASE WHEN plp.performance_round <= pls.observation_round        and plp.played = true     then 1             ELSE null END) as games_played
,MAX( CASE WHEN plp.performance_round <= pls.observation_round                              then plp.points ELSE null END) as max_points
,MIN( CASE WHEN plp.performance_round <= pls.observation_round        and plp.played = true     then plp.points ELSE null END) as min_points
--,STDDEV( CASE WHEN plp.performance_round <= pls.observation_round  and plp.played = true     then plp.points ELSE null END) as sd_points
--,STDDEV( CASE WHEN plp.performance_round <= pls.observation_round  and plp.played = true     then plp.points ELSE null END)
-- / AVG( CASE WHEN plp.performance_round <= pls.observation_round   and plp.played = true     then plp.points ELSE null END) as sdpct_points


,AVG( CASE WHEN plp.performance_round >= pls.observation_round - 3  and plp.played = true then plp.points ELSE null END) as avg_points_L3
,AVG( CASE WHEN plp.performance_round >= pls.observation_round - 5  and plp.played = true then plp.points ELSE null END) as avg_points_L5
,AVG( CASE WHEN plp.performance_round >= pls.observation_round - 7  and plp.played = true then plp.points ELSE null END) as avg_points_L7

--,AVG( CASE WHEN plp.performance_round >= pls.observation_round - 3  and plp.played = true then plp.points ELSE null END) 
--/ AVG( CASE WHEN plp.performance_round < pls.observation_round - 3  and plp.played = true then plp.points ELSE null END) as avg_points_L3_vs_PREV
--,AVG( CASE WHEN plp.performance_round >= pls.observation_round - 5  and plp.played = true then plp.points ELSE null END)  
--/ AVG( CASE WHEN plp.performance_round < pls.observation_round - 5  and plp.played = true then plp.points ELSE null END) as avg_points_L5_vs_PREV
--,AVG( CASE WHEN plp.performance_round >= pls.observation_round - 7  and plp.played = true then plp.points ELSE null END) 
--/ AVG( CASE WHEN plp.performance_round < pls.observation_round - 7  and plp.played = true then plp.points ELSE null END) as avg_points_L7_vs_PREV


--,AVG( CASE WHEN  pred.performance_round = pls.observation_round and pred.prediction_round = pls.observation_round then pred.points ELSE null END) as curr_predpoints_N1
--,AVG( CASE WHEN  pred.performance_round = pls.observation_round and pred.prediction_round < pls.observation_round then pred.points ELSE null END) as trend_predpoints_N1

,AVG( CASE WHEN pred.performance_round >= pls.observation_round and pred.performance_round < pls.observation_round + 3   and pred.prediction_round = pls.observation_round  then pred.points ELSE null END) as avg_predpoints_N3
,AVG( CASE WHEN pred.performance_round >= pls.observation_round and pred.performance_round < pls.observation_round + 5   and pred.prediction_round = pls.observation_round  then pred.points ELSE null END) as avg_predpoints_N5


from
player_status_current pls

left join tbl_playerperformance plp
    on pls.player_id = plp.player_id
    and pls.season = plp.season
    and pls.observation_round > plp.performance_round
    and plp.performance_type = 'performance'

left join tbl_playerperformance pred
    on pred.player_id = pls.player_id 
    and pred.season = pls.season 
    and pred.performance_type = 'prediction'     



group by pls.player_id, pls.player_siteid, pls.league_id, pls.player_name, pls.aflteam, pls.league_id, pls.season, pls.observation_round
,pls.position, pls.position_B, pls.position_c, pls.position_r, pls.position_f;


--Used in team analysis
--drop view player_performance_summary_current_b;

--create or replace view player_performance_summary_current_b as

select pls.player_siteid, pls.player_id, pls.league_id, pls.player_name
--pls.aflteam, pls.league_id, pls.season, pls.observation_round
--,pls.position, pls.position_B, pls.position_c, pls.position_r, pls.position_f
, pls.position_b, pls.position_c, pls.position_r, pls.position_f

, plp.sum_points, plp.avg_points, plp.avg_points_l3, plp.avg_points_l5, plp.avg_points_l7 
, plp.games_played, plp.max_points, plp.min_points 
 
, pred.avg_predpoints_n1, pred.avg_predpoints_n3, pred.avg_predpoints_n5 
 

 
 
 

from
player_status_current pls

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
player_status_current pls1

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
player_status_current pls2

left join tbl_playerperformance pred2
    on pred2.player_id = pls2.player_id 
    and pred2.season = pls2.season 
    and pred2.performance_type = 'prediction'
    and pred2.prediction_round = pls2.observation_round

    group by pls2.player_siteid
) pred
on pred.player_siteid = pls.player_siteid


--group by pls.player_id, pls.player_siteid, pls.player_name
--, pls.position_b, pls.position_c, pls.position_r, pls.position_f
--pls.aflteam, pls.league_id, pls.season, pls.observation_round
--,pls.position, pls.position_B, pls.position_c, pls.position_r, pls.position_f



create view team_summary as
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


left join player_performance_summary_current ppsc
    on ppsc.player_id = tpp.player_id
    and ppsc.observation_round = tpp.observation_round

) a
group by team_name, named_position, named_18, named_emergency, a.observation_round
order by observation_round, team_name, named_position;



--create view team_player_summary_curr as

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


from
    tbl_team t
    
left join tbl_teamplayerposition tpp
    on t.team_id = tpp.team_id


left join player_performance_summary_current ppsc
    on ppsc.player_id = tpp.player_id
    and ppsc.observation_round = tpp.observation_round

where named_position = 'BN'
order by ppsc.avg_points_l3_vs_prev desc;

create or replace view team_player_summary_curr as
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


from  player_performance_summary_current ppsc

left outer join tbl_teamplayerposition tpp
    on ppsc.player_id = tpp.player_id
    and ppsc.observation_round = tpp.observation_round
    
left outer join tbl_team t
    on t.team_id = tpp.team_id
    



where team_name is null

--where player_name in ('Dale Thomas', 'Farren Ray')
order by ppsc.avg_points_l3_vs_prev desc;



insert into tbl_round
(round)
VALUES
(1),
(2),
(3),
(4),
(5),
(6),
(7),
(8),
(9),
(10),
(11),
(12),
(13),
(14),
(15),
(16),
(17),
(18),
(19),
(20),
(21),
(22),
(23)        
        
        """
           

      
            
                   
   
    @transaction.commit_on_success      
    def get_prod_player_cache(self, con):

        try:            
            cache = {}

            players = Player.objects.raw( "SELECT * FROM tbl_player;")
            
            for objPlayer in players:
                cache[objPlayer.player_siteid] = objPlayer

            return cache
                
        except Exception, e:
            log.msg("Error: get_prod_player_cache(): {0}".format(e) )          
            


                
                         
                
