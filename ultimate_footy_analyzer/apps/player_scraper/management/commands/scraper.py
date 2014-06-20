#django
from django.db.models.loading import get_model
from django.core.management.base import BaseCommand
from optparse import make_option

import numpy as np


from uf_scraper.apps.player_scraper.models import  League, Team

#models
#from uf_scraper.apps.player_scraper.models import player
from uf_scraper.apps.player_scraper.spiders.uf_spiders import PlayerPerformanceSpider, PlayerTeamSpider 

from uf_scraper.apps.player_scraper.framework.UFAnalysis import UF_TeamOptimiser#, UF_TradeOptimiser



from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy import log, signals
from scrapy.utils.project import get_project_settings
import sys, traceback

class Command(BaseCommand):

    args = '<filename>'
    help = 'Run a scraper'


    option_list = BaseCommand.option_list + (
                                             
        make_option('--action',               action='store', dest='action',            type='string', help='Perform stated action.'),
        
        make_option('--df', '--datafile',     action='store', dest='datafile',          type='string'),
        make_option('--sn', '--season',       action='store', dest='season',            type='string'),
                
        make_option('--or', '--obsround',     action='store', dest='obsround',          type='int' ),
        make_option('--l',  '--leagueid',     action='store', dest='leagueid',          type='string'),
        make_option('--pt',  '--perftype',    action='store', dest='performance_type',  type='string'),
        make_option('--nt',  '--numteams',    action='store', dest='numteams',          type='int'),
        make_option('--ts',  '--team_siteid',    action='store', dest='team_siteid',          type='int')
     )


    """    
    #team updates    
    manage.py scraper --sn 2014 --l 309252 --action teamplayer --nt 12   
    
    #player performance
    manage.py scraper --df "file://localhost/Users/nathanielramm/Dropbox/dev/python/uf_scraper/fixtures/2014_playerperf_R12.html" --sn 2014 --or 12 --l 309252 --pt performance --action playerperf    
    """

    def handle(self, action=None, datafile=None, season=None, obsround=None, leagueid=None, performance_type=None, numteams=None, team_siteid = None, **options):

        """======================
            Time windows
        """
        if action == "select_team":
            
            team_players = {}
            
            selected_team = {}
            player_performances = {}
            
            objTeam = Team.objects.get(league_id=leagueid, team_siteid=team_siteid) 
            
            teamid = objTeam.pk
            
            objTeamOptimiser = UF_TeamOptimiser()
            objTeamOptimiser.initialise_team(teamid)
            objTeamOptimiser.get_player_performance(teamid) 
            objTeamOptimiser.select_team_positions(teamid, 'last5') 
            

            #print player_performances
            



        if action == "add_league":

            if leagueid <> None:
                
                objLeague, created = League.objects.get_or_create(league_siteid=leagueid);
                
                if numteams <> None:
                        
                    objLeague.numteams = numteams
                    objLeague.save()
                
                
                if created == True:
                    log.msg("League {0} created".format(leagueid))
                else:
                    log.msg("League {0} already in database".format(leagueid))


        if action == "playerperf":


            playerperf_spider = PlayerPerformanceSpider()
            playerperf_spider.init(season, obsround, performance_type)       
            playerperf_spider.add_datafile(datafile)
            
            settings = get_project_settings()
            
            playerperf_crawler = Crawler(settings)
            playerperf_crawler.signals.connect(reactor.stop, signal=signals.spider_closed)
            playerperf_crawler.configure()
            playerperf_crawler.crawl(playerperf_spider)
            playerperf_crawler.start()
            
            log.start()
            log.msg('Running login reactor...')
            
            
            reactor.run()  # the script will block here until the spider is closed
            log.msg('Reactor login stopped.')        
        
        
        if action == "teamplayer":

            if leagueid == None:
                leagueid_list = League.objects.all()
            else:
                leagueid_list, found = League.objects.get(league_siteid=leagueid)

            playerteam_spiders = {}
            playerteam_crawlers = {}

            #loop through leagues
            for objLeague in leagueid_list:
                
                curr_leagueid = objLeague.league_siteid
                numteams = objLeague.numteams


                try:

                    #loop through number of teams and add urls
                    base_url = "http://ultimate-footy.theage.com.au/{league_id}/{teamnum}"                 
                    base_league_url = "http://ultimate-footy.theage.com.au/{league_id}".format(league_id=curr_leagueid)                 



                    playerteam_spiders[curr_leagueid] = PlayerTeamSpider(domain=base_league_url)
                    playerteam_spiders[curr_leagueid].init(season, obsround, numteams)
        
                    
                    i=1
                    while i <= numteams:
                        
                        team_url = base_url.format(league_id=curr_leagueid, teamnum=str(i))
                        playerteam_spiders[curr_leagueid].add_datafile(team_url)
                        i +=1


                    settings = get_project_settings()
                    playerteam_crawlers[curr_leagueid] = Crawler(settings)
                    
                    #log.msg(str(playerteam_spider.start_urls))
                    
                    playerteam_crawlers[curr_leagueid].signals.connect(reactor.stop, signal=signals.spider_closed)
                    playerteam_crawlers[curr_leagueid].configure()
                    playerteam_crawlers[curr_leagueid].crawl(playerteam_spiders[curr_leagueid])
                    playerteam_crawlers[curr_leagueid].start()
                    
                except Exception, e:
                    log.msg("Error Loading Teamplayer details {0}".format(str(e)))
                    print(e)
                    traceback.print_exc(file=sys.stdout)

            log.start()
            log.msg('Running login reactor...')
            
            reactor.run()  # the script will block here until the spider is closed
            log.msg('Reactor login stopped.')  

       