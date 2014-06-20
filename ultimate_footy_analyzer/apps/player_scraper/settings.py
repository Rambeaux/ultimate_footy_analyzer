
########## SCRAPY CONFIGURATION

import os

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ultimate_footy_analyzer.settings") #Changed in DDS v.0.3

BOT_NAME = 'player_scraper'

SPIDER_MODULES = ['ultimate_footy_analyzer.apps.player_scraper.spiders.uf_spiders']
USER_AGENT = '%s/%s' % (BOT_NAME, '1.0')

ITEM_PIPELINES = {
    #'ultimate_footy_analyzer.apps.player_scraper.pipelines.FilterWordsPipeline':1,
}

NEWSPIDER_MODULE = 'ultimate_footy_analyzer.apps.player_scraper.spiders.spiders'

TELNETCONSOLE_PORT = 0

DOWNLOAD_DELAY = 0

########## END SCRAPY CONFIGURATION