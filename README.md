# Ultimate Footy Analyzer #

Meta
====

* authors: Nathaniel Ramm
* email:  rambeaux@gmail.com
* status: in development
* notes:  

Purpose
=======

Ultimate Footy analyser
- imports player statistics
- imports player status (named to play, 
- imports league and team configurations
- imports actual and projected player performances
- provides analysis of team and player performance

TODO
====
- team selection optimiser
- trade analyser & recommender
- support multiple types of league

Usage
====

The loading of player performances and projected performances requires the saving of html from the ultimate footy site.
The scrapers are configured to read to the layout of the 'condensed' prinatable page for all players.

- Actual Performance
http://ultimate-footy.theage.com.au/{your_league_id}/players_print?type=condensed&l={your_league_id}&status=ALL&pos=P&club=ALL&stats=2014_PS_L24

- Projected Performance
http://ultimate-footy.theage.com.au/{your_league_id}/players_print?type=condensed&l={your_league_id}&status=ALL&pos=P&club=ALL&stats=2014_PS_L24


To load data into the database run the following management commands:

- to add a league to be managed. The league id is taken from the URL of your league page
manage.py scraper  --action add_league --l {your_league_id}  --nt {number of teams in league} --sn {current_season}

- to load actual player performance
manage.py scraper --action playerperf --df "file://yourdir/condensed_player_perf_RoundX.html" --sn {current_season} --or {current/next round} --l {your_league_id} --pt performance 

- to load projected player performance
manage.py scraper --action playerperf--df "file://yourdir/condensed_player_proj_RoundX.html" --sn {current_season} --or {current/next round} --l {your_league_id} --pt prediction

- to update team lists and player statuses (named to play, injuries, etc.). This may be run multiple times during the week as trades take place and Free Agents are drafted.
manage.py scraper --sn 2014 --l {your_league_id} --action teamplayer  --or {current/next round}  


Install
=======

using ``virtualenvwrapper``
~~~
    $ mkvirtualenv uf_scraper
    $ workon uf_scraper
    $ git clone https://github.com/Rambeaux/ultimate_footy_analyzer
    $ cd ultimate_footy_analyzer
    $ pip install -r reqs/dev.txt
    $ python manage.py player_scraper migrate
~~~


Note: To install Scrapy:
if install fails, first install 'cryptography' package like so: 
CFLAGS="-I/usr/include" pip install cryptography
CFLAGS="-I/usr/include" pip install Twisted


