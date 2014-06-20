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
- imports league and team configurations
- provides analysis of team and plyer performance

TODO
====
- team selection optimiser
- trade analyser & recommender
- support multiple types of league

Docs
====



Install
=======

using ``virtualenvwrapper``
~~~
    $ mkvirtualenv uf_scraper
    $ workon uf_scraper
    $ git clone repo
    $ cd repo
    $ pip install -r reqs/dev.txt
    $ pip install -r reqs/common.txt
    $ python manage.py player_scraper migrate
~~~


Note: To install Scrapy:
if install fails, first install 'cryptography' package like so: 
CFLAGS="-I/usr/include" pip install cryptography
CFLAGS="-I/usr/include" pip install Twisted


