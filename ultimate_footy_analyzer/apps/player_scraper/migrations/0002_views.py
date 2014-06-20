# -*- coding: utf-8 -*-
from south.v2 import DataMigration

class Migration(DataMigration):

    def forwards(self, orm):
        "Write your forwards methods here."
        # Note: Don't use "from appname.models import ModelName". 
        # Use orm.ModelName to refer to models in this application,
        # and orm['appname.ModelName'] for models in other applications.

        from ultimate_footy_analyzer.apps.player_scraper.util_migrations.south_input_ddl import South_Input_DDL
        
        objSP = South_Input_DDL()
        objSP.migrate_0002_views()
        objSP.migrate_0002_staging_indexes()


    def backwards(self, orm):
        "Write your backwards methods here."

        #objSP = South_Input_DDL()

    complete_apps = ['player_scraper']
    symmetrical = True
