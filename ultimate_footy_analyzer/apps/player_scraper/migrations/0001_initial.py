# -*- coding: utf-8 -*-
import datetime
from south.db import db
from south.v2 import SchemaMigration
from django.db import models


class Migration(SchemaMigration):

    def forwards(self, orm):
        # Adding model 'League'
        db.create_table('tbl_league', (
            ('league_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('league_siteid', self.gf('django.db.models.fields.IntegerField')()),
            ('league_name', self.gf('django.db.models.fields.CharField')(max_length=10)),
        ))
        db.send_create_signal(u'player_scraper', ['League'])

        # Adding model 'Player'
        db.create_table('tbl_player', (
            ('player_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('player_siteid', self.gf('django.db.models.fields.IntegerField')(max_length=200)),
            ('player_name', self.gf('django.db.models.fields.CharField')(max_length=100)),
        ))
        db.send_create_signal(u'player_scraper', ['Player'])

        # Adding model 'Player_stg'
        db.create_table('tbl_player_stg', (
            ('player_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('player_siteid', self.gf('django.db.models.fields.IntegerField')(max_length=200)),
            ('player_name', self.gf('django.db.models.fields.CharField')(max_length=100)),
        ))
        db.send_create_signal(u'player_scraper', ['Player_stg'])

        # Adding model 'PlayerStatus'
        db.create_table('tbl_playerstatus', (
            ('playerstatus_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('player', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['player_scraper.Player'])),
            ('league', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['player_scraper.League'])),
            ('season', self.gf('django.db.models.fields.IntegerField')()),
            ('observation_round', self.gf('django.db.models.fields.IntegerField')()),
            ('named_to_play', self.gf('django.db.models.fields.NullBooleanField')(null=True, blank=True)),
            ('named_emergency', self.gf('django.db.models.fields.NullBooleanField')(null=True, blank=True)),
            ('named_interchange', self.gf('django.db.models.fields.NullBooleanField')(null=True, blank=True)),
            ('not_playing', self.gf('django.db.models.fields.NullBooleanField')(null=True, blank=True)),
            ('injured_suspended', self.gf('django.db.models.fields.NullBooleanField')(null=True, blank=True)),
            ('hurt', self.gf('django.db.models.fields.NullBooleanField')(null=True, blank=True)),
            ('expected_return_round', self.gf('django.db.models.fields.IntegerField')(null=True)),
            ('out_for_season', self.gf('django.db.models.fields.NullBooleanField')(null=True, blank=True)),
            ('position', self.gf('django.db.models.fields.CharField')(max_length=10)),
            ('position_B', self.gf('django.db.models.fields.BooleanField')()),
            ('position_C', self.gf('django.db.models.fields.BooleanField')()),
            ('position_R', self.gf('django.db.models.fields.BooleanField')()),
            ('position_F', self.gf('django.db.models.fields.BooleanField')()),
            ('aflteam', self.gf('django.db.models.fields.CharField')(max_length=10)),
        ))
        db.send_create_signal(u'player_scraper', ['PlayerStatus'])

        # Adding model 'PlayerStatus_stg'
        db.create_table('tbl_playerstatus_stg', (
            ('playerstatus_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('player', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['player_scraper.Player'])),
            ('league', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['player_scraper.League'])),
            ('season', self.gf('django.db.models.fields.IntegerField')()),
            ('observation_round', self.gf('django.db.models.fields.IntegerField')()),
            ('named_to_play', self.gf('django.db.models.fields.NullBooleanField')(null=True, blank=True)),
            ('named_emergency', self.gf('django.db.models.fields.NullBooleanField')(null=True, blank=True)),
            ('named_interchange', self.gf('django.db.models.fields.NullBooleanField')(null=True, blank=True)),
            ('not_playing', self.gf('django.db.models.fields.NullBooleanField')(null=True, blank=True)),
            ('injured_suspended', self.gf('django.db.models.fields.NullBooleanField')(null=True, blank=True)),
            ('hurt', self.gf('django.db.models.fields.NullBooleanField')(null=True, blank=True)),
            ('expected_return_round', self.gf('django.db.models.fields.IntegerField')(null=True)),
            ('out_for_season', self.gf('django.db.models.fields.NullBooleanField')(null=True, blank=True)),
            ('position', self.gf('django.db.models.fields.CharField')(max_length=10)),
            ('position_B', self.gf('django.db.models.fields.BooleanField')()),
            ('position_C', self.gf('django.db.models.fields.BooleanField')()),
            ('position_R', self.gf('django.db.models.fields.BooleanField')()),
            ('position_F', self.gf('django.db.models.fields.BooleanField')()),
            ('aflteam', self.gf('django.db.models.fields.CharField')(max_length=10)),
        ))
        db.send_create_signal(u'player_scraper', ['PlayerStatus_stg'])

        # Adding model 'PlayerPerformance'
        db.create_table('tbl_playerperformance', (
            ('playerperf_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('player', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['player_scraper.Player'])),
            ('league', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['player_scraper.League'])),
            ('season', self.gf('django.db.models.fields.IntegerField')()),
            ('performance_type', self.gf('django.db.models.fields.CharField')(max_length=20)),
            ('performance_round', self.gf('django.db.models.fields.IntegerField')(max_length=10)),
            ('prediction_round', self.gf('django.db.models.fields.IntegerField')(max_length=10, null=True)),
            ('points', self.gf('django.db.models.fields.IntegerField')(max_length=10, null=True)),
            ('played', self.gf('django.db.models.fields.NullBooleanField')(max_length=10, null=True, blank=True)),
        ))
        db.send_create_signal(u'player_scraper', ['PlayerPerformance'])

        # Adding model 'PlayerPerformance_stg'
        db.create_table('tbl_playerperformance_stg', (
            ('playerperf_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('player', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['player_scraper.Player'])),
            ('league', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['player_scraper.League'])),
            ('season', self.gf('django.db.models.fields.IntegerField')()),
            ('performance_type', self.gf('django.db.models.fields.CharField')(max_length=20)),
            ('performance_round', self.gf('django.db.models.fields.IntegerField')(max_length=10)),
            ('prediction_round', self.gf('django.db.models.fields.IntegerField')(max_length=10, null=True)),
            ('points', self.gf('django.db.models.fields.IntegerField')(max_length=10, null=True)),
            ('played', self.gf('django.db.models.fields.NullBooleanField')(max_length=10, null=True, blank=True)),
        ))
        db.send_create_signal(u'player_scraper', ['PlayerPerformance_stg'])

        # Adding model 'Team'
        db.create_table('tbl_team', (
            ('team_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('team_siteid', self.gf('django.db.models.fields.IntegerField')(max_length=200)),
            ('league', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['player_scraper.League'])),
            ('season', self.gf('django.db.models.fields.IntegerField')()),
            ('team_name', self.gf('django.db.models.fields.CharField')(max_length=50)),
        ))
        db.send_create_signal(u'player_scraper', ['Team'])

        # Adding model 'Team_stg'
        db.create_table('tbl_team_stg', (
            ('team_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('team_siteid', self.gf('django.db.models.fields.IntegerField')(max_length=200)),
            ('league', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['player_scraper.League'])),
            ('season', self.gf('django.db.models.fields.IntegerField')()),
            ('team_name', self.gf('django.db.models.fields.CharField')(max_length=50)),
        ))
        db.send_create_signal(u'player_scraper', ['Team_stg'])

        # Adding model 'TeamPlayer'
        db.create_table('tbl_teamplayer', (
            ('teamplayer_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('team', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['player_scraper.Team'])),
            ('player', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['player_scraper.Player'])),
            ('start_date', self.gf('django.db.models.fields.DateField')()),
            ('end_date', self.gf('django.db.models.fields.DateField')()),
            ('active', self.gf('django.db.models.fields.BooleanField')()),
        ))
        db.send_create_signal(u'player_scraper', ['TeamPlayer'])

        # Adding model 'TeamPlayer_stg'
        db.create_table('tbl_teamplayer_stg', (
            ('teamplayer_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('team', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['player_scraper.Team'])),
            ('player', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['player_scraper.Player'])),
            ('start_date', self.gf('django.db.models.fields.DateField')()),
            ('end_date', self.gf('django.db.models.fields.DateField')()),
            ('active', self.gf('django.db.models.fields.BooleanField')()),
        ))
        db.send_create_signal(u'player_scraper', ['TeamPlayer_stg'])

        # Adding model 'TeamPlayerPosition'
        db.create_table('tbl_teamplayerposition', (
            ('teamplayerposition_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('team', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['player_scraper.Team'])),
            ('player', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['player_scraper.Player'])),
            ('observation_round', self.gf('django.db.models.fields.IntegerField')()),
            ('named_position', self.gf('django.db.models.fields.CharField')(max_length=2)),
            ('named_18', self.gf('django.db.models.fields.BooleanField')()),
            ('named_emergency', self.gf('django.db.models.fields.BooleanField')()),
            ('named_bench', self.gf('django.db.models.fields.BooleanField')()),
        ))
        db.send_create_signal(u'player_scraper', ['TeamPlayerPosition'])

        # Adding model 'TeamPlayerPosition_stg'
        db.create_table('tbl_teamplayerposition_stg', (
            ('teamplayerposition_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('team', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['player_scraper.Team'])),
            ('player', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['player_scraper.Player'])),
            ('observation_round', self.gf('django.db.models.fields.IntegerField')()),
            ('named_position', self.gf('django.db.models.fields.CharField')(max_length=2)),
            ('named_18', self.gf('django.db.models.fields.BooleanField')()),
            ('named_emergency', self.gf('django.db.models.fields.BooleanField')()),
            ('named_bench', self.gf('django.db.models.fields.BooleanField')()),
        ))
        db.send_create_signal(u'player_scraper', ['TeamPlayerPosition_stg'])


    def backwards(self, orm):
        # Deleting model 'League'
        db.delete_table('tbl_league')

        # Deleting model 'Player'
        db.delete_table('tbl_player')

        # Deleting model 'Player_stg'
        db.delete_table('tbl_player_stg')

        # Deleting model 'PlayerStatus'
        db.delete_table('tbl_playerstatus')

        # Deleting model 'PlayerStatus_stg'
        db.delete_table('tbl_playerstatus_stg')

        # Deleting model 'PlayerPerformance'
        db.delete_table('tbl_playerperformance')

        # Deleting model 'PlayerPerformance_stg'
        db.delete_table('tbl_playerperformance_stg')

        # Deleting model 'Team'
        db.delete_table('tbl_team')

        # Deleting model 'Team_stg'
        db.delete_table('tbl_team_stg')

        # Deleting model 'TeamPlayer'
        db.delete_table('tbl_teamplayer')

        # Deleting model 'TeamPlayer_stg'
        db.delete_table('tbl_teamplayer_stg')

        # Deleting model 'TeamPlayerPosition'
        db.delete_table('tbl_teamplayerposition')

        # Deleting model 'TeamPlayerPosition_stg'
        db.delete_table('tbl_teamplayerposition_stg')


    models = {
        u'player_scraper.league': {
            'Meta': {'object_name': 'League', 'db_table': "'tbl_league'"},
            'league_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'league_name': ('django.db.models.fields.CharField', [], {'max_length': '10'}),
            'league_siteid': ('django.db.models.fields.IntegerField', [], {})
        },
        u'player_scraper.player': {
            'Meta': {'object_name': 'Player', 'db_table': "'tbl_player'"},
            'player_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'player_name': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            'player_siteid': ('django.db.models.fields.IntegerField', [], {'max_length': '200'})
        },
        u'player_scraper.player_stg': {
            'Meta': {'object_name': 'Player_stg', 'db_table': "'tbl_player_stg'"},
            'player_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'player_name': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            'player_siteid': ('django.db.models.fields.IntegerField', [], {'max_length': '200'})
        },
        u'player_scraper.playerperformance': {
            'Meta': {'object_name': 'PlayerPerformance', 'db_table': "'tbl_playerperformance'"},
            'league': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['player_scraper.League']"}),
            'performance_round': ('django.db.models.fields.IntegerField', [], {'max_length': '10'}),
            'performance_type': ('django.db.models.fields.CharField', [], {'max_length': '20'}),
            'played': ('django.db.models.fields.NullBooleanField', [], {'max_length': '10', 'null': 'True', 'blank': 'True'}),
            'player': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['player_scraper.Player']"}),
            'playerperf_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'points': ('django.db.models.fields.IntegerField', [], {'max_length': '10', 'null': 'True'}),
            'prediction_round': ('django.db.models.fields.IntegerField', [], {'max_length': '10', 'null': 'True'}),
            'season': ('django.db.models.fields.IntegerField', [], {})
        },
        u'player_scraper.playerperformance_stg': {
            'Meta': {'object_name': 'PlayerPerformance_stg', 'db_table': "'tbl_playerperformance_stg'"},
            'league': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['player_scraper.League']"}),
            'performance_round': ('django.db.models.fields.IntegerField', [], {'max_length': '10'}),
            'performance_type': ('django.db.models.fields.CharField', [], {'max_length': '20'}),
            'played': ('django.db.models.fields.NullBooleanField', [], {'max_length': '10', 'null': 'True', 'blank': 'True'}),
            'player': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['player_scraper.Player']"}),
            'playerperf_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'points': ('django.db.models.fields.IntegerField', [], {'max_length': '10', 'null': 'True'}),
            'prediction_round': ('django.db.models.fields.IntegerField', [], {'max_length': '10', 'null': 'True'}),
            'season': ('django.db.models.fields.IntegerField', [], {})
        },
        u'player_scraper.playerstatus': {
            'Meta': {'object_name': 'PlayerStatus', 'db_table': "'tbl_playerstatus'"},
            'aflteam': ('django.db.models.fields.CharField', [], {'max_length': '10'}),
            'expected_return_round': ('django.db.models.fields.IntegerField', [], {'null': 'True'}),
            'hurt': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'}),
            'injured_suspended': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'}),
            'league': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['player_scraper.League']"}),
            'named_emergency': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'}),
            'named_interchange': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'}),
            'named_to_play': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'}),
            'not_playing': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'}),
            'observation_round': ('django.db.models.fields.IntegerField', [], {}),
            'out_for_season': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'}),
            'player': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['player_scraper.Player']"}),
            'playerstatus_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'position': ('django.db.models.fields.CharField', [], {'max_length': '10'}),
            'position_B': ('django.db.models.fields.BooleanField', [], {}),
            'position_C': ('django.db.models.fields.BooleanField', [], {}),
            'position_F': ('django.db.models.fields.BooleanField', [], {}),
            'position_R': ('django.db.models.fields.BooleanField', [], {}),
            'season': ('django.db.models.fields.IntegerField', [], {})
        },
        u'player_scraper.playerstatus_stg': {
            'Meta': {'object_name': 'PlayerStatus_stg', 'db_table': "'tbl_playerstatus_stg'"},
            'aflteam': ('django.db.models.fields.CharField', [], {'max_length': '10'}),
            'expected_return_round': ('django.db.models.fields.IntegerField', [], {'null': 'True'}),
            'hurt': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'}),
            'injured_suspended': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'}),
            'league': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['player_scraper.League']"}),
            'named_emergency': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'}),
            'named_interchange': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'}),
            'named_to_play': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'}),
            'not_playing': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'}),
            'observation_round': ('django.db.models.fields.IntegerField', [], {}),
            'out_for_season': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'}),
            'player': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['player_scraper.Player']"}),
            'playerstatus_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'position': ('django.db.models.fields.CharField', [], {'max_length': '10'}),
            'position_B': ('django.db.models.fields.BooleanField', [], {}),
            'position_C': ('django.db.models.fields.BooleanField', [], {}),
            'position_F': ('django.db.models.fields.BooleanField', [], {}),
            'position_R': ('django.db.models.fields.BooleanField', [], {}),
            'season': ('django.db.models.fields.IntegerField', [], {})
        },
        u'player_scraper.team': {
            'Meta': {'object_name': 'Team', 'db_table': "'tbl_team'"},
            'league': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['player_scraper.League']"}),
            'season': ('django.db.models.fields.IntegerField', [], {}),
            'team_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'team_name': ('django.db.models.fields.CharField', [], {'max_length': '50'}),
            'team_siteid': ('django.db.models.fields.IntegerField', [], {'max_length': '200'})
        },
        u'player_scraper.team_stg': {
            'Meta': {'object_name': 'Team_stg', 'db_table': "'tbl_team_stg'"},
            'league': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['player_scraper.League']"}),
            'season': ('django.db.models.fields.IntegerField', [], {}),
            'team_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'team_name': ('django.db.models.fields.CharField', [], {'max_length': '50'}),
            'team_siteid': ('django.db.models.fields.IntegerField', [], {'max_length': '200'})
        },
        u'player_scraper.teamplayer': {
            'Meta': {'object_name': 'TeamPlayer', 'db_table': "'tbl_teamplayer'"},
            'active': ('django.db.models.fields.BooleanField', [], {}),
            'end_date': ('django.db.models.fields.DateField', [], {}),
            'player': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['player_scraper.Player']"}),
            'start_date': ('django.db.models.fields.DateField', [], {}),
            'team': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['player_scraper.Team']"}),
            'teamplayer_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'})
        },
        u'player_scraper.teamplayer_stg': {
            'Meta': {'object_name': 'TeamPlayer_stg', 'db_table': "'tbl_teamplayer_stg'"},
            'active': ('django.db.models.fields.BooleanField', [], {}),
            'end_date': ('django.db.models.fields.DateField', [], {}),
            'player': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['player_scraper.Player']"}),
            'start_date': ('django.db.models.fields.DateField', [], {}),
            'team': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['player_scraper.Team']"}),
            'teamplayer_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'})
        },
        u'player_scraper.teamplayerposition': {
            'Meta': {'object_name': 'TeamPlayerPosition', 'db_table': "'tbl_teamplayerposition'"},
            'named_18': ('django.db.models.fields.BooleanField', [], {}),
            'named_bench': ('django.db.models.fields.BooleanField', [], {}),
            'named_emergency': ('django.db.models.fields.BooleanField', [], {}),
            'named_position': ('django.db.models.fields.CharField', [], {'max_length': '2'}),
            'observation_round': ('django.db.models.fields.IntegerField', [], {}),
            'player': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['player_scraper.Player']"}),
            'team': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['player_scraper.Team']"}),
            'teamplayerposition_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'})
        },
        u'player_scraper.teamplayerposition_stg': {
            'Meta': {'object_name': 'TeamPlayerPosition_stg', 'db_table': "'tbl_teamplayerposition_stg'"},
            'named_18': ('django.db.models.fields.BooleanField', [], {}),
            'named_bench': ('django.db.models.fields.BooleanField', [], {}),
            'named_emergency': ('django.db.models.fields.BooleanField', [], {}),
            'named_position': ('django.db.models.fields.CharField', [], {'max_length': '2'}),
            'observation_round': ('django.db.models.fields.IntegerField', [], {}),
            'player': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['player_scraper.Player']"}),
            'team': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['player_scraper.Team']"}),
            'teamplayerposition_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'})
        }
    }

    complete_apps = ['player_scraper']