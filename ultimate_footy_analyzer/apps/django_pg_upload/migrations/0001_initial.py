# -*- coding: utf-8 -*-
import datetime
from south.db import db
from south.v2 import SchemaMigration
from django.db import models


class Migration(SchemaMigration):

    def forwards(self, orm):
        # Adding model 'ReferenceOne'
        db.create_table('tbl_referenceone', (
            ('reference_one_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('reference_one_name', self.gf('django.db.models.fields.CharField')(max_length=10)),
        ))
        db.send_create_signal(u'django_pg_upload', ['ReferenceOne'])

        # Adding model 'ReferenceTwo'
        db.create_table('tbl_referencetwo', (
            ('reference_two_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('reference_two_name', self.gf('django.db.models.fields.CharField')(max_length=10)),
        ))
        db.send_create_signal(u'django_pg_upload', ['ReferenceTwo'])

        # Adding model 'OneStepLoad'
        db.create_table('tbl_onestepload', (
            ('onestepload_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('reference_one', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['django_pg_upload.ReferenceOne'])),
            ('reference_two', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['django_pg_upload.ReferenceTwo'])),
            ('data_int', self.gf('django.db.models.fields.IntegerField')()),
            ('data_float', self.gf('django.db.models.fields.FloatField')()),
            ('data_string', self.gf('django.db.models.fields.CharField')(max_length=100)),
            ('data_date', self.gf('django.db.models.fields.DateField')()),
        ))
        db.send_create_signal(u'django_pg_upload', ['OneStepLoad'])

        # Adding model 'TwoStepLoad'
        db.create_table('tbl_twostepload', (
            ('twostepload_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('reference_one', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['django_pg_upload.ReferenceOne'])),
            ('reference_two', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['django_pg_upload.ReferenceTwo'])),
            ('reference_date', self.gf('django.db.models.fields.DateField')()),
            ('data_int', self.gf('django.db.models.fields.IntegerField')()),
            ('data_float', self.gf('django.db.models.fields.FloatField')()),
            ('data_string', self.gf('django.db.models.fields.CharField')(max_length=100)),
        ))
        db.send_create_signal(u'django_pg_upload', ['TwoStepLoad'])

        # Adding model 'TwoStepLoad_stg'
        db.create_table('tbl_twostepload_stg', (
            ('twostepload_id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('reference_one', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['django_pg_upload.ReferenceOne'])),
            ('reference_two', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['django_pg_upload.ReferenceTwo'])),
            ('reference_date', self.gf('django.db.models.fields.DateField')()),
            ('data_int', self.gf('django.db.models.fields.IntegerField')()),
            ('data_float', self.gf('django.db.models.fields.FloatField')()),
            ('data_string', self.gf('django.db.models.fields.CharField')(max_length=100)),
        ))
        db.send_create_signal(u'django_pg_upload', ['TwoStepLoad_stg'])


    def backwards(self, orm):
        # Deleting model 'ReferenceOne'
        db.delete_table('tbl_referenceone')

        # Deleting model 'ReferenceTwo'
        db.delete_table('tbl_referencetwo')

        # Deleting model 'OneStepLoad'
        db.delete_table('tbl_onestepload')

        # Deleting model 'TwoStepLoad'
        db.delete_table('tbl_twostepload')

        # Deleting model 'TwoStepLoad_stg'
        db.delete_table('tbl_twostepload_stg')


    models = {
        u'django_pg_upload.onestepload': {
            'Meta': {'object_name': 'OneStepLoad', 'db_table': "'tbl_onestepload'"},
            'data_date': ('django.db.models.fields.DateField', [], {}),
            'data_float': ('django.db.models.fields.FloatField', [], {}),
            'data_int': ('django.db.models.fields.IntegerField', [], {}),
            'data_string': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            'onestepload_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'reference_one': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['django_pg_upload.ReferenceOne']"}),
            'reference_two': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['django_pg_upload.ReferenceTwo']"})
        },
        u'django_pg_upload.referenceone': {
            'Meta': {'object_name': 'ReferenceOne', 'db_table': "'tbl_referenceone'"},
            'reference_one_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'reference_one_name': ('django.db.models.fields.CharField', [], {'max_length': '10'})
        },
        u'django_pg_upload.referencetwo': {
            'Meta': {'object_name': 'ReferenceTwo', 'db_table': "'tbl_referencetwo'"},
            'reference_two_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'reference_two_name': ('django.db.models.fields.CharField', [], {'max_length': '10'})
        },
        u'django_pg_upload.twostepload': {
            'Meta': {'object_name': 'TwoStepLoad', 'db_table': "'tbl_twostepload'"},
            'data_float': ('django.db.models.fields.FloatField', [], {}),
            'data_int': ('django.db.models.fields.IntegerField', [], {}),
            'data_string': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            'reference_date': ('django.db.models.fields.DateField', [], {}),
            'reference_one': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['django_pg_upload.ReferenceOne']"}),
            'reference_two': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['django_pg_upload.ReferenceTwo']"}),
            'twostepload_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'})
        },
        u'django_pg_upload.twostepload_stg': {
            'Meta': {'object_name': 'TwoStepLoad_stg', 'db_table': "'tbl_twostepload_stg'"},
            'data_float': ('django.db.models.fields.FloatField', [], {}),
            'data_int': ('django.db.models.fields.IntegerField', [], {}),
            'data_string': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            'reference_date': ('django.db.models.fields.DateField', [], {}),
            'reference_one': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['django_pg_upload.ReferenceOne']"}),
            'reference_two': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['django_pg_upload.ReferenceTwo']"}),
            'twostepload_id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'})
        }
    }

    complete_apps = ['django_pg_upload']