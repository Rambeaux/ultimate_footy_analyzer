#django
from django.db import models

#project
from libs.Util import *
from django_postgres_dataloader import *


#system
import sys, traceback
import numpy as np
from datetime import datetime
from struct import pack


"""
    Dummy reference tables to link to via foreign keys in the example
    
"""
class ReferenceOne(models.Model):

    reference_one_id = models.AutoField(primary_key=True)
    reference_one_name = models.CharField(max_length=10)

    def __unicode__(self):
        return self.reference_one_name

    class Meta:
        db_table = "tbl_referenceone"
        
class ReferenceTwo(models.Model):

    reference_two_id = models.AutoField(primary_key=True)
    reference_two_name = models.CharField(max_length=10)

    def __unicode__(self):
        return self.reference_two_name

    class Meta:
        db_table = "tbl_referencetwo"        
        

"""
    Example 1: One Step Load
    
    Data is loaded directly into this table via postgres' COPY bulkload method
    
"""
@BulkLoadable
class OneStepLoad(models.Model):

    onestepload_id       = models.AutoField(primary_key=True)
    
    reference_one   = models.ForeignKey(ReferenceOne, null=False)
    reference_two   = models.ForeignKey(ReferenceTwo, null=False)
    
    data_int      = models.IntegerField()
    data_float    = models.FloatField()
    data_string   = models.CharField(max_length=100)
    data_date     = models.DateField()


    def __unicode__(self):
        return self.league_name

    class Meta:
        db_table = "tbl_onestepload"

"""
    Example 2: Two Step Load

    1. Firstly a 'base' model is defined, which specifies the table structure.
    This base class is defined as abstract in the Meta class - this means django will not create a table for this model class. 
    
    2. The base class is sub-classed twice: 
        - for the 'production' table - the final target table of your data,
        - for the 'staging' table, where new and updated records can be inserted before
         an 'upsert' (combined update and insert) is performed to load the correct data into the production table.
         
        These two model classes inherit the same fields from the base class.
        Note that Django does not permit the inbuilt django 'bulk_upload' process to be performed on inherited model classes.
        
    3. A concept of Keys and Data fields is necessary for an upsert process to make sense.
        Keys are the fields that uniquely identify the row, in a meaningful way (as opposed to an arbitrary auto-generated field)
        These are fields that may typically be included in an 'index_together' or 'unique_together' list.
        
        Data fields are the information that is referenced by a given combination of keys, and the upsert process means they may be updated at some stage after the initial data load.
         
        Two dictionaries are required to control the upsert process: key_fields and data_fields

"""
#Base model class
class TwoStepLoad_base(models.Model):
    twostepload_id       = models.AutoField(primary_key=True)
    
    #key fields
    reference_one   = models.ForeignKey(ReferenceOne, null=False)
    reference_two   = models.ForeignKey(ReferenceTwo, null=False)
    reference_date  = models.DateField(null=False)

    #data fields
    data_int      = models.IntegerField()
    data_float    = models.FloatField()
    data_string   = models.CharField(max_length=100)


    key_fields = {"reference_one_id", "reference_two_id", "reference_date"}
    data_fields = {"data_int", "data_float", "data_string" }
    
    def __unicode__(self):
        return "%s %s, %d" % self.reference_one, self.reference_two, self.reference_date

    class Meta:
        abstract = True

        index_together = [["reference_one", "reference_two", "reference_date"]]
        unique_together = ("reference_one", "reference_two", "reference_date")




#Production table model class
class TwoStepLoad(TwoStepLoad_base):

    class Meta:
        db_table = "tbl_twostepload"

#Staging table model class       
@BulkLoadable
class TwoStepLoad_stg(TwoStepLoad_base):

    class Meta:
        db_table = "tbl_twostepload_stg"
        





