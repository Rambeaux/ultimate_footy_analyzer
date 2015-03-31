"""
    @class:     BulkLoadable
    @purpose:    Apply this as a decorator to a django model class to allow FAST bulk uploading to postgres databases via a psycopg2 connection
        
        
        
    There are two use-cases:
        1. A 1-stage simple fast bulkload into a table.
        2. A 2-stage bulkoad into a staging table, followed by an 'upsert' from the staging into a final target table.

    

    TODO:
        Develop the binary streaming COPY method


"""



#django
from django.db import models, transaction

#data structures
from datetime import datetime
from struct import pack
import numpy as np

#python
import sys, traceback
from io import BytesIO

"""
    BulkLoadable  Class
    @purpose:    Apply this as a decorator to a django model class to allow FAST bulk uploading to postgres databases via a psycopg2 connection

"""

def BulkLoadable(Class):

    #Bulk insert attributes for staging
    setattr(Class, "fields_list", None)
    setattr(Class, "column_names", None)
    setattr(Class, "column_names_list", None)
    setattr(Class, "column_types_list", None)

    #flag fields that require quotes to be inserted.
    setattr(Class, "quotables", {})

    
    """==========
         Construct a list of field names
       =========="""
       
    # initialising function to build a list of field types, often referenced by other functions in this class
    def get_fields_list(self, con):
        
        if self.fields_list == None:
            self.fields_list = [ f for f in self._meta.fields if not isinstance( f, models.AutoField) ]    

        return self.fields_list
    
    setattr(Class, "get_fields_list", get_fields_list)


    #Returns a comma-separated list of field names
    def get_column_names_string(self, con):

        self.get_fields_list(con)
        if self.column_names == None:
            self.column_names = ",".join(con.ops.quote_name(f.column) for f in self.fields_list)

        return self.column_names    
    
    setattr(Class, "get_column_names_string", get_column_names_string)


    #Returns a dictionary of column types, indexed by column name
    def get_column_types_dict(self, con):

        self.get_fields_list(con)
        
        if self.column_types_list == None:
            
            self.column_types_list = {}
            
            for f in self.fields_list:
                self.column_types_list[(con.ops.quote_name(f.column))] = type(f)
                
        return self.column_types_list  
    
    setattr(Class, "get_column_types_dict", get_column_types_dict)

    #Returns an iterable list of column names    
    def get_column_names_list(self, con):

        self.get_fields_list(con)
        
        if self.column_names_list == None:
            
            self.column_names_list = []
            for f in self.fields_list:
                self.column_names_list.append(con.ops.quote_name(f.column))
                
        return self.column_names_list    
    
    setattr(Class, "get_column_names_list", get_column_names_list)  




    """==========
         Construct an array of bulkloadable values from the current django model instance
       =========="""


    #==============================
    #NOTE: THIS IS NOT YET WORKING!
    #==============================
    #Convert an instance of a djano model object into a bulkloadable array of biary values, ready for loading.
    def get_bulkinsert_binaryvalues(self, con):
        
        self.get_fields_list(con)
        
        vals = []
        
        null_val = ""
        
        format_lookup = { 
                            'BigIntegerField': {'size': 8, 'format': '!i'},                         
                            'BooleanField':    {'size': 1, 'format': '!?'},
                            'CharField':       {'size': -1, 'format': '!s', },                            
                            'DateField':       {'size': 4, 'format': '!i'},                         
                            'DateTimeField':   {'size': 4, 'format': '!i'},                         
                            'FloatField':      {'size': 8, 'format': '!d'},                         
                            'ForeignKey':      {'size': 4, 'format': '!i'},
                            'IntegerField':    {'size': 4, 'format': '!i'},                         
                            'NullBooleanField': {'size': 1, 'format': '!?'},
                         }       
        
        
        POSTGRES_EPOCH_DATE = datetime.date(1970,1,1)
        
        
        for f in self.fields_list:
            
            
            fclass = type(f)
            fvalue = f.get_db_prep_save(f.pre_save(self, True), connection=con )
            
            size = None
            pgtype = None
            format = None
            binaryvalue = None
            
            try:           
                if fclass == models.AutoField:
                    pass
                else:

                    if fclass == models.BigIntegerField:
                        size= format_lookup['BigIntegerField']['size']
                        format= format_lookup['BigIntegerField']['format']
                        binaryvalue= pack('!il', size, fvalue)
                        
                    if fclass == models.BooleanField:
                        size= format_lookup['BooleanField']['size']
                        format= format_lookup['BooleanField']['format']
                        binaryvalue= pack('!i?', size,  fvalue)
                                            
                    if fclass == models.CharField:
                        
                        size= sys.getsizeof(fvalue)
                        format= format_lookup['CharField']['format']
                        binaryvalue= pack('!i{size}s'.format(size=size), size, fvalue)
                        
                    if fclass == models.TextField:
                        size= sys.getsizeof(fvalue)
                        format= format_lookup['TextField']['format']
                        binaryvalue= pack('!i{size}s'.format(size=size), size, fvalue)

                        
                    if fclass == models.DateField:
                        size= format_lookup['DateField']['size']
                        format= format_lookup['DateField']['format']
                        
                        if fvalue != None:
                            datediff = fvalue - POSTGRES_EPOCH_DATE
                            secondsdiff = int(datediff.days) * 3600*24
                        else:
                            secondsdiff = 0
                        #format a date into the postgres integer format
                        binaryvalue= pack('!ii', size,  secondsdiff )
                        

                    if fclass == models.DateTimeField:
                        size= format_lookup['DateTimeField']['size']
                        format= format_lookup['DateTimeField']['format']
                    
                    if fclass == models.DecimalField:
                        size= format_lookup['DecimalField']['size']
                        format= format_lookup['DecimalField']['format']
                        binaryvalue= pack('!if', size, fvalue)
                    
                    if fclass == models.FloatField:
                        size= format_lookup['FloatField']['size']
                        format= format_lookup['FloatField']['format']
                        binaryvalue= pack('!if', size,  fvalue)
                    
                    if fclass == models.ForeignKey:
                        size= format_lookup['ForeignKey']['size']
                        format= format_lookup['ForeignKey']['format']
                        
                        if fvalue != None:
                            fkval = int(fvalue)
                        else:
                            fkval = 0
                        
                        binaryvalue= pack('!ii', size, fkval)
                    
                    if fclass == models.IntegerField:
                        size= format_lookup['IntegerField']['size']
                        format= format_lookup['IntegerField']['format']
                        binaryvalue= pack('!ii',  fvalue)
                    
                    if fclass == models.NullBooleanField:
                        size= format_lookup['NullBooleanField']['size']
                        format= format_lookup['NullBooleanField']['format']
                        binaryvalue= pack('!i?', size,  fvalue)
                    
                    if fclass == models.PositiveIntegerField:
                        size= format_lookup['PositiveIntegerField']['size']
                        format= format_lookup['PositiveIntegerField']['format']
                        binaryvalue= pack('!iI', size, fvalue)
                    
                    if fclass == models.PositiveSmallIntegerField:
                        size= format_lookup['PositiveSmallIntegerField']['size']
                        format= format_lookup['PositiveSmallIntegerField']['format']
                        binaryvalue= pack('!iH', size, fvalue)
                    
                    if fclass == models.SmallIntegerField:
                        size= format_lookup['SmallIntegerField']['size']
                        format= format_lookup['SmallIntegerField']['format']
                        binaryvalue= pack('!ih', size, fvalue)
                    
                    if fclass == models.TimeField:
                        size= format_lookup['TimeField']['size']
                        format= format_lookup['TimeField']['format']
                        #maybe not right for a timefield, but close...
                        binaryvalue = pack('!il',size,  datetime.timedelta( fvalue ,POSTGRES_EPOCH_DATE).seconds)
                    
                    if fclass == models.IPAddressField:
                        pass
                    if fclass == models.GenericIPAddressField:
                        pass
                    if fclass == models.EmailField:
                        pass
                    if fclass == models.CommaSeparatedIntegerField:
                        pass
                    if fclass == models.SlugField:
                        pass
                    if fclass == models.URLField:
                        pass
                    
                    print "RawVal: {0} BinaryVal {1} Size:{2}".format(fvalue, binaryvalue,size)
                    
                    vals.append( binaryvalue  )
            except:
                #If something screws up, insert a null...
                vals.append(pack('!i', -1))
                
        #return a numpy array...          
        retvals = np.array(vals)
        
        return retvals
    
    setattr(Class, "get_bulkinsert_binaryvalues", get_bulkinsert_binaryvalues)
    
    
    #Convert an instance of a djano model object into a bulkloadable array of text values, ready for loading.
    def get_bulkinsert_textvalues_array(self, con, null_value=None):
        
        #initialise fields list
        self.get_fields_list(con)
        
        #array representing the values in the current object to be inserted as a row
        vals = []
        
        #The value to stream for NULL data values. Recognised by postgres as a NULL.
        if null_value == None:
            null_val = "\\N"
        else:
            null_val = null_value
            
        #loop through fields
        for f in self.fields_list:
            
            try:           
                #We don't want to bulk insert autofields. This is the database's problem to generate.
                if isinstance( f, models.AutoField):
                    pass
                else:
                    
                    #Character and Date fields need to be quoted
                    if isinstance( f, models.CharField) or isinstance( f, models.DateField):
                        
                        val = "{0}".format(f.get_db_prep_save(f.pre_save(self, True), connection=con ))
                        
                        #If raw value represents a "None" object, then this really should be a null
                        if val in ("'None'", "None"):
                            val=null_val
                            
                    #Float fields cast as float       
                    elif isinstance( f, models.FloatField): 
                        val = str(float(f.get_db_prep_save( f.pre_save(self, True), connection=con )))
                      
                    #Int fields cast as int       
                    elif isinstance( f, models.IntegerField): 
                        val = str(int(f.get_db_prep_save( f.pre_save(self, True), connection=con )))

                    #ForeignKey fields cast as int       
                    elif isinstance( f, models.ForeignKey): 
                        val = str(int(f.get_db_prep_save( f.pre_save(self, True), connection=con )))

                    #Other fields, take them as they are                            
                    else:
                        val = f.get_db_prep_save( f.pre_save(self, True), connection=con )
                
                    #If the value is null, then we need to substitute the postgress NULL indicator
                    if val == None:
                        val = null_val                 
                    
                    #add this value to the array representing this row.
                    vals.append( val  )
            except:
                #If something screws up, just insert a null
                vals.append(null_val)
        
        #tup = tuple(vals)
        retvals = np.array(vals)
        
        return retvals
    
    setattr(Class, "get_bulkinsert_textvalues_array", get_bulkinsert_textvalues_array)    

    
    
    """ Build and execute the UPSERT query - performs an update and insert.
        REquires that dictionaries key_fields and data_fields are specified on the staging table's meta class.
    """
    @transaction.commit_on_success          
    def execute_upsert(self, con, p_key_fields=None, p_data_fields=None, prod_tablename=None, print_query=False):


        #These need to be specified in the meta
        # The 'key' fields represent the fields used to join the staging to the production table
        # the key fields are usually what you would place in an 'index_together' or 'unique_together' 
        # DO NOT include an auto-generated primary key in this 
        if p_key_fields == None:
            
            if self.data_fields == None:
                e = Exception("ERROR: data_fields not specified in model: {0}".format(self))
                raise Exception(e)
            else:
                #use the values specified in the meta class
                key_fields = self.key_fields
        else:
            #use the values specified in the function call
            key_fields = p_key_fields
            
        
        #datafields are all the other fields in the table whose values may chnage over time, and hence require updating/inserting.
        if p_key_fields == None:
            
            #TODO: Perform better error checking for meta class configuration
            if self.key_fields == None:
                e = Exception("ERROR: key_fields not specified in model: {0}".format(self))
                raise Exception(e)
            else:
                #use the values specified in the meta class
                data_fields = self.data_fields
                
        else:
            #use the values specified in the function call
            data_fields = p_data_fields



        #the current class's table is the staging table for loading into.
        stg_tablename = self._meta.db_table

        #if production tablename is not specified, assume the staging tablename excluding the trailing '_stg'
        if prod_tablename == None:
            #TODO: Error check this...!
            prod_tablename = stg_tablename[:-4]



        """ ========================
            Start building the query
            ========================
            """        
        #Build update set clause
        update_set = ' SET '
        for data_field in data_fields:
            update_set = update_set + " {data_field} = {stg_tablename}.{data_field},".format(data_field=data_field, stg_tablename=stg_tablename)

        update_set = update_set[:-1]
        
        #Build update where clause
        update_where = ' WHERE '
        for key_field in key_fields:
            update_where = update_where + " {prod_tablename}.{key_field} = {stg_tablename}.{key_field} AND".format(key_field=key_field, stg_tablename=stg_tablename, prod_tablename=prod_tablename)

        update_where = update_where[:-3]


        #Build delete where clause
        delete_where = ' WHERE '
        for key_field in key_fields:
            delete_where = delete_where + " prod.{key_field} = stg.{key_field} AND".format(key_field=key_field, stg_tablename=stg_tablename, prod_tablename=prod_tablename)

        #remove the trailing 'AND'
        delete_where = delete_where[:-3]
        
        #get the column names string (a comma-separated list of field names)
        field_list = self.get_column_names_string(con)     
            
            
        #the shell of the UPSERT query.
        _upsert_query = """ UPDATE {prod_tablename}
        
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
              
        #print, if requested
        if print_query:
            print(_upsert_query)
              
        #EXECUTE the constructed query
        try:
                        
            cursor = con.cursor()  
            cursor.execute(_upsert_query)
            con.commit() 
                
        except:
            print("Error: upsert_from_staging {0}".format(prod_tablename) )  
           
    setattr(Class, "execute_upsert", execute_upsert)    
           
           
    #deletes all values currently in the staging table.
    @transaction.commit_on_success      
    def clear_staging_table(self, con, table_name=None):

        #set default table to the staging table of the model.
        if table_name == None:
            table_name = self._meta.db_table

        try:            
            cursor = con.cursor()
            sql = "DELETE FROM {table_name};".format(table_name=table_name)
            cursor.execute( sql )

                
        except Exception, e:
            print("Error: clear_staging_table(): {0}, {1}".format(e, sql) )    

    setattr(Class, "clear_staging_table", clear_staging_table)    



    # Performs a bulkload into Postgres table. 
    # Requires a list of Django Bulkloadable model objects to be passed to this 
        
    def bulk_insert_textstream(self, objects_list, con):

        #cache the table names form the first object in the list.
        #initially set as None, these will be extracted only the once.
        column_names = None
        table_name = None
                
        numinserted = 0
                
        #Only run if there are records to be loaded
        if len(objects_list) > 0:
            
            try:
                # Make a text file stream object to stream to the COPY FROM postgres interface
                cpy = BytesIO()

                #iterate through the objects in the list.
                for object in objects_list:
                    
                    #get the column names and tablename from the first object in the list
                    if column_names == None:
                        column_names = [str( object.get_column_names_string(con) )]
                        
                    if table_name == None:
                        table_name = object._meta.db_table
                    
                    
                    #get the values from the current object
                    valuelist = object.get_bulkinsert_textvalues_array(con)

                    #format the values list for the text stream                    
                    fmtstr = '\t'.join([ fldval for fldval in valuelist]) + '\n'
                    
                    #write the values to the text stream
                    cpy.write(fmtstr)

                # Execute the postgres COPY process to upload data to database via copy_from()
                cpy.seek(0)
                cursor = con.cursor()
                cursor.copy_from(cpy, table_name, columns=column_names)
                con.commit()
                
            
            except Exception, e:
                print ("ERROR: Bulk Load Failed Load for {0}: {1}".format(table_name, str(e)))   

        numinserted = len(objects_list)
        return numinserted
        

    setattr(Class, "bulk_insert_textstream", bulk_insert_textstream)    

    return Class 