from django.db import transaction, models, connection
from datetime import datetime

from struct import pack
import sys, traceback
import numpy as np


class Singleton:
    """
    A non-thread-safe helper class to ease implementing singletons.
    This should be used as a decorator -- not a metaclass -- to the
    class that should be a singleton.

    The decorated class can define one `__init__` function that
    takes only the `self` argument. Other than that, there are
    no restrictions that apply to the decorated class.

    To get the singleton instance, use the `Instance` method. Trying
    to use `__call__` will result in a `TypeError` being raised.

    Limitations: The decorated class cannot be inherited from.

    """

    def __init__(self, decorated):
        self._decorated = decorated

    def Instance(self):
        """
        Returns the singleton instance. Upon its first call, it creates a
        new instance of the decorated class and calls its `__init__` method.
        On all subsequent calls, the already created instance is returned.

        """
        try:
            return self._instance
        except AttributeError:
            self._instance = self._decorated()
            return self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._decorated)
    
    
    
    
"""
    Constants classes - 
    Allows constants classes and properties similar to 'public static final' properties in Java
    code from http://stackoverflow.com/questions/2682745/creating-constant-in-python

"""    
def constant(f):
    def fset(self, value):
        raise SyntaxError
    def fget(self):
        return f()
    return property(fget, fset)




# Bulk insert/update DB operations for the Django ORM. Useful when
# inserting/updating lots of objects where the bottleneck is overhead
# in talking to the database. Instead of doing this
#
#   for x in seq:
#       o = SomeObject()
#       o.foo = x
#       o.save()
#
# or equivalently this
#
#   for x in seq:
#       SomeObject.objects.create(foo=x)
#
# do this
#
#   l = []
#   for x in seq:
#       o = SomeObject()
#       o.foo = x
#       l.append(o)
#   insert_many(l)
#
# Note that these operations are really simple. They won't work with
# many-to-many relationships, and you may have to divide really big
# lists into smaller chunks before sending them through.
#
# History
# 2010-12-10: quote column names, reported by Beres Botond. 
#
# From: http://people.iola.dk/olau/python/bulkops.py

@transaction.commit_on_success        
def djangosql_insert_many(objects, using="default"):
    """Insert list of Django objects in one SQL query. Objects must be
    of the same Django model. Note that save is not called and signals
    on the model are not raised."""
    if not objects:
        return

    import django.db.models
    from django.db import connections
    con = connections[using]


    startBuildParams = datetime.now()
    
    model = objects[0].__class__    
    fields = [ f for f in model._meta.fields if not isinstance( f, django.db.models.AutoField) ]    
    parameters = []
    
    for o in objects:
        #print (f.colum for f in fields)
        #TODO: I suspect this is what is slowing things down - especially for ForeignKey Fields
        parameters.append(  tuple(  f.get_db_prep_save( f.pre_save(o, True), connection=con ) for f in fields))
        
    #print "Parameters:{0}".format(parameters)

    #TODO: Make sure that the string formats equate with the type of field
    #Make an array of field types and associated string formats


    table = model._meta.db_table
    column_names = ",".join(con.ops.quote_name(f.column) for f in fields)
    placeholders = ",".join(("%s",) * len(fields))
    
    sql_str = "insert into %s (%s) values (%s)" % (table, column_names, placeholders)
    
    
    #print sql_str+": {0}".format(sql_str)
    
    startExecute = datetime.now()
    
    con.cursor().executemany(sql_str, parameters)

    print "Time to execute insert: {0}".format(datetime.now() - startExecute)       


def djangosql_update_many(objects, fields=[], using="default"):
    """Update list of Django objects in one SQL query, optionally only
    overwrite the given fields (as names, e.g. fields=["foo"]).
    Objects must be of the same Django model. Note that save is not
    called and signals on the model are not raised."""
    if not objects:
        return

    import django.db.models
    from django.db import connections
    con = connections[using]

    names = fields
    meta = objects[0]._meta
    fields = [f for f in meta.fields if not isinstance(f, django.db.models.AutoField) and (not names or f.name in names)]

    if not fields:
        raise ValueError("No fields to update, field names are %s." % names)
    
    fields_with_pk = fields + [meta.pk]
    parameters = []
    for o in objects:
        parameters.append(tuple(f.get_db_prep_save(f.pre_save(o, True), connection=con) for f in fields_with_pk))

    table = meta.db_table
    assignments = ",".join(("%s=%%s"% con.ops.quote_name(f.column)) for f in fields)
    con.cursor().executemany(
        "update %s set %s where %s=%%s" % (table, assignments, con.ops.quote_name(meta.pk.column)),
        parameters)
    
    
    
class BulkInsertable():
    
    sql_fields = None
    sql_template = None
    
    def __init__(self, decorated):
        pass
    def __call__(self):
        pass
        #raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._decorated)
    
    
    def get_bulkinsert_fields(self, con):
        
        if self.sql_fields == None:
            self.sql_fields = [ f for f in self._meta.fields if not isinstance( f, models.AutoField) ]    

        return self.sql_fields

    def get_bulkinsert_parameters(self, con):
        
        vals = tuple(  f.get_db_prep_save( f.pre_save(self, True), connection=con ) for f in self.sql_fields)
        return vals

    def get_bulkinsert_sql(self, con):

        if self.sql_template == None:
            table = self._meta.db_table
            column_names = ",".join(con.ops.quote_name(f.column) for f in self.sql_fields)
            placeholders = ",".join(("%s",) * len(self.sql_fields))
            
            self.sql_template = "insert into %s (%s) values (%s)" % (table, column_names, placeholders)

        return self.sql_template  
    

"""
    Bulk Load Base Class
    @purpose:    subclass this to allow for bulk load code generation into the subclass' table

"""

def BulkLoadable(Class):

    #Bulk insert attributes for staging
    setattr(Class, "sql_fields", None)
    setattr(Class, "sql_template", None)
    setattr(Class, "sql_template_header", None)
    setattr(Class, "sql_template_values", None)
    setattr(Class, "column_names", None)
    setattr(Class, "column_names_list", None)
    setattr(Class, "column_types_list", None)

    #flag fields that require quotes to be inserted.
    setattr(Class, "quotables", {})

    
    """==========
         Construct a list of field names
       =========="""
    def get_bulkinsert_fieldlist(self, con):
        
        if self.sql_fields == None:
            self.sql_fields = [ f for f in self._meta.fields if not isinstance( f, models.AutoField) ]    

        return self.sql_fields
    
    setattr(Class, "get_bulkinsert_fieldlist", get_bulkinsert_fieldlist)


    def get_column_names(self, con):

        self.get_bulkinsert_fieldlist(con)
        if self.column_names == None:
            self.column_names = ",".join(con.ops.quote_name(f.column) for f in self.sql_fields)

        return self.column_names    
    
    setattr(Class, "get_column_names", get_column_names)


    def get_column_types(self, con):

        self.get_bulkinsert_fieldlist(con)
        
        if self.column_types_list == None:
            
            self.column_types_list = {}
            
            for f in self.sql_fields:
                self.column_types_list[(con.ops.quote_name(f.column))] = type(f)
                
        return self.column_types_list  
    
    setattr(Class, "get_column_types", get_column_types)
    
    def get_column_names_list(self, con):

        self.get_bulkinsert_fieldlist(con)
        
        if self.column_names_list == None:
            
            self.column_names_list = []
            for f in self.sql_fields:
                self.column_names_list.append(con.ops.quote_name(f.column))
                
        return self.column_names_list    
    
    setattr(Class, "get_column_names_list", get_column_names_list)  




    """==========
         Construct a list of values
       =========="""
    def get_bulkinsert_parameters_old(self, con):
        
        self.get_bulkinsert_fieldlist(con)
        
        vals = tuple(  f.get_db_prep_save( f.pre_save(self, True), connection=con ) for f in self.sql_fields)
        
        #str_parameters
        
        return vals
    
    setattr(Class, "get_bulkinsert_parameters_old", get_bulkinsert_parameters_old)
    
    

    def get_bulkinsert_binaryvalues(self, con):
        
        self.get_bulkinsert_fieldlist(con)
        
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
        
        
        for f in self.sql_fields:
            
            
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
                vals.append(pack('!i', -1))
                print '-'*60
                traceback.print_exc(file=sys.stdout)
                print '-'*60          
        retvals = np.array(vals)
        
        return retvals
    
    setattr(Class, "get_bulkinsert_binaryvalues", get_bulkinsert_binaryvalues)
    
    
    def get_bulkinsert_valuelist(self, con):
        
        self.get_bulkinsert_fieldlist(con)
        
        vals = []
        
        null_val = "\\N"
        
        for f in self.sql_fields:
            """
            if f.name in self.quotables:
                val = "'{0}'".format(f.get_db_prep_save(f.pre_save(self, True), connection=con ))
            else:
                val = f.get_db_prep_save( f.pre_save(self, True), connection=con )
                
            """
            try:           
                if isinstance( f, models.AutoField):
                    pass
                else:
                    if isinstance( f, models.CharField) or isinstance( f, models.DateField):
                        #print "char, Date"
                        
                        val = "{0}".format(f.get_db_prep_save(f.pre_save(self, True), connection=con ))
                        
                        if val in ("'None'", "None"):
                            val=null_val
                            
                    elif isinstance( f, models.FloatField): 
                        
                        val = str(float(f.get_db_prep_save( f.pre_save(self, True), connection=con )))
                      
                    elif isinstance( f, models.IntegerField): 
                        
                        val = str(int(f.get_db_prep_save( f.pre_save(self, True), connection=con )))

                    elif isinstance( f, models.ForeignKey): 
                        
                        val = str(int(f.get_db_prep_save( f.pre_save(self, True), connection=con )))
                            
                            
                    else:
                        #print "Not Char, Date"
                        val = f.get_db_prep_save( f.pre_save(self, True), connection=con )
                
                    if val == None:
                        #print "Null Value"
                        val = null_val                 
                        
                    vals.append( val  )
            except:
                vals.append(null_val)
        
        #tup = tuple(vals)
        retvals = np.array(vals)
        
        return retvals
    
    setattr(Class, "get_bulkinsert_valuelist", get_bulkinsert_valuelist)    
    
    

    def get_bulkinsert_sqltemplate(self, con):

        self.get_bulkinsert_fieldlist(con)

        if self.sql_template == None:
            table = self._meta.db_table
            #column_names = ",".join(con.ops.quote_name(f.column) for f in self.sql_fields)
            placeholders = ",".join(("%s",) * len(self.sql_fields))
            
            self.sql_template = "insert into %s (%s) values (%s)" % (table, self.column_names, placeholders)

        return self.sql_template
    
    setattr(Class, "get_bulkinsert_sqltemplate", get_bulkinsert_sqltemplate)
    
    
    def get_bulkinsert_sqlvalues(self, con):

        self.get_bulkinsert_fieldlist(con)

        if self.sql_template_values == None:
            placeholders = ",".join(("%s",) * len(self.sql_fields))
            
            self.sql_template_values = " (%s) " % (placeholders)

        return self.sql_template_values
    
    setattr(Class, "get_bulkinsert_sqlvalues", get_bulkinsert_sqlvalues)
    
    
    
    
    
    def get_bulkinsert_sqlheader(self, con):

        self.get_bulkinsert_fieldlist(con)

        if self.sql_template_header == None:
            table = self._meta.db_table
            column_names = ",".join(con.ops.quote_name(f.column) for f in self.sql_fields)
            
            self.sql_template_header = "insert into %s (%s) VALUES" % (table, column_names)

        return self.sql_template_header 
    
    setattr(Class, "get_bulkinsert_sqlheader", get_bulkinsert_sqlheader)
    

    return Class  
    
         