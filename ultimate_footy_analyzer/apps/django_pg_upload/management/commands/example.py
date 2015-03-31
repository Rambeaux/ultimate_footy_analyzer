#django
from django.core.management.base import BaseCommand
from django.db import connection, connections, transaction

#python
import random
from optparse import make_option
from datetime import date, timedelta

#app
from ultimate_footy_analyzer.apps.django_pg_upload.models import  *


class Command(BaseCommand):

    args = '<action>'
    help = 'Demonstrate Bulk Upload'

    option_list = BaseCommand.option_list + (
                                             
        make_option('-a', '--action',               action='store', dest='action',            type='string', help='Perform stated action.'),
        make_option('-n', '--numinsert',            action='store', dest='numinsert',         type='int', help='Perform stated action.'),
        make_option( '--verbose',                    action="store_true", dest='verbose',        help='Perform stated action.'),
        
     )

    referenceOneCache = {}
    referenceTwoCache = {}


    def handle(self, action=None, numinsert=100, verbose=False, **options):


        con = connections['default']    

        self.initialise()
        
        #controls whether to print messages
        self.verbose = verbose
        
        self.onestep_start = None
        self.onestep_end = None
        
        self.djbulk_start = None
        self.djbulk_end = None
        
        
        """======================
            Time windows
        """
        #action to demonstrate one-step upload
        if action == "one_step":

            onestep_data = []       
            
            # generate one-step data
            if self.verbose:
                print("generating {0} one-step upload objects".format( numinsert ))
            i=1
            while i<= numinsert:
                onestep_data.append(self.generate_onestep())
                i+=1
            
            # upload one-step data
            objOneStep = OneStepLoad()
            
            #bulkload Onestep objects
            if self.verbose:
                print("performing one-step bulk upload")
            
            self.onestep_start = datetime.now()
            numinserted = objOneStep.bulk_insert_textstream( onestep_data, con)
            self.onestep_end = datetime.now()

            if self.verbose:
                print("Using text-streaming, Uploaded {0} one-step objects in {1}".format(numinserted, str(self.onestep_end-self.onestep_start) ))


            """Compare with bulkload"""
            self.djbulk_start = datetime.now()
            OneStepLoad.objects.bulk_create(onestep_data, 50)
            self.djbulk_end = datetime.now()
            if self.verbose:
                print("Using Django bulk-load, Uploaded {0} one-step objects in {1}".format(numinserted, (self.djbulk_end-self.djbulk_start) ))


        #action to demonstrate two-step upload and upsert
        if action == "two_step":

            twostep_data = []            

            # generate two-step data
            if self.verbose:
                print("generating {0} two-step upload objects".format( numinsert ))
            
            i=1
            while i<= numinsert:
                twostep_data.append(self.generate_twostep(i))
                i+=1
            
            # upload one-step data
            objTwoStep = TwoStepLoad_stg()
            
            # clear staging table
            if self.verbose:
                print("clearing staging table")
            
            objTwoStep.clear_staging_table(con)
            
            # upload two-step data to staging
            if self.verbose:
                print("performing two-step bulk upload to staging")
            numinserted = objTwoStep.bulk_insert_textstream( twostep_data, con)            
            
            if self.verbose:
                print("Uploaded {0} two-step objects".format(numinserted))            
            
            # 'upsert' staging data to production
            if self.verbose:
                print("performing 'upsert' from staging to production ")
            objTwoStep.execute_upsert(con, print_query=True)

        #close the connection
        con.close()




    def initialise(self):
        
        #Ref 1 objects: X, Y, Z
        self.referenceOneCache[0], found = ReferenceOne.objects.get_or_create(reference_one_name="Ref1-A")
        self.referenceOneCache[1], found = ReferenceOne.objects.get_or_create(reference_one_name="Ref1-B")
        self.referenceOneCache[2], found = ReferenceOne.objects.get_or_create(reference_one_name="Ref1-C")

        #Ref 2 objects: X, Y, Z
        self.referenceTwoCache[0], found = ReferenceTwo.objects.get_or_create(reference_two_name="Ref1-X")
        self.referenceTwoCache[1], found = ReferenceTwo.objects.get_or_create(reference_two_name="Ref1-Y")
        self.referenceTwoCache[2], found = ReferenceTwo.objects.get_or_create(reference_two_name="Ref1-Z")




       
    def generate_onestep(self):
    
    
        rand1 = random.random()
        rand2 = random.random()
        
        objRef1 = None
        objRef2 = None
        
        if rand1 > 0 and rand1 <= .2:
            objRef1 = self.referenceOneCache[0]
        elif rand1 > .2 and rand1 <= .7:
            objRef1 = self.referenceOneCache[1]
        else:
            objRef1 = self.referenceOneCache[2]

        if rand2 > 0 and rand2 <= .2:
            objRef2 = self.referenceTwoCache[0]
        elif rand2 > .2 and rand2 <= .7:
            objRef2 = self.referenceTwoCache[1]
        else:
            objRef2 = self.referenceTwoCache[2]

        randInt = int(100* random.gauss(0, 1))
        randFloat = random.gauss(0, 1)
        
        year=random.randint(1990,2000)
        month=random.randint(1,12)
        day=random.randint(1,28)
        #datestr = year+"-"+month+"-"+day
        randDate = date(year, month, day)
        
        string = "The quick brown fox jumps over the lazy dog"
        randString = string[random.randint(1,len(string)) : random.randint(1,len(string))]
        
        objOnestep = OneStepLoad(reference_one = objRef1,
                                 reference_two = objRef2,
                                 data_int=randInt,
                                 data_float=randFloat,
                                 data_string=randString,
                                 data_date=randDate
                                 )

        return objOnestep
    
    
       
    def generate_twostep(self, i):
    
    
        rand1 = random.random()
        rand2 = random.random()
        
        objRef1 = None
        objRef2 = None
        
        if rand1 > 0 and rand1 <= .2:
            objRef1 = self.referenceOneCache[0]
        elif rand1 > .2 and rand1 <= .7:
            objRef1 = self.referenceOneCache[1]
        else:
            objRef1 = self.referenceOneCache[2]

        if rand2 > 0 and rand2 <= .2:
            objRef2 = self.referenceTwoCache[0]
        elif rand2 > .2 and rand2 <= .7:
            objRef2 = self.referenceTwoCache[1]
        else:
            objRef2 = self.referenceTwoCache[2]

        randInt = int(100* random.gauss(0,1))
        randFloat = random.gauss(0,1)
        
        baseDate = date(year = 1970, month=1, day=1)
        dateDiff = timedelta(days=int(i/2))
        uniquedate = baseDate + dateDiff
        
        
        
        string = "The quick brown fox jumps over the lazy dog"
        randString = string[random.randint(1,len(string)) : random.randint(1,len(string))]

        #Create a staging object        
        objTwostep = TwoStepLoad_stg(reference_one = objRef1,
                                     reference_two = objRef2,
                                     reference_date = uniquedate,
                                     
                                     data_int=randInt,
                                     data_float=randFloat,
                                     data_string=randString,
                                 )

        return objTwostep    
       
       