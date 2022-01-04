# This script will merge the 20 csv files for each type into a single csv per type
# It uses python 2.7
#

import os, dotenv
dotenv.load_dotenv(".env")
BASE_OUTPUT_DIRECTORY           = os.environ['BASE_OUTPUT_DIRECTORY']


files = {
   'care_site.csv'               : 'care_site_',
   'condition_occurrence.csv'    : 'condition_occurrence_',
   'death.csv'                   : 'death_',
   'device_cost.csv'             : 'device_cost_',
   'device_exposure.csv'         : 'device_exposure_',
   'drug_cost.csv'               : 'drug_cost_',
   'drug_exposure.csv'           : 'drug_exposure_',
   'location.csv'                : 'location_',
   'measurement_occurrence.csv'  : 'measurement_occurrence_',
   'observation.csv'             : 'observation_',
   'observation_period.csv'      : 'observation_period_',
   'payer_plan_period.csv'       : 'payer_plan_period_',
   'person.csv'                  : 'person_',
   'procedure_cost.csv'          : 'procedure_cost_',
   'procedure_occurrence.csv'    : 'procedure_occurrence_',
   'provider.csv'                : 'provider_',
   'specimen.csv'                : 'specimen_',
   'visit_cost.csv'              : 'visit_cost_',
   'visit_occurrence.csv'        : 'visit_occurrence_'
}
   
for key, value in files.iteritems():
    print "Processing: " + key
    fout=open(os.path.join(BASE_OUTPUT_DIRECTORY, key),"w")
    # first file:
    fstring = value + "1.csv"
    for line in open(os.path.join(BASE_OUTPUT_DIRECTORY, fstring)):
        fout.write(line)
    # now the rest:    
    for num in range(2,20):
        f = open(os.path.join(BASE_OUTPUT_DIRECTORY, value+str(num)+".csv"))
        f.next() # skip the header
        for line in f:
            fout.write(line)
        f.close() # not really needed
    fout.close()
