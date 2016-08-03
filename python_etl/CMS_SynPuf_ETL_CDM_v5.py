import csv,os,os.path,sys
from time import strftime
from collections import OrderedDict
import argparse
import dotenv
import math
from constants import OMOP_CONSTANTS, OMOP_MAPPING_RECORD, BENEFICIARY_SUMMARY_RECORD, OMOP_CONCEPT_RECORD, OMOP_CONCEPT_RELATIONSHIP_RECORD
from utility_classes import Table_ID_Values
from beneficiary import Beneficiary
from FileControl import FileControl
from SynPufFiles import PrescriptionDrug, InpatientClaim, OutpatientClaim, CarrierClaim
from datetime import date
import calendar
# ------------------------
# TODO: polish for updating to OHDSI (doc strings, testing, comments, pylint, etc)
#
# ------------------------

# ------------------------
# This python script creates the OMOP CDM v5 tables from the CMS SynPuf (Synthetic Public Use Files).
# ------------------------
#
#  Input Required:
#       OMOP Vocabulary v5 Concept  file. Remember to run: java -jar cpt4.jar (appends CPT4 concepts from concept_cpt4.csv to CONCEPT.csv)
#           BASE_OMOP_INPUT_DIRECTORY    /  CONCEPT.csv
#                                        /  CONCEPT_RELATIONSHIP.csv
#
#
#       SynPuf data files
#           BASE_SYNPUF_INPUT_DIRECTORY
#                             /  DE1_0_2008_Beneficiary_Summary_File_Sample_<sample_number>.csv
#                             /  DE1_0_2009_Beneficiary_Summary_File_Sample_<sample_number>.csv
#                             /  DE1_0_2010_Beneficiary_Summary_File_Sample_<sample_number>.csv
#                             /  DE1_0_2008_to_2010_Carrier_Claims_Sample_<sample_number>_A.csv
#                             /  DE1_0_2008_to_2010_Carrier_Claims_Sample_<sample_number>_B.csv
#                             /  DE1_0_2008_to_2010_Inpatient_Claims_Sample_<sample_number>_B.csv
#                             /  DE1_0_2008_to_2010_Outpatient_Claims_Sample_<sample_number>_B.csv
#                             /  DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_<sample_number>_B.csv
#
#  Output Produced:
#     Last-used concept_IDs for CDM v5 tables
#           BASE_OUTPUT_DIRECTORY       /  etl_synpuf_last_table_ids.txt
#                                       /  npi_provider_id.txt
#                                       /  provider_id_care_site.txt
#                                       /  location_dictionary.csv
#
#     SynPuf Beneficiary Files with year prefix
#           BASE_SYNPUF_INPUT_DIRECTORY
#                                /  DE1_0_comb_Beneficiary_Summary_File_Sample_<sample_number>.csv
#                                /  DE1_0_comb_Beneficiary_Summary_File_Sample_<sample_number>.csv.srt
#                                /  DE1_0_2008_to_2010_Carrier_Claims_Sample_<sample_number>.csv.srt
#                                /  DE1_0_2008_to_2010_Inpatient_Claims_Sample_<sample_number>.csv.srt
#                                /  DE1_0_2008_to_2010_Outpatient_Claims_Sample_<sample_number>.csv.srt
#                                /  DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_<sample_number>.csv.srt
#
#
#     OMOP CDM v5 Tables
#           BASE_OUTPUT_DIRECTORY       /  care_site_<sample_number>.csv
#                                       /  condition_occurrence_<sample_number>.csv
#                                       /  death_<sample_number>.csv
#                                       /  device_cost_<sample_number>.csv
#                                       /  device_exposure_<sample_number>.csv
#                                       /  drug_cost_<sample_number>.csv
#                                       /  drug_exposure_<sample_number>.csv
#                                       /  location_<sample_number>.csv
#                                       /  measurement_occurrence_<sample_number>.csv
#                                       /  observation_<sample_number>.csv
#                                       /  observation_period_<sample_number>.csv
#                                       /  payer_plan_period_<sample_number>.csv
#                                       /  person_<sample_number>.csv
#                                       /  procedure_cost_<sample_number>.csv
#                                       /  procedure_occurrence_<sample_number>.csv
#                                       /  provider_<sample_number>.csv
#                                       /  specimen_<sample_number>.csv
#                                       /  visit_cost_<sample_number>.csv
#                                       /  visit_occurrence_<sample_number>.csv
#
#
#                                       ** Various debug and log files
#
# ------------------------

# ------------------------
#  2015-02-05  C. Dougherty         Created
#
#  2016-06-17  Christophe Lambert, Praveen Kumar, Amritansh -- University of New Mexico -- Major overhaul
# ------------------------

dotenv.load_dotenv(".env")

# -----------------------------------
# - Configuration
# -----------------------------------
# ---------------------------------

# Edit your .env file to change which directories to use in the ETL process

# Path to the directory where control files should be saved (input/output
BASE_ETL_CONTROL_DIRECTORY      = os.environ['BASE_ETL_CONTROL_DIRECTORY']

# Path to the directory containing the downloaded SynPUF files
BASE_SYNPUF_INPUT_DIRECTORY     = os.environ['BASE_SYNPUF_INPUT_DIRECTORY']

# Path to the directory containing the OMOP Vocabulary v5 files (can be downloaded from http://www.ohdsi.org/web/athena/)
BASE_OMOP_INPUT_DIRECTORY       = os.environ['BASE_OMOP_INPUT_DIRECTORY']

# Path to the directory where CDM-compatible CSV files should be saved
BASE_OUTPUT_DIRECTORY           = os.environ['BASE_OUTPUT_DIRECTORY']

# SynPUF dir format.  I've seen DE1_{0} and DE_{0} as different prefixes for the name of the directory containing a slice of SynPUF data
SYNPUF_DIR_FORMAT               = os.environ['SYNPUF_DIR_FORMAT']

DESTINATION_FILE_DRUG               = 'drug'
DESTINATION_FILE_CONDITION          = 'condition'
DESTINATION_FILE_PROCEDURE          = 'procedure'
DESTINATION_FILE_OBSERVATION        = 'observation'
DESTINATION_FILE_MEASUREMENT        = 'measurement'
DESTINATION_FILE_DEVICE             = 'device'
DESTINATION_FILE_VISIT              = 'visit'

class SourceCodeConcept(object):
    def __init__(self, source_concept_code, source_concept_id, target_concept_id, destination_file):
        self.source_concept_code = source_concept_code
        self.source_concept_id = source_concept_id
        self.target_concept_id = target_concept_id
        self.destination_file = destination_file

# -----------------------------------
# Globals
# -----------------------------------
file_control = None
table_ids = None

source_code_concept_dict = {}   # stores source and target concept ids + destination file
concept_relationship_dict = {}  # stores the source concept id and its mapped target concept id

person_location_dict = {}   # stores location_id for a given state + county
current_stats_filename = ''

#This was used to detect death via ICD9 codes, but since death information is
#listed in the beneficiary file, we will not use. Plus this isn't even a complete list
#icd9_codes_death = ['761.6', '798', '798.0', '798.1', '798.2','798.9', '799.9', 'E913.0','E913.1','E913.2','E913.3','E913.8','E913.9', 'E978']

provider_id_care_site_id = {} # sotres care site id for a provider_num(institution)
visit_id_list = set()   # stores unique visit ids written to visit occurrence file
visit_occurrence_ids = OrderedDict()   # stores visit ids generated by determine_visits function
npi_provider_id = {}    # stores provider id for an npi

#-------------------------------------------------------------------------------
# SSA codes for Puerto Rico('40') and Virgin Islands ('48') have not been added
# to the following dictionary. SSA code '54' is for others where others=
# PUERTO RICO, VIRGIN ISLANDS, AFRICA, ASIA OR CALIFORNIA; INSTITUTIONAL PROVIDER
# OF SERVICES (IPS) ONLY, CANADA & ISLANDS, CENTRAL AMERICA AND WEST INDIES,
# EUROPE, MEXICO, OCEANIA, PHILIPPINES, SOUTH AMERICA, U.S. POSSESSIONS, AMERICAN
# SAMOA, GUAM, SAIPAN OR NORTHERN MARIANAS, TEXAS; INSTITUTIONAL PROVIDER OF SERVICES
# (IPS) ONLY, NORTHERN MARIANAS, GUAM, UNKNOWN.
#-------------------------------------------------------------------------------
SSA_state_codes = {
'01':'AL',
'02':'AK',
'03':'AZ',
'04':'AR',
'05':'CA',
'06':'CO',
'07':'CT',
'08':'DE',
'09':'DC',
'10':'FL',
'11':'GA',
'12':'HI',
'13':'ID',
'14':'IL',
'15':'IN',
'16':'IA',
'17':'KS',
'18':'KY',
'19':'LA',
'20':'ME',
'21':'MD',
'22':'MA',
'23':'MI',
'24':'MN',
'25':'MS',
'26':'MO',
'27':'MT',
'28':'NE',
'29':'NV',
'30':'NH',
'31':'NJ',
'32':'NM',
'33':'NY',
'34':'NC',
'35':'ND',
'36':'OH',
'37':'OK',
'38':'OR',
'39':'PA',
'41':'RI',
'42':'SC',
'43':'SD',
'44':'TN',
'45':'TX',
'46':'UT',
'47':'VT',
'49':'VA',
'50':'WA',
'51':'WV',
'52':'WI',
'53':'WY',
'54':'54'}

domain_destination_file_list = {
    'Condition'             : DESTINATION_FILE_CONDITION,
    'Condition/Meas'        : DESTINATION_FILE_MEASUREMENT,
    'Condition/Obs'         : DESTINATION_FILE_OBSERVATION,
    'Condition/Procedure'   : DESTINATION_FILE_PROCEDURE,
    'Device'                : DESTINATION_FILE_DEVICE,
    'Device/Obs'            : DESTINATION_FILE_OBSERVATION,
    'Device/Procedure'      : DESTINATION_FILE_PROCEDURE,
    'Drug'                  : DESTINATION_FILE_DRUG,
    'Measurement'           : DESTINATION_FILE_MEASUREMENT,
    'Meas/Procedure'        : DESTINATION_FILE_PROCEDURE,
    'Obs/Procedure'         : DESTINATION_FILE_PROCEDURE,
    'Observation'           : DESTINATION_FILE_OBSERVATION,
    'Procedure'             : DESTINATION_FILE_PROCEDURE,
    'Visit'                 : DESTINATION_FILE_VISIT
    }

# -----------------------------------
# get timestamp
# -----------------------------------
def get_timestamp():
    return strftime("%Y-%m-%d %H:%M:%S")

# -----------------------------------
# TODO: use standard python logger...
# -----------------------------------
def log_stats(msg):
    print msg
    global current_stats_filename
    with open(current_stats_filename,'a') as fout:
        fout.write('[{0}]{1}\n'.format(get_timestamp(),msg))

# -----------------------------------
# format date in YYYYMMDD
# -----------------------------------
def get_date_YYYY_MM_DD(date_YYYYMMDD):
    if len(date_YYYYMMDD) == 0:
        return ''
    return '{0}-{1}-{2}'.format(date_YYYYMMDD[0:4], date_YYYYMMDD[4:6], date_YYYYMMDD[6:8])

# -----------------------------------------------------------------------------------------------------
# Each provider_num (institution) has a unique care_site_id. It is generated by the following code by
# adding 1 to previous care_site_id.
# -------------------------------------------------------------------------------------------------------
def get_CareSite(provider_num):
    global table_ids
    if provider_num not in provider_id_care_site_id:
        provider_id_care_site_id[provider_num] = [table_ids.last_care_site_id,0]
        table_ids.last_care_site_id += 1
    return provider_id_care_site_id[provider_num][0]

# -------------------------------------------------------------------------
# A unique provider_id for each npi is generated by adding 1 to the previous provider_id
# --------------------------------------------------------------------------
def get_Provider(npi):
    global table_ids
    if npi not in npi_provider_id:
        npi_provider_id[npi] = [table_ids.last_provider_id,0]
        table_ids.last_provider_id += 1
    return npi_provider_id[npi][0]

# --------------------------------------------------------------------------------------------------
# A unique location id for each unique combination of state+county is generated by adding 1 to
# the previous location id
# ------------------------------------------------------------------------------------------------
def get_location_id(state_county):
    global table_ids
    if state_county not in person_location_dict:
        person_location_dict[state_county] = [table_ids.last_location_id,0]
        table_ids.last_location_id += 1
    return person_location_dict[state_county][0]


# -----------------------------------
# This function produces dictionaries that give mappings between SynPUF codes and OMOP concept_ids
# -----------------------------------
def build_maps():
    log_stats('-'*80)
    log_stats('build_maps starting...')

    #--------------------------------------------------------------------------------------
    # load existing person_location_dict. v5
    # It populates the dictionary with the existing data so that the subsequent run of this
    # program doesn't generate the duplicate location_id.
    #--------------------------------------------------------------------------------------
    recs_in = 0
    global table_ids
    global person_location_dict

    location_dict_file = os.path.join(BASE_ETL_CONTROL_DIRECTORY,"location_dictionary.txt")
    if os.path.exists(location_dict_file):
        log_stats('reading existing location_dict_file ->' + location_dict_file)
        with open(location_dict_file,'r') as fin:
            for rec in fin:
                recs_in += 1
                flds = (rec[:-1]).split('\t')
                if len(flds) == 2:
                    state_county = flds[0]
                    location_id = flds[1]
                    location_id = location_id.lstrip('[').rstrip(']').split(',')   #convert string to list as the file data is string
                    location_id = [int(location_id[0]), int(location_id[1])]     # convert the data in the list to integer
                    person_location_dict[state_county] = location_id
        log_stats('done, recs_in={0}, len person_location_dict={1}'.format(recs_in, len(person_location_dict)))
    else:
        log_stats('No existing location_dict_file found (looked for ->' + location_dict_file + ')')


    #----------------
    # load existing provider_id_care_site_id.
    # It populates the dictionary with the existing data so that the subsequent run of this
    # program doesn't generate the duplicate care_site_id.
    #----------------
    recs_in = 0
    global table_ids
    global provider_id_care_site_id

    provider_id_care_site_file = os.path.join(BASE_ETL_CONTROL_DIRECTORY,'provider_id_care_site.txt')
    if os.path.exists(provider_id_care_site_file):
        log_stats('reading existing provider_id_care_site_file ->' + provider_id_care_site_file)
        with open(provider_id_care_site_file,'r') as fin:
            for rec in fin:
                recs_in += 1
                flds = (rec[:-1]).split('\t')
                if len(flds) == 2:
                    provider_num = flds[0]
                    care_site_id = flds[1]
                    care_site_id = care_site_id.lstrip('[').rstrip(']').split(',')   #convert string to list as the file data is string
                    care_site_id = [int(care_site_id[0]), int(care_site_id[1])]     # convert the data in the list to integer
                    provider_id_care_site_id[provider_num] = care_site_id
        log_stats('done, recs_in={0}, len provider_id_care_site_id={1}'.format(recs_in, len(provider_id_care_site_id)))
    else:
        log_stats('No existing provider_id_care_site_file found (looked for ->' + provider_id_care_site_file + ')')

    #----------------
    # load existing npi_provider_id
    # It populates the dictionary with the existing data so that the subsequent run of this
    # program doesn't generate the duplicate provider_id.
    #----------------
    recs_in = 0
    global npi_provider_id

    npi_provider_id_file = os.path.join(BASE_ETL_CONTROL_DIRECTORY,'npi_provider_id.txt')
    if os.path.exists(npi_provider_id_file):
        log_stats('reading existing npi_provider_id_file ->' + npi_provider_id_file)
        with open(npi_provider_id_file,'r') as fin:
            for rec in fin:
                recs_in += 1
                flds = (rec[:-1]).split('\t')
                if len(flds) == 2:
                    npi = flds[0]
                    provider_id = flds[1]
                    provider_id = provider_id.lstrip('[').rstrip(']').split(',')   #convert string to list as the file data is string
                    provider_id = [int(provider_id[0]), int(provider_id[1])]       # convert the data in the list to integer
                    npi_provider_id[npi] = provider_id
        log_stats('done, recs_in={0}, len npi_provider_id={1}'.format(recs_in, len(npi_provider_id_file)))
    else:
        log_stats('No existing npi_provider_id_file found (looked for ->' + npi_provider_id_file + ')')


    #----------------
    # Load the OMOP v5 Concept file to build the source code to conceptID xref.
    # NOTE: This version of the flat file had embedded newlines. This code handles merging the split
    #       records. This may not be needed when the final OMOP v5 Concept file is produced.
    #----------------
    omop_concept_relationship_debug_file = os.path.join(BASE_OUTPUT_DIRECTORY,'concept_relationship_debug_log.txt')
    omop_concept_relationship_file = os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT_RELATIONSHIP.csv')
    omop_concept_debug_file = os.path.join(BASE_OUTPUT_DIRECTORY,'concept_debug_log.txt')
    omop_concept_file = os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT.csv')

    recs_in = 0
    recs_skipped = 0

    log_stats('Reading omop_concept_relationship_file   -> ' + omop_concept_relationship_file)
    log_stats('Writing to log file              -> ' + omop_concept_relationship_debug_file)

    with open(omop_concept_relationship_file,'r') as fin, \
         open(omop_concept_relationship_debug_file, 'w') as fout_log:
        fin.readline() #skip header
        for rec in fin:
            recs_in += 1
            if recs_in % 100000 == 0: print 'omop concept relationship recs=',recs_in
            flds = (rec[:-1]).split('\t')
            if len(flds) == OMOP_CONCEPT_RELATIONSHIP_RECORD.fieldCount:
                concept_id1 = flds[OMOP_CONCEPT_RELATIONSHIP_RECORD.CONCEPT_ID_1]
                concept_id2 = flds[OMOP_CONCEPT_RELATIONSHIP_RECORD.CONCEPT_ID_2]
                relationship_id = flds[OMOP_CONCEPT_RELATIONSHIP_RECORD.RELATIONSHIP_ID]
                invalid_reason = flds[OMOP_CONCEPT_RELATIONSHIP_RECORD.INVALID_REASON]

                if concept_id1 != '' and concept_id2 != '' and relationship_id == "Maps to" and invalid_reason == '':
                    if concept_relationship_dict.has_key(concept_id1):         # one concept id might have several mapping, so values are stored as list
                        concept_relationship_dict[concept_id1].append(concept_id2)
                    else:
                        concept_relationship_dict[concept_id1] = [concept_id2]
                else:
                    recs_skipped = recs_skipped + 1

        log_stats('Done, omop concept recs_in            = ' + str(recs_in))
        log_stats('recs_skipped                          = ' + str(recs_skipped))
        log_stats('len source_code_concept_dict           = ' + str(len(source_code_concept_dict)))

    recs_in = 0
    recs_skipped = 0
    merged_recs=0
    recs_checked=0

    #TODO: there is an overlap of 41 2-character codes that are the same between CPT4 and HCPCS,
    #but map to different OMOP concepts. Need to determine which should prevail. Whichever prevails should call one of the next 2 code blocks first.

    log_stats('Reading omop_concept_file        -> ' + omop_concept_file)
    log_stats('Writing to log file              -> ' + omop_concept_debug_file)

    #First pass to obtain domain ids of concepts
    domain_dict = {}
    with open(omop_concept_file,'r') as fin:
        fin.readline()
        for rec in fin:
            flds = (rec[:-1]).split('\t')
            if len(flds) == OMOP_CONCEPT_RECORD.fieldCount:
                concept_id = flds[OMOP_CONCEPT_RECORD.CONCEPT_ID]
                domain_id = flds[OMOP_CONCEPT_RECORD.DOMAIN_ID]
                domain_dict[concept_id] = domain_id
    print "loaded domain dict with this many records: ", len(domain_dict)

    with open(omop_concept_file,'r') as fin, \
         open(omop_concept_debug_file, 'w') as fout_log:
         # open(omop_concept_file_mini, 'w') as fout_mini:
        fin.readline() #skip header
        for rec in fin:
            recs_in += 1
            if recs_in % 100000 == 0: print 'omop concept recs=',recs_in
            flds = (rec[:-1]).split('\t')

            if len(flds) == OMOP_CONCEPT_RECORD.fieldCount:
                concept_id = flds[OMOP_CONCEPT_RECORD.CONCEPT_ID]
                concept_code = original_concept_code = flds[OMOP_CONCEPT_RECORD.CONCEPT_CODE].replace(".","")
                vocabulary_id = flds[OMOP_CONCEPT_RECORD.VOCABULARY_ID]
                if vocabulary_id ==  OMOP_CONSTANTS.CPT4_VOCABULARY_ID:
                    vocabulary_id = OMOP_CONSTANTS.HCPCS_VOCABULARY_ID
                if(vocabulary_id in [OMOP_CONSTANTS.ICD_9_DIAGNOSIS_VOCAB_ID,OMOP_CONSTANTS.ICD_9_PROCEDURES_VOCAB_ID]):
                    vocabulary_id = OMOP_CONSTANTS.ICD_9_VOCAB_ID

                domain_id = flds[OMOP_CONCEPT_RECORD.DOMAIN_ID]
                invalid_reason = flds[OMOP_CONCEPT_RECORD.INVALID_REASON]

                status = ''
                if concept_id != '':
                    if vocabulary_id in [OMOP_CONSTANTS.ICD_9_VOCAB_ID,
                                         OMOP_CONSTANTS.HCPCS_VOCABULARY_ID,
                                         OMOP_CONSTANTS.NDC_VOCABULARY_ID]:
                        recs_checked += 1

                        if  not concept_relationship_dict.has_key(concept_id):
                            destination_file = domain_destination_file_list[domain_id]
                            if( vocabulary_id == OMOP_CONSTANTS.ICD_9_VOCAB_ID):
                                status = "No map from ICD9 code, or code invalid for " + concept_id
                                recs_skipped += 1
                            if( vocabulary_id == OMOP_CONSTANTS.HCPCS_VOCABULARY_ID):
                                status = "No self map from OMOP (HCPCS/CPT4) to OMOP (HCPCS/CPT4) or code invalid for " + concept_id
                                recs_skipped += 1
                            if( vocabulary_id == OMOP_CONSTANTS.NDC_VOCABULARY_ID):
                                status = "No map from OMOP (NCD) to OMOP (RxNorm) or code invalid for " + concept_id
                                recs_skipped += 1
                            source_code_concept_dict[vocabulary_id,concept_code] = [SourceCodeConcept(concept_code, concept_id, "0", destination_file)]
                        else:
                            source_code_concept_dict[vocabulary_id,concept_code] = []
                            for concept in concept_relationship_dict[concept_id]:
                                destination_file = domain_destination_file_list[domain_dict[concept]]
                                source_code_concept_dict[vocabulary_id,concept_code].append(SourceCodeConcept(concept_code, concept_id, concept, destination_file))

                if status != '':
                    fout_log.write(status + ': \t')
                    # for fld in line: fout_log.write(fld + '\t')
                    fout_log.write(rec + '\n')

        log_stats('Done, omop concept recs_in            = ' + str(recs_in))
        log_stats('recs_checked                          = ' + str(recs_checked))
        log_stats('recs_skipped                          = ' + str(recs_skipped))
        log_stats('merged_recs                           = ' + str(merged_recs))
        log_stats('len source_code_concept_dict           = ' + str(len(source_code_concept_dict)))

        #---------------------------


# -----------------------------------
# write the provider_num(institution) + care_site_id to provider_id_care_site.txt file.
# write the npi + provider_id to npi_provider_id.txt file.
# the data from these two files are loaded to dictionaries before processing the input
# records to make sure that the duplicate records are not written to care_site and provider files.
# -----------------------------------
def persist_lookup_tables():
    recs_out = 0
    location_dict_file = os.path.join(BASE_ETL_CONTROL_DIRECTORY,'location_dictionary.txt')
    log_stats('writing  location_dict_file ->' + location_dict_file)
    with open(location_dict_file,'w') as fout:
        for state_county, location_id in person_location_dict.items():
            fout.write('{0}\t{1}\n'.format(state_county, location_id))
            recs_out += 1
    log_stats('done, recs_out={0}, len person_location_dict={1}'.format(recs_out, len(person_location_dict)))

    recs_out = 0
    provider_id_care_site_file = os.path.join(BASE_ETL_CONTROL_DIRECTORY,'provider_id_care_site.txt')
    log_stats('writing  provider_id_care_site_file ->' + provider_id_care_site_file)
    with open(provider_id_care_site_file,'w') as fout:
        for provider_num, care_site_id in provider_id_care_site_id.items():
            fout.write('{0}\t{1}\n'.format(provider_num, care_site_id))
            recs_out += 1
    log_stats('done, recs_out={0}, len provider_id_care_site_id={1}'.format(recs_out, len(provider_id_care_site_id)))

    recs_out = 0
    npi_provider_id_file = os.path.join(BASE_ETL_CONTROL_DIRECTORY,'npi_provider_id.txt')
    log_stats('writing  npi_provider_id_file ->' + npi_provider_id_file)
    with open(npi_provider_id_file,'w') as fout:
        for npi, provider_id in npi_provider_id.items():
            fout.write('{0}\t{1}\n'.format(npi, provider_id))
            recs_out += 1
    log_stats('done, recs_out={0}, len npi_provider_id={1}'.format(recs_out, len(npi_provider_id)))

# ------------------------------------------------------------------------------------------------------------------------
# Logic to determine visits. visit_dates is used to determine the start and end date of observation period for a beneficiary.
# visit_occurrence_ids keeps track of unique visits.
# -------------------------------------------------------------------------------------------------------------------------
def determine_visits(bene):
    # each unique date gets a visit id

    visit_id = table_ids.last_visit_occurrence_id
    #For death records just track dates for purpose of observation_period
    yd = bene.LatestYearData()
    if yd is not None and yd.BENE_DEATH_DT != '':
        bene.visit_dates[yd.BENE_DEATH_DT] = visit_id

    #For prescription records just track dates for purpose of observation_period
    for raw_rec in bene.prescription_records:
        rec = PrescriptionDrug(raw_rec)
        if rec.SRVC_DT == '':
            continue
        bene.visit_dates[rec.SRVC_DT] = visit_id

    #For inpatient records, if same patient, same date range, and same provider institution number, is same visit
    for raw_rec in bene.inpatient_records:
        rec = InpatientClaim(raw_rec)
        if rec.CLM_FROM_DT == '':
            continue
        if not visit_occurrence_ids.has_key((rec.DESYNPUF_ID,rec.CLM_FROM_DT,rec.CLM_THRU_DT,rec.PRVDR_NUM)):
            bene.visit_dates[rec.CLM_FROM_DT] = visit_id
            bene.visit_dates[rec.CLM_THRU_DT] = visit_id
            visit_occurrence_ids[rec.DESYNPUF_ID,rec.CLM_FROM_DT,rec.CLM_THRU_DT,rec.PRVDR_NUM] = visit_id
            visit_id+=1

    #For outpatient records, if same patient, same date range, and same provider institution number, is same visit
    for raw_rec in bene.outpatient_records:
        rec = OutpatientClaim(raw_rec)
        if rec.CLM_FROM_DT == '':
            continue
        if not visit_occurrence_ids.has_key((rec.DESYNPUF_ID,rec.CLM_FROM_DT,rec.CLM_THRU_DT,rec.PRVDR_NUM)):
            bene.visit_dates[rec.CLM_FROM_DT] = visit_id
            bene.visit_dates[rec.CLM_THRU_DT] = visit_id
            visit_occurrence_ids[rec.DESYNPUF_ID,rec.CLM_FROM_DT,rec.CLM_THRU_DT,rec.PRVDR_NUM] = visit_id
            visit_id+=1

    #For carrier claims, if same patient, same date range, and same institution tax number, is same visit
    for raw_rec in bene.carrier_records:
        rec = CarrierClaim(raw_rec)
        if rec.CLM_FROM_DT == '':
            continue
        if not visit_occurrence_ids.has_key((rec.DESYNPUF_ID,rec.CLM_FROM_DT,rec.CLM_THRU_DT,rec.TAX_NUM)):
            bene.visit_dates[rec.CLM_FROM_DT] = visit_id
            bene.visit_dates[rec.CLM_THRU_DT] = visit_id
            visit_occurrence_ids[rec.DESYNPUF_ID,rec.CLM_FROM_DT,rec.CLM_THRU_DT,rec.TAX_NUM] = visit_id
            visit_id+=1

    table_ids.last_visit_occurrence_id = visit_id  #store the last_visit_occurrence_id

# -----------------------------------
# CDM v5 Person - Write person records
# -----------------------------------
def write_person_record(beneficiary):
    person_fd = file_control.get_Descriptor('person')
    yd = beneficiary.LatestYearData()
    if yd is None: return

    person_fd.write('{0},'.format(beneficiary.person_id))                                   # person_id
    if int(yd.BENE_SEX_IDENT_CD) == 1:                                                      # gender_concept_id
        person_fd.write('{0},'.format(OMOP_CONSTANTS.GENDER_MALE))
    elif int(yd.BENE_SEX_IDENT_CD) == 2:
        person_fd.write('{0},'.format(OMOP_CONSTANTS.GENDER_FEMALE))
    else:
        person_fd.write('0,')

    person_fd.write('{0},'.format(yd.BENE_BIRTH_DT[0:4]))                                    # year_of_birth
    person_fd.write('{0},'.format(yd.BENE_BIRTH_DT[4:6]))                                    # month_of_birth
    person_fd.write('{0},'.format(yd.BENE_BIRTH_DT[6:8]))                                    # day_of_birth
    person_fd.write(',')                                                                     # time_of_birth
    #print ("yd.BENE_RACE_CD: " + str(yd.BENE_RACE_CD))
    if int(yd.BENE_RACE_CD) == 1:    #White                                                      # race_concept_id and ethnicity_concept_id
        person_fd.write('{0},'.format(OMOP_CONSTANTS.RACE_WHITE))
        person_fd.write('{0},'.format(OMOP_CONSTANTS.ETHNICITY_NON_HISPANIC))
    elif int(yd.BENE_RACE_CD) == 2:  #Black
        person_fd.write('{0},'.format(OMOP_CONSTANTS.RACE_BLACK))
        person_fd.write('{0},'.format(OMOP_CONSTANTS.ETHNICITY_NON_HISPANIC))
    elif int(yd.BENE_RACE_CD) == 3:  #Others
        person_fd.write('{0},'.format(OMOP_CONSTANTS.RACE_OTHER))
        person_fd.write('{0},'.format(OMOP_CONSTANTS.ETHNICITY_NON_HISPANIC))
    elif int(yd.BENE_RACE_CD) == 5:  #Hispanic
        person_fd.write('{0},'.format(OMOP_CONSTANTS.RACE_NON_WHITE))
        person_fd.write('{0},'.format(OMOP_CONSTANTS.ETHNICITY_HISPANIC))
    else:
        person_fd.write('0,')
        person_fd.write('0,')

    #write person records to the person file
    state_county = str(beneficiary.SP_STATE_CODE) + '-' + str(beneficiary.BENE_COUNTY_CD)
    current_location_id = get_location_id(state_county)     # get the location id for the given pair of state & county
    person_fd.write('{0},'.format(current_location_id))                              # location_id
    person_fd.write(',')                                                                    # provider_id
    person_fd.write(',')                                                                    # care_site_id
    person_fd.write('{0},'.format(beneficiary.DESYNPUF_ID))                                 # person_source_value
    person_fd.write('{0},'.format(yd.BENE_SEX_IDENT_CD))                                    # gender_source_value
    person_fd.write(',')                 													# gender_source_concept_id
    person_fd.write('{0},'.format(yd.BENE_RACE_CD))                                         # race_source_value
    person_fd.write(',')                                                                    # race_source_concept_id
    person_fd.write('{0},'.format(yd.BENE_RACE_CD))                                         # ethnicity_source_value
    #person_fd.write('')                                                                    # ethnicity_source_concept_id
    person_fd.write('\n')
    person_fd.increment_recs_written(1)

# ----------------------------------------------------
# Write payer plan period records for each beneficiary
# ----------------------------------------------------
def write_payer_plan_period_record(beneficiary):
    payer_plan_period_fd = file_control.get_Descriptor('payer_plan_period')
    plan_source_value_list = ["Medicare Part A", "Medicare Part B", "HMO", "Medicare Part D"]
    ppyd = beneficiary.PayerPlanPerioYearDict()     # for all 3 years, get the number of months for each plan

    if not bool(ppyd):
        return       # dictionary is empty
    else:
        '''
        for k,v in ppyd.iteritems():
            if k[1] == 'BENE_HI_CVRAGE_TOT_MONS':       #plan A
                planA[k[0]] = v
            if k[1] == 'BENE_SMI_CVRAGE_TOT_MONS':      #plan B
                planB[k[0]] = v
            if k[1] == 'BENE_HMO_CVRAGE_TOT_MONS':      #HMO
                hmo[k[0]] = v
            if k[1] == 'PLAN_CVRG_MOS_NUM':             #plan D
                planD[k[0]] = v
        '''
        for plan_source_value in plan_source_value_list:
            if plan_source_value == "Medicare Part A":
                nd = {k[0]:v for k,v in ppyd.iteritems() if k[1] == 'BENE_HI_CVRAGE_TOT_MONS'}  # new dictionary with year as key and value as val
                payer_plan_period_dates = get_payer_plan_period_date_list(nd)
                for i in range(len(payer_plan_period_dates)):
                    payer_plan_period_start_date = payer_plan_period_dates[i][0]
                    payer_plan_period_end_date = payer_plan_period_dates[i][1]
                    plan_source_value = "Medicare Part A"
                    write_to_payer_plan_period_file(payer_plan_period_fd, beneficiary.person_id, payer_plan_period_start_date, payer_plan_period_end_date, plan_source_value)
            elif plan_source_value == "Medicare Part B":
                nd = {k[0]:v for k,v in ppyd.iteritems() if k[1] == 'BENE_SMI_CVRAGE_TOT_MONS'}  # new dictionary with year as key and value as val
                payer_plan_period_dates = get_payer_plan_period_date_list(nd)
                for i in range(len(payer_plan_period_dates)):
                    payer_plan_period_start_date = payer_plan_period_dates[i][0]
                    payer_plan_period_end_date = payer_plan_period_dates[i][1]
                    plan_source_value = "Medicare Part B"
                    write_to_payer_plan_period_file(payer_plan_period_fd, beneficiary.person_id, payer_plan_period_start_date, payer_plan_period_end_date, plan_source_value)
            elif plan_source_value == "Medicare Part D":
                nd = {k[0]:v for k,v in ppyd.iteritems() if k[1] == 'PLAN_CVRG_MOS_NUM'}  # new dictionary with year as key and value as val
                payer_plan_period_dates = get_payer_plan_period_date_list(nd)
                for i in range(len(payer_plan_period_dates)):
                    payer_plan_period_start_date = payer_plan_period_dates[i][0]
                    payer_plan_period_end_date = payer_plan_period_dates[i][1]
                    plan_source_value = "Medicare Part D"
                    write_to_payer_plan_period_file(payer_plan_period_fd, beneficiary.person_id, payer_plan_period_start_date, payer_plan_period_end_date, plan_source_value)
            elif plan_source_value == "HMO":
                nd = {k[0]:v for k,v in ppyd.iteritems() if k[1] == 'BENE_HMO_CVRAGE_TOT_MONS'}  # new dictionary with year as key and value as val
                payer_plan_period_dates = get_payer_plan_period_date_list(nd)
                for i in range(len(payer_plan_period_dates)):
                    payer_plan_period_start_date = payer_plan_period_dates[i][0]
                    payer_plan_period_end_date = payer_plan_period_dates[i][1]
                    plan_source_value = "HMO"
                    write_to_payer_plan_period_file(payer_plan_period_fd, beneficiary.person_id, payer_plan_period_start_date, payer_plan_period_end_date, plan_source_value)

#------------------------------------------------------
# write payer plan period data to the file
#--------------------------------------------------------
def write_to_payer_plan_period_file(payer_plan_period_fd, person_id, payer_plan_period_start_date, payer_plan_period_end_date, plan_source_value):
    payer_plan_period_fd.write('{0},'.format(table_ids.last_payer_plan_period_id))          # payer_plan_period_id
    payer_plan_period_fd.write('{0},'.format(person_id))                                    # person_id
    payer_plan_period_fd.write('{0},'.format(payer_plan_period_start_date))                 # payer_plan_period_start_date
    payer_plan_period_fd.write('{0},'.format(payer_plan_period_end_date))                   # payer_plan_period_end_date
    payer_plan_period_fd.write(',')                                                         # payer_source_value
    payer_plan_period_fd.write('{0},'.format(plan_source_value))                            # plan_source_value
    payer_plan_period_fd.write('')                                                          # family_source_value
    payer_plan_period_fd.write('\n')
    payer_plan_period_fd.increment_recs_written(1)
    table_ids.last_payer_plan_period_id += 1

#----------------------------------------------------------------
# generate the list of payer_plan_period start date and end date.
# date_list will be in this format date_list = [(d1,d2),(d1,d2)]
#-----------------------------------------------------------------
def get_payer_plan_period_date_list(plan):
    date_list = []
    # check if any year is missing. If yes, add that year. This will prevent dictionary keyError at runtime.
    for year in ['2008','2009','2010']:
        if year not in plan:
            plan[year] = 0
    # determine the start and end date for payer plan period
    if plan['2008'] == 12 and plan['2009'] == 12 and plan['2010'] == 12:
        payer_plan_period_start_date = '2008-01-01'
        payer_plan_period_end_date = '2010-12-31'
        date_list.append((payer_plan_period_start_date, payer_plan_period_end_date))
    elif plan['2008'] == 12 and plan['2009'] == 12 and plan['2010'] < 12:
        payer_plan_period_start_date = '2008-01-01'
        payer_plan_period_end_date = get_payer_plan_period_date(date(2009,12,31), plan['2010'])
        date_list.append((payer_plan_period_start_date, payer_plan_period_end_date))
    elif plan['2008'] == 12 and plan['2009'] < 12 and plan['2010'] == 12:
        payer_plan_period_start_date = '2008-01-01'
        payer_plan_period_end_date = '2008-12-31'
        date_list.append((payer_plan_period_start_date, payer_plan_period_end_date))
        if plan['2009'] > 0:
            payer_plan_period_start_date = '2009-01-01'
            payer_plan_period_end_date = get_payer_plan_period_date(date(2009,01,01), plan['2009'])
            date_list.append((payer_plan_period_start_date, payer_plan_period_end_date))
        payer_plan_period_start_date = '2010-01-01'
        payer_plan_period_end_date = '2010-12-31'
        date_list.append((payer_plan_period_start_date, payer_plan_period_end_date))
    elif plan['2008'] == 12 and plan['2009'] < 12 and plan['2010'] < 12:
        payer_plan_period_start_date = '2008-01-01'
        payer_plan_period_end_date = '2008-12-31'
        date_list.append((payer_plan_period_start_date, payer_plan_period_end_date))
        if plan['2009'] > 0:
            payer_plan_period_start_date = '2009-01-01'
            payer_plan_period_end_date = get_payer_plan_period_date(date(2009,01,01), plan['2009'])
            date_list.append((payer_plan_period_start_date, payer_plan_period_end_date))
        if plan['2010'] > 0:
            payer_plan_period_start_date = '2010-01-01'
            payer_plan_period_end_date = get_payer_plan_period_date(date(2010,01,01), plan['2010'])
            date_list.append((payer_plan_period_start_date, payer_plan_period_end_date))
    elif plan['2008'] < 12 and plan['2009'] == 12 and plan['2010'] == 12:
        if plan['2008'] == 0:
            payer_plan_period_start_date = '2009-01-01'
        else:
            payer_plan_period_start_date = get_payer_plan_period_date(date(2008,12,31), -1*plan['2008'])
        payer_plan_period_end_date = '2010-12-31'
        date_list.append((payer_plan_period_start_date, payer_plan_period_end_date))
    elif plan['2008'] < 12 and plan['2009'] == 12 and plan['2010'] < 12:
        if plan['2008'] == 0:
            payer_plan_period_start_date = '2009-01-01'
        else:
            payer_plan_period_start_date = get_payer_plan_period_date(date(2008,12,31), -1*plan['2008'])
        payer_plan_period_end_date = get_payer_plan_period_date(date(2009,12,31), plan['2010'])
        date_list.append((payer_plan_period_start_date, payer_plan_period_end_date))
    elif plan['2008'] < 12 and plan['2009'] < 12 and plan['2010'] == 12:
        if plan['2008'] > 0:
            payer_plan_period_start_date = '2008-01-01'
            payer_plan_period_end_date = get_payer_plan_period_date(date(2008,01,01), plan['2008'])
            date_list.append((payer_plan_period_start_date, payer_plan_period_end_date))
        if plan['2009'] > 0:
            payer_plan_period_start_date = '2009-01-01'
            payer_plan_period_end_date = get_payer_plan_period_date(date(2009,01,01), plan['2009'])
            date_list.append((payer_plan_period_start_date, payer_plan_period_end_date))
        payer_plan_period_start_date = '2010-01-01'
        payer_plan_period_end_date = '2010-12-31'
        date_list.append((payer_plan_period_start_date, payer_plan_period_end_date))
    elif plan['2008'] < 12 and plan['2009'] < 12 and plan['2010'] < 12:
        if plan['2008'] > 0:
            payer_plan_period_start_date = '2008-01-01'
            payer_plan_period_end_date = get_payer_plan_period_date(date(2008,01,01), plan['2008'])
            date_list.append((payer_plan_period_start_date, payer_plan_period_end_date))
        if plan['2009'] > 0:
            payer_plan_period_start_date = '2009-01-01'
            payer_plan_period_end_date = get_payer_plan_period_date(date(2009,01,01), plan['2009'])
            date_list.append((payer_plan_period_start_date, payer_plan_period_end_date))
        if plan['2010'] > 0:
            payer_plan_period_start_date = '2010-01-01'
            payer_plan_period_end_date = get_payer_plan_period_date(date(2010,01,01), plan['2010'])
            date_list.append((payer_plan_period_start_date, payer_plan_period_end_date))
    return date_list

#---------------------------------------------------------------------
# use the start/end date and number of months(delta) to calculate the
# end/start date
#--------------------------------------------------------------------
def get_payer_plan_period_date(date, delta):
    m, y = (date.month+delta) % 12, date.year + ((date.month)+delta-1) // 12  # calculate new month and year
    if m == 0:
        m = 12
    d = min(date.day, calendar.monthrange(y, m)[1])     # get the last date of the month
    return date.replace(day=d,month=m, year=y)      # return the new date

# -----------------------------------
# Write Location records
# -----------------------------------
def write_location_record(beneficiary):
    state_county = str(beneficiary.SP_STATE_CODE) + '-' + str(beneficiary.BENE_COUNTY_CD)
    current_location_id = get_location_id(state_county)     # get the location id for the given pair of state & county
    idx = person_location_dict[state_county][1]
    if idx == 0:
        location_fd = file_control.get_Descriptor('location')
        location_fd.write('{0},'.format(current_location_id))                               # location_id
        location_fd.write(',')
        location_fd.write(',')
        location_fd.write(',')
        try:
            location_fd.write('{0},'.format(SSA_state_codes[beneficiary.SP_STATE_CODE]))    # state_code - if SSA code is present in the dictionary
        except:
            location_fd.write('{0},'.format(beneficiary.SP_STATE_CODE))                     # if SSA code is not present in the dictionary
        location_fd.write(',')
        if len(beneficiary.SP_STATE_CODE) == 1:             # convert to 2 bytes
            beneficiary.SP_STATE_CODE = '0' + str(beneficiary.SP_STATE_CODE)
        if len(beneficiary.BENE_COUNTY_CD) == 1:            # convert to 3 bytes
            beneficiary.BENE_COUNTY_CD = '00' + str(beneficiary.BENE_COUNTY_CD)
        elif len(beneficiary.BENE_COUNTY_CD) == 2:            # convert to 3 bytes
            beneficiary.BENE_COUNTY_CD = '0' + str(beneficiary.BENE_COUNTY_CD)
        local_county_code = str(beneficiary.SP_STATE_CODE) + str(beneficiary.BENE_COUNTY_CD)
        location_fd.write('{0},'.format(local_county_code))     # county_code
        location_fd.write('{0}'.format(beneficiary.LOCATION_ID))                         # location_source_value
        location_fd.write('\n')
        location_fd.increment_recs_written(1)
        person_location_dict[state_county] = [person_location_dict[state_county][0],1]  # change the status to written

# -----------------------------------
# Observation Period
# -----------------------------------
def write_observation_period_records(beneficiary):

    #There are beneficiaries who are listed but have no activity, so we generate no observation period
    if len(beneficiary.visit_dates) == 0:
        return
    obs_period_fd = file_control.get_Descriptor('observation_period')
    start_date = min(beneficiary.visit_dates.keys())
    end_date = max(beneficiary.visit_dates.keys())

    obs_period_fd.write('{0},'.format(table_ids.last_observation_period_id))
    obs_period_fd.write('{0},'.format(beneficiary.person_id))
    obs_period_fd.write('{0},'.format(start_date))
    obs_period_fd.write('{0},'.format(end_date))
    obs_period_fd.write('{0}'.format(OMOP_CONSTANTS.OBS_PERIOD_ENROLLED_INSURANCE))
    obs_period_fd.write('\n')
    obs_period_fd.increment_recs_written(1)
    table_ids.last_observation_period_id += 1

# -----------------------------------
# Death Record
# -----------------------------------
def write_death_records(death_fd, beneficiary, death_type_concept_id, cause_source_concept_id):
    yd = beneficiary.LatestYearData()
    if yd is not None and yd.BENE_DEATH_DT != '':       # if year data for BENE_DEATH_DT is not available, don't write to death file.
        death_fd.write('{0},'.format(beneficiary.person_id))
        death_fd.write('{0},'.format(get_date_YYYY_MM_DD(yd.BENE_DEATH_DT)))
        death_fd.write('{0},'.format(death_type_concept_id))
        death_fd.write(',')                                                    # cause_concept_id
        death_fd.write(',')                                                     # cause_source_value
        death_fd.write('{0}'.format(cause_source_concept_id))
        death_fd.write('\n')
        death_fd.increment_recs_written(1)

# -----------------------------------
# Drug Exposure
# -----------------------------------
def write_drug_exposure(drug_exp_fd, person_id, drug_concept_id, start_date, drug_type_concept_id,
                        quantity, days_supply, drug_source_concept_id, drug_source_value, provider_id, visit_occurrence_id):
        drug_exp_fd.write('{0},'.format(table_ids.last_drug_exposure_id))
        drug_exp_fd.write('{0},'.format(person_id))
        drug_exp_fd.write('{0},'.format(drug_concept_id))
        drug_exp_fd.write('{0},'.format(get_date_YYYY_MM_DD(start_date)))              # drug_exposure_start_date
        drug_exp_fd.write(',')                                                          # drug_exposure_end_date
        drug_exp_fd.write('{0},'.format(drug_type_concept_id))
        drug_exp_fd.write(',')                                                          # stop_reason
        drug_exp_fd.write(',')                                                          # refills
        if quantity is None:
            drug_exp_fd.write(',')
        else:
            drug_exp_fd.write('{0},'.format(float(quantity)))
        if days_supply is None:
            drug_exp_fd.write(',')
        else:
            drug_exp_fd.write('{0},'.format(days_supply))
        drug_exp_fd.write(',')                                                          # sig
        drug_exp_fd.write(',')                                                          # route_concept_id
        drug_exp_fd.write(',')                                                          # effective_drug_dose
        drug_exp_fd.write(',')                                                          # dose_unit_concept_ id
        drug_exp_fd.write(',')                                                          # lot_number
        drug_exp_fd.write('{0},'.format(provider_id))                                   # provider_id
        drug_exp_fd.write('{0},'.format(visit_occurrence_id))
        drug_exp_fd.write('{0},'.format(drug_source_value))
        drug_exp_fd.write('{0},'.format(drug_source_concept_id))
        drug_exp_fd.write(',')                                                          # route_source_value
        #drug_exp_fd.write('')                                                          # dose_unit_source_value
        drug_exp_fd.write('\n')
        drug_exp_fd.increment_recs_written(1)
        table_ids.last_drug_exposure_id += 1

# -----------------------------------
# Device Exposure
# -----------------------------------
def write_device_exposure(device_fd, person_id, device_concept_id, start_date, end_date, device_type_concept_id,
                          device_source_value, device_source_concept_id, provider_id, visit_occurrence_id):
    device_fd.write('{0},'.format(table_ids.last_device_exposure_id))
    device_fd.write('{0},'.format(person_id))
    device_fd.write('{0},'.format(device_concept_id))
    device_fd.write('{0},'.format(get_date_YYYY_MM_DD(start_date)))
    device_fd.write('{0},'.format(get_date_YYYY_MM_DD(end_date)))
    device_fd.write('{0},'.format(device_type_concept_id))
    device_fd.write(',')        # unique_device_id
    device_fd.write(',')        # quantity
    device_fd.write('{0},'.format(provider_id))        # provider_id
    device_fd.write('{0},'.format(visit_occurrence_id))
    device_fd.write('{0},'.format(device_source_value))
    device_fd.write('{0}'.format(device_source_concept_id))
    device_fd.write('\n')
    device_fd.increment_recs_written(1)
    table_ids.last_device_exposure_id += 1

# -----------------------------------
# Prescription Drug File -> Drug Exposure; Drug Cost
# -----------------------------------
def write_drug_records(beneficiary):
    drug_exp_fd = file_control.get_Descriptor('drug_exposure')
    drug_cost_fd = file_control.get_Descriptor('drug_cost')

    for raw_rec in beneficiary.prescription_records:
        rec = PrescriptionDrug(raw_rec)
        if rec.SRVC_DT == '':
            continue

        ndc_code = rec.PROD_SRVC_ID

        if (OMOP_CONSTANTS.NDC_VOCABULARY_ID,ndc_code) in source_code_concept_dict:
            #In practice we do not see multiple mappings of drugs, but in principle it could happen
            for sccd in source_code_concept_dict[OMOP_CONSTANTS.NDC_VOCABULARY_ID,ndc_code]:
                drug_source_concept_id = sccd.source_concept_id
                drug_concept_id = sccd.target_concept_id
                write_drug_exposure(drug_exp_fd, beneficiary.person_id,
                                    drug_concept_id=drug_concept_id,
                                    start_date=rec.SRVC_DT,
                                    drug_type_concept_id=OMOP_CONSTANTS.DRUG_TYPE_PRESCRIPTION,
                                    quantity=rec.QTY_DSPNSD_NUM,
                                    days_supply=rec.DAYS_SUPLY_NUM,
                                    drug_source_concept_id=drug_source_concept_id,
                                    drug_source_value=ndc_code,
                                    provider_id="",
                                    visit_occurrence_id="")
        else:
            #These are for any NDC codes not in CONCEPT.csv
            dline = 'DrugRecords--- ' + 'Unmapped NDC code: ' + str(ndc_code) + ' DESYNPUF_ID: ' + rec.DESYNPUF_ID + '\n'
            unmapped_log.write(dline)
            write_drug_exposure(drug_exp_fd, beneficiary.person_id,
                                drug_concept_id="0",
                                start_date=rec.SRVC_DT,
                                drug_type_concept_id=OMOP_CONSTANTS.DRUG_TYPE_PRESCRIPTION,
                                quantity=rec.QTY_DSPNSD_NUM,
                                days_supply=rec.DAYS_SUPLY_NUM,
                                drug_source_concept_id="0",
                                drug_source_value=ndc_code,
                                provider_id="",
                                visit_occurrence_id="")
        #----------------------
        # drug cost -- only written once, even if (doesn't happen now) NDC code maps to multiple RxNorm drugs
        #----------------------
        current_drug_exposure_id = table_ids.last_drug_exposure_id - 1  #subtracted 1 as drug_exposure function added 1 to last_drug_exposure_id
        drug_cost_fd.write('{0},'.format(table_ids.last_drug_cost_id))
        drug_cost_fd.write('{0},'.format(current_drug_exposure_id))
        drug_cost_fd.write('{0},'.format(OMOP_CONSTANTS.CURRENCY_US_DOLLAR))
        drug_cost_fd.write(',')                                          # paid_copay
        drug_cost_fd.write('{0},'.format(rec.PTNT_PAY_AMT))              # paid_coinsurance
        drug_cost_fd.write(',')                                          # paid_toward_deductible
        drug_cost_fd.write(',')                                          # paid_by_payer
        drug_cost_fd.write(',')                                          # paid_by_coordination_of_benefits
        drug_cost_fd.write('{0},'.format(rec.PTNT_PAY_AMT))              # total_out_of_pocket      #
        drug_cost_fd.write('{0},'.format(rec.TOT_RX_CST_AMT))            # total_paid  #
        drug_cost_fd.write(',')                                          # ingredient_cost
        drug_cost_fd.write(',')                                          # dispensing_fee
        drug_cost_fd.write(',')                                          # average_wholesale_price
        #drug_cost_fd.write('')                                          # payer_plan_period_id                  ##### At moment we do not have payer_plan_period implemented, as we have no payer plan information.
        drug_cost_fd.write('\n')
        drug_cost_fd.increment_recs_written(1)
        table_ids.last_drug_cost_id += 1

# -----------------------------------
# Provider file
# -----------------------------------
def write_provider_record(provider_fd, npi, provider_id, care_site_id, provider_source_value):
    if not provider_id:
        return
    idx = npi_provider_id[npi][1]
    if idx == 0:
        provider_fd.write('{0},'.format(provider_id))
        provider_fd.write(',')                                                            # provider_name
        provider_fd.write('{0},'.format(npi))
        provider_fd.write(',')                                                            # dea
        provider_fd.write(',')
        provider_fd.write('{0},'.format(care_site_id))
        provider_fd.write(',')                                                            # year_of_birth
        provider_fd.write(',')                                                            # gender_concept_id
        provider_fd.write('{0},'.format(provider_source_value))                           # provider_source_value
        provider_fd.write(',')                                                            # specialty_source_value
        provider_fd.write(',')                                                            # specialty_source_concept_id
        provider_fd.write(',')                                                            # gender_source_value
        #provider_fd.write('')                                                            # gender_source_concept_id
        provider_fd.write('\n')
        provider_fd.increment_recs_written(1)
        npi_provider_id[npi] = [npi_provider_id[npi][0],1]  #set index to 1 to mark provider_id written


# -----------------------------------
# Condition Occurence file
#  - Added provider_id
# -----------------------------------
def write_condition_occurrence(cond_occur_fd, person_id, condition_concept_id,
                              from_date, thru_date, condition_type_concept_id, provider_id,
                              condition_source_value, condition_source_concept_id, visit_occurrence_id):
    cond_occur_fd.write('{0},'.format(table_ids.last_condition_occurrence_id))
    cond_occur_fd.write('{0},'.format(person_id))
    cond_occur_fd.write('{0},'.format(condition_concept_id))
    cond_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(from_date)))
    cond_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(thru_date)))
    cond_occur_fd.write('{0},'.format(condition_type_concept_id))
    cond_occur_fd.write(',')                                          # stop_reason
    cond_occur_fd.write('{0},'.format(provider_id))                   # provider_id
    cond_occur_fd.write('{0},'.format(visit_occurrence_id))
    cond_occur_fd.write('{0},'.format(condition_source_value))
    cond_occur_fd.write('{0}'.format(condition_source_concept_id))
    cond_occur_fd.write('\n')
    cond_occur_fd.increment_recs_written(1)
    table_ids.last_condition_occurrence_id += 1

# -----------------------------------
#  - Added this new function to
# create Visit Occurence file
# -----------------------------------
def write_visit_occurrence(visit_occur_fd, person_id, visit_concept_id, visit_occurrence_id, care_site_id, visit_source_concept_id,
                              from_date, thru_date, visit_type_concept_id, provider_id, visit_source_value):
    visit_occur_fd.write('{0},'.format(visit_occurrence_id))
    visit_occur_fd.write('{0},'.format(person_id))
    visit_occur_fd.write('{0},'.format(visit_concept_id))
    visit_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(from_date)))
    visit_occur_fd.write(',')                                          # visit_start_time
    visit_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(thru_date)))
    visit_occur_fd.write(',')                                          # visit_end_time
    visit_occur_fd.write('{0},'.format(visit_type_concept_id))
    visit_occur_fd.write('{0},'.format(provider_id))                   # provider_id
    visit_occur_fd.write('{0},'.format(care_site_id))                  # care_site_id
    visit_occur_fd.write('{0},'.format(visit_source_value))
    #visit_occur_fd.write('')                                          # visit_source_concept_id
    visit_occur_fd.write('\n')
    visit_occur_fd.increment_recs_written(1)

# -----------------------------------
# Procedure Occurence file
# -----------------------------------
def write_procedure_occurrence(proc_occur_fd, person_id, procedure_concept_id,
                              from_date, procedure_type_concept_id,provider_id,modifier_concept_id,
                              procedure_source_value, procedure_source_concept_id, visit_occurrence_id):
    proc_occur_fd.write('{0},'.format(table_ids.last_procedure_occurrence_id))
    proc_occur_fd.write('{0},'.format(person_id))
    proc_occur_fd.write('{0},'.format(procedure_concept_id))
    proc_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(from_date))) # procedure_date
    proc_occur_fd.write('{0},'.format(procedure_type_concept_id))
    proc_occur_fd.write(',')                                           # modifier_concept_id
    proc_occur_fd.write(',')                                           # quantity
    proc_occur_fd.write('{0},'.format(provider_id))                    # provider_id
    proc_occur_fd.write('{0},'.format(visit_occurrence_id))
    proc_occur_fd.write('{0},'.format(procedure_source_value))
    proc_occur_fd.write('{0},'.format(procedure_source_concept_id))
    #proc_occur_fd.write('')                                          # qualifier_source_value
    proc_occur_fd.write('\n')
    proc_occur_fd.increment_recs_written(1)
    table_ids.last_procedure_occurrence_id += 1

# -----------------------------------
# Measurement file
# -----------------------------------
def write_measurement(measurement_fd, person_id, measurement_concept_id,
                      measurement_date, measurement_type_concept_id,
                      measurement_source_value, measurement_source_concept_id, provider_id, visit_occurrence_id):
    measurement_fd.write('{0},'.format(table_ids.last_measurement_id))
    measurement_fd.write('{0},'.format(person_id))
    measurement_fd.write('{0},'.format(measurement_concept_id))
    measurement_fd.write('{0},'.format(get_date_YYYY_MM_DD(measurement_date)))
    measurement_fd.write(',')        # measurement_time
    measurement_fd.write('{0},'.format(measurement_type_concept_id))
    measurement_fd.write(',')        # operator_concept_id
    measurement_fd.write(',')        # value_as_number
    measurement_fd.write('0,')       # value_as_concept_id
    measurement_fd.write(',')        # unit_concept_id
    measurement_fd.write(',')        # range_low
    measurement_fd.write(',')        # range_high
    measurement_fd.write('{0},'.format(provider_id))        # provider_id
    measurement_fd.write('{0},'.format(visit_occurrence_id))
    measurement_fd.write('{0},'.format(measurement_source_value))
    measurement_fd.write('{0},'.format(measurement_source_concept_id))
    measurement_fd.write(',')        # unit_source_value
    #measurement_fd.write('')        # value_source_value
    measurement_fd.write('\n')
    measurement_fd.increment_recs_written(1)
    table_ids.last_measurement_id += 1

# -----------------------------------
# Observation file
# -----------------------------------
def write_observation(observation_fd, person_id, observation_concept_id,provider_id,
                      observation_date, observation_type_concept_id,
                      observation_source_value, observation_source_concept_id, visit_occurrence_id):
    observation_fd.write('{0},'.format(table_ids.last_observation_id))
    observation_fd.write('{0},'.format(person_id))
    observation_fd.write('{0},'.format(observation_concept_id))
    observation_fd.write('{0},'.format(get_date_YYYY_MM_DD(observation_date)))
    observation_fd.write(',')        # observation_time
    observation_fd.write('{0},'.format(observation_type_concept_id))
    observation_fd.write(',')        # value_as_number
    observation_fd.write(',')        # value_as_string
    observation_fd.write('0,')       # value_as_concept_id
    observation_fd.write(',')        # qualifier_concept_id
    observation_fd.write(',')        # unit_concept_id
    observation_fd.write('{0},'.format(provider_id))   # provider_id
    observation_fd.write('{0},'.format(visit_occurrence_id))
    observation_fd.write('{0},'.format(observation_source_value))
    observation_fd.write('{0},'.format(observation_source_concept_id))
    observation_fd.write(',')        # unit_source_value
    #observation_fd.write('')        # qualifier_source_value
    observation_fd.write('\n')
    observation_fd.increment_recs_written(1)
    table_ids.last_observation_id += 1


# -----------------------------------
# Write to Care Site file
# -----------------------------------
def write_care_site(care_site_fd, care_site_id, place_of_service_concept_id, care_site_source_value, place_of_service_source_value):
    if not care_site_id:
        return
    idx = provider_id_care_site_id[care_site_source_value][1]
    if idx == 0:
        care_site_fd.write('{0},'.format(care_site_id))
        care_site_fd.write(',')                                      # care_site_name
        care_site_fd.write('{0},'.format(place_of_service_concept_id))
        care_site_fd.write(',')                                      # location_id
        care_site_fd.write('{0},'.format(care_site_source_value))
        care_site_fd.write('{0}'.format(place_of_service_source_value))
        care_site_fd.write('\n')
        care_site_fd.increment_recs_written(1)
        provider_id_care_site_id[care_site_source_value] = [provider_id_care_site_id[care_site_source_value][0],1]   # change index to 1 to mark it written

# -----------------------------------
# From Inpatient Records:
#     --> Visit Occurrence
#     --> Visit Cost
#     --> Procedure Occurrence
#     --> Drug Exposure
#     --> Device Exposure
#     --> Condition Occurrence
#     --> Measurement Occurrence
#     --> Observation
#     --> Care Site
#     --> Provider
# -----------------------------------
def process_inpatient_records(beneficiary):
    drug_exp_fd = file_control.get_Descriptor('drug_exposure')
    drug_cost_fd = file_control.get_Descriptor('drug_cost')
    proc_occur_fd = file_control.get_Descriptor('procedure_occurrence')
    proc_cost_fd = file_control.get_Descriptor('procedure_cost')
    cond_occur_fd = file_control.get_Descriptor('condition_occurrence')
    death_fd = file_control.get_Descriptor('death')
    care_site_fd = file_control.get_Descriptor('care_site')
    provider_fd = file_control.get_Descriptor('provider')
    measurement_fd = file_control.get_Descriptor('measurement_occurrence')
    observation_fd = file_control.get_Descriptor('observation')
    device_fd = file_control.get_Descriptor('device_exposure')
    visit_occur_fd = file_control.get_Descriptor('visit_occurrence')
    visit_cost_fd = file_control.get_Descriptor('visit_cost')

#    location_fd = file_control.get_Descriptor('location')
    for raw_rec in beneficiary.inpatient_records:
        rec = InpatientClaim(raw_rec)
        if rec.CLM_FROM_DT == '':
            continue

        # initialize both care_site_id and provider_id to null as some institution might not have PRVDR_NUM and some NPI might be null.
        care_site_id = ""
        provider_id  = ""
        # --get care_site_id (a unique number generated by the program) for the given institution (PRVDR_NUM)
        if rec.PRVDR_NUM != '':
            provider_number = rec.PRVDR_NUM
            care_site_id = get_CareSite(provider_number)
            write_care_site(care_site_fd, care_site_id,
                           place_of_service_concept_id=OMOP_CONSTANTS.INPATIENT_PLACE_OF_SERVICE,
                           care_site_source_value=rec.PRVDR_NUM,
                           place_of_service_source_value=OMOP_CONSTANTS.INPATIENT_PLACE_OF_SERVICE_SOURCE)
        #-- get provider_id (a unique number generated by the program) for the given NPI. Each NPI will have its own provider_id
        for npi in (rec.AT_PHYSN_NPI, rec.OP_PHYSN_NPI, rec.OT_PHYSN_NPI):
            if npi != '':
                provider_id = get_Provider(npi)
                write_provider_record(provider_fd, npi, provider_id, care_site_id, rec.AT_PHYSN_NPI)

        #-- get visit id. Person id + CLM_FROM_DT + CLM_THRU_DT + institution number(PRVDR_NUM) make the key for a particular visit
        current_visit_id = visit_occurrence_ids[rec.DESYNPUF_ID,rec.CLM_FROM_DT,rec.CLM_THRU_DT,rec.PRVDR_NUM]
        for (vocab,code) in ([(OMOP_CONSTANTS.ICD_9_VOCAB_ID, x) for x in rec.ICD9_DGNS_CD_list] +
                             [(OMOP_CONSTANTS.ICD_9_VOCAB_ID,x) for x in rec.ICD9_PRCDR_CD_list] +
                             [(OMOP_CONSTANTS.HCPCS_VOCABULARY_ID, x) for x in rec.HCPCS_CD_list]):

            if rec.CLM_FROM_DT != '':
                if (vocab,code) in source_code_concept_dict:
                    for sccd in source_code_concept_dict[vocab,code]:
                        target_concept_id = sccd.target_concept_id
                        source_concept_id = sccd.source_concept_id
                        destination_file = sccd.destination_file

                        if destination_file == DESTINATION_FILE_PROCEDURE:
                            write_procedure_occurrence(proc_occur_fd, beneficiary.person_id,
                                                       procedure_concept_id=target_concept_id,
                                                       from_date=rec.CLM_FROM_DT,
                                                       procedure_type_concept_id=OMOP_CONSTANTS.INPAT_PROCEDURE_1ST_POSITION,
                                                       procedure_source_value=code,
                                                       procedure_source_concept_id=source_concept_id,
                                                       provider_id=provider_id,
                                                       modifier_concept_id=0,
                                                       visit_occurrence_id=current_visit_id)

                        elif destination_file == DESTINATION_FILE_CONDITION:
                            write_condition_occurrence(cond_occur_fd,beneficiary.person_id,
                                                       condition_concept_id=target_concept_id,
                                                       from_date=rec.CLM_FROM_DT, thru_date=rec.CLM_THRU_DT,
                                                       condition_type_concept_id=OMOP_CONSTANTS.INPAT_CONDITION_1ST_POSITION,
                                                       condition_source_value=code,
                                                       condition_source_concept_id=source_concept_id,
                                                       provider_id=provider_id,
                                                       visit_occurrence_id=current_visit_id)

                        elif destination_file == DESTINATION_FILE_DRUG:
                            write_drug_exposure(drug_exp_fd, beneficiary.person_id,
                                                drug_concept_id=target_concept_id,
                                                start_date=rec.CLM_FROM_DT,
                                                drug_type_concept_id=OMOP_CONSTANTS.DRUG_TYPE_PRESCRIPTION,
                                                quantity=None,
                                                days_supply=None,
                                                drug_source_value=code,
                                                drug_source_concept_id=source_concept_id,
                                                provider_id=provider_id,
                                                visit_occurrence_id=current_visit_id)

                        elif destination_file == DESTINATION_FILE_MEASUREMENT:
                            write_measurement(measurement_fd, beneficiary.person_id,
                                              measurement_concept_id=target_concept_id,
                                              measurement_date=rec.CLM_FROM_DT,
                                              measurement_type_concept_id=OMOP_CONSTANTS.MEASUREMENT_DERIVED_VALUE,
                                              measurement_source_value=code,
                                              measurement_source_concept_id=source_concept_id,
                                              provider_id=provider_id,
                                              visit_occurrence_id=current_visit_id)

                        elif destination_file == DESTINATION_FILE_OBSERVATION:
                            write_observation(observation_fd, beneficiary.person_id,
                                              observation_concept_id=target_concept_id,
                                              observation_date=rec.CLM_FROM_DT,
                                              observation_type_concept_id=OMOP_CONSTANTS.OBSERVATION_CHIEF_COMPLAINT,
                                              observation_source_value=code,
                                              observation_source_concept_id=source_concept_id,
                                              provider_id=provider_id,
                                              visit_occurrence_id=current_visit_id)

                        elif destination_file == DESTINATION_FILE_DEVICE:
                            write_device_exposure(device_fd, beneficiary.person_id,
                                                  device_concept_id=target_concept_id,
                                                  start_date=rec.CLM_FROM_DT,
                                                  end_date=rec.CLM_THRU_DT,
                                                  device_type_concept_id=OMOP_CONSTANTS.DEVICE_INFERRED_PROCEDURE_CLAIM,
                                                  device_source_value=code,
                                                  device_source_concept_id=source_concept_id,
                                                  provider_id=provider_id,
                                                  visit_occurrence_id=current_visit_id)

                #-- Write each unique visit to visit_occurrence file.
                    if current_visit_id not in visit_id_list:
                        write_visit_occurrence(visit_occur_fd,beneficiary.person_id,
                                                   visit_concept_id=OMOP_CONSTANTS.INPAT_VISIT_CONCEPT_ID,
                                                   from_date=rec.CLM_FROM_DT, thru_date=rec.CLM_THRU_DT,
                                                   visit_type_concept_id=OMOP_CONSTANTS.INPAT_VISIT_1ST_POSITION,
                                                   visit_source_value=rec.CLM_ID,
                                                   visit_source_concept_id=source_concept_id,
                                                   care_site_id=care_site_id,
                                                   provider_id=provider_id,
                                                   visit_occurrence_id=current_visit_id)

                        visit_id_list.add(current_visit_id)

                else:
                    dfile = 'Inpatient--- unmapped ' + str(vocab) + ' code: ' + str(code) + ' DESYNPUF_ID: ' + rec.DESYNPUF_ID + '\n'
                    unmapped_log.write(dfile)

        #-- care site / provider

# -----------------------------------
# From Outpatient Records:
#     --> Visit Occurrence
#     --> Visit Cost
#     --> Procedure Occurrence
#     --> Drug Exposure
#     --> Device Exposure
#     --> Device Exposure Cost
#     --> Condition Occurrence
#     --> Measurement Occurrence
#     --> Observation
#     --> Care Site
#     --> Provider
# -----------------------------------

def process_outpatient_records(beneficiary):
    drug_exp_fd = file_control.get_Descriptor('drug_exposure')
    drug_cost_fd = file_control.get_Descriptor('drug_cost')
    proc_occur_fd = file_control.get_Descriptor('procedure_occurrence')
    proc_cost_fd = file_control.get_Descriptor('procedure_cost')
    cond_occur_fd = file_control.get_Descriptor('condition_occurrence')
    death_fd = file_control.get_Descriptor('death')
    care_site_fd = file_control.get_Descriptor('care_site')
    provider_fd = file_control.get_Descriptor('provider')
    measurement_fd = file_control.get_Descriptor('measurement_occurrence')
    observation_fd = file_control.get_Descriptor('observation')
    device_fd = file_control.get_Descriptor('device_exposure')
    visit_occur_fd = file_control.get_Descriptor('visit_occurrence')
    visit_cost_fd = file_control.get_Descriptor('visit_cost')

    for raw_rec in beneficiary.outpatient_records:
        rec = OutpatientClaim(raw_rec)
        if rec.CLM_FROM_DT == '':
            continue

        # initialize both care_site_id and provider_id to null as some institution might not have PRVDR_NUM and some NPI might be null.
        care_site_id = ""
        provider_id  = ""
        #-- get care_site_id (a unique number generated by the program) for the given institution (PRVDR_NUM)
        if rec.PRVDR_NUM != '':
            provider_number = rec.PRVDR_NUM
            care_site_id = get_CareSite(provider_number)
            write_care_site(care_site_fd, care_site_id,
                           place_of_service_concept_id=OMOP_CONSTANTS.OUTPATIENT_PLACE_OF_SERVICE,
                           care_site_source_value=rec.PRVDR_NUM,
                           place_of_service_source_value=OMOP_CONSTANTS.OUTPATIENT_PLACE_OF_SERVICE_SOURCE)

        #-- get provider_id (a unique number generated by the program) for the given NPI. Each NPI will have its own provider_id
        for npi in (rec.AT_PHYSN_NPI, rec.OP_PHYSN_NPI, rec.OT_PHYSN_NPI):
            if npi != '':
                provider_id = get_Provider(npi)
                write_provider_record(provider_fd, npi, provider_id, care_site_id, rec.AT_PHYSN_NPI)
        #-- get visit id. Person id + CLM_FROM_DT + CLM_THRU_DT + institution number(PRVDR_NUM) make the key for a particular visit
        current_visit_id = visit_occurrence_ids[rec.DESYNPUF_ID,rec.CLM_FROM_DT,rec.CLM_THRU_DT,rec.PRVDR_NUM]
        for (vocab,code) in ( ([] if rec.ADMTNG_ICD9_DGNS_CD == "" else [(OMOP_CONSTANTS.ICD_9_VOCAB_ID,rec.ADMTNG_ICD9_DGNS_CD)]) +
                                       [(OMOP_CONSTANTS.ICD_9_VOCAB_ID,x) for x in rec.ICD9_DGNS_CD_list] +
                                       [(OMOP_CONSTANTS.ICD_9_VOCAB_ID,x) for x in rec.ICD9_PRCDR_CD_list] +
                                       [(OMOP_CONSTANTS.HCPCS_VOCABULARY_ID,x) for x in rec.HCPCS_CD_list]):
            if rec.CLM_FROM_DT != '':
                if (vocab,code) in source_code_concept_dict:
                    for sccd in source_code_concept_dict[vocab,code]:
                        target_concept_id = sccd.target_concept_id
                        source_concept_id = sccd.source_concept_id
                        destination_file = sccd.destination_file

                        if destination_file == DESTINATION_FILE_PROCEDURE:
                            write_procedure_occurrence(proc_occur_fd, beneficiary.person_id,
                                                       procedure_concept_id=target_concept_id,
                                                       from_date=rec.CLM_FROM_DT,
                                                       procedure_type_concept_id=OMOP_CONSTANTS.OUTPAT_PROCEDURE_1ST_POSITION,
                                                       procedure_source_value=code,
                                                       procedure_source_concept_id=source_concept_id,
                                                       provider_id=provider_id,
                                                       modifier_concept_id=0,
                                                       visit_occurrence_id=current_visit_id)

                        elif destination_file == DESTINATION_FILE_CONDITION:
                            write_condition_occurrence(cond_occur_fd,beneficiary.person_id,
                                                       condition_concept_id=target_concept_id,
                                                       from_date=rec.CLM_FROM_DT, thru_date=rec.CLM_THRU_DT,
                                                       condition_type_concept_id=OMOP_CONSTANTS.OUTPAT_CONDITION_1ST_POSITION,
                                                       condition_source_value=code,
                                                       condition_source_concept_id=source_concept_id,
                                                       provider_id=provider_id,
                                                       visit_occurrence_id=current_visit_id)

                        elif destination_file == DESTINATION_FILE_DRUG:
                            write_drug_exposure(drug_exp_fd, beneficiary.person_id,
                                                drug_concept_id=target_concept_id,
                                                start_date=rec.CLM_FROM_DT,
                                                drug_type_concept_id=OMOP_CONSTANTS.DRUG_TYPE_PRESCRIPTION,
                                                quantity=None,
                                                days_supply=None,
                                                drug_source_value=code,
                                                drug_source_concept_id=source_concept_id,
                                                provider_id=provider_id,
                                                visit_occurrence_id=current_visit_id)

                        elif destination_file == DESTINATION_FILE_MEASUREMENT:
                            write_measurement(measurement_fd, beneficiary.person_id,
                                              measurement_concept_id=target_concept_id,
                                              measurement_date=rec.CLM_FROM_DT,
                                              measurement_type_concept_id=OMOP_CONSTANTS.MEASUREMENT_DERIVED_VALUE,
                                              measurement_source_value=code,
                                              measurement_source_concept_id=source_concept_id,
                                              provider_id=provider_id,
                                              visit_occurrence_id=current_visit_id)

                        elif destination_file == DESTINATION_FILE_OBSERVATION:
                            write_observation(observation_fd, beneficiary.person_id,
                                              observation_concept_id=target_concept_id,
                                              observation_date=rec.CLM_FROM_DT,
                                              observation_type_concept_id=OMOP_CONSTANTS.OBSERVATION_CHIEF_COMPLAINT,
                                              observation_source_value=code,
                                              observation_source_concept_id=source_concept_id,
                                              provider_id=provider_id,
                                              visit_occurrence_id=current_visit_id)

                        elif destination_file == DESTINATION_FILE_DEVICE:
                            write_device_exposure(device_fd, beneficiary.person_id,
                                                  device_concept_id=target_concept_id,
                                                  start_date=rec.CLM_FROM_DT,
                                                  end_date=rec.CLM_THRU_DT,
                                                  device_type_concept_id=OMOP_CONSTANTS.DEVICE_INFERRED_PROCEDURE_CLAIM,
                                                  device_source_value=code,
                                                  device_source_concept_id=source_concept_id,
                                                  provider_id=provider_id,
                                                  visit_occurrence_id=current_visit_id)

                #-- Write each unique visit to visit_occurrence file.
                    if current_visit_id not in visit_id_list:
                        write_visit_occurrence(visit_occur_fd,beneficiary.person_id,
                                                   visit_concept_id=OMOP_CONSTANTS.OUTPAT_VISIT_CONCEPT_ID,
                                                   from_date=rec.CLM_FROM_DT, thru_date=rec.CLM_THRU_DT,
                                                   visit_type_concept_id=OMOP_CONSTANTS.OUTPAT_VISIT_1ST_POSITION,
                                                   visit_source_value=rec.CLM_ID,
                                                   visit_source_concept_id=source_concept_id,
                                                   care_site_id=care_site_id,
                                                   provider_id=provider_id,
                                                   visit_occurrence_id=current_visit_id)
                        visit_id_list.add(current_visit_id)

                else:
                    dfile = 'Outpatient--- unmapped ' + str(vocab) + ' code: ' + str(code) + ' DESYNPUF_ID: ' + rec.DESYNPUF_ID + '\n'
                    unmapped_log.write(dfile)

# -----------------------------------
# From Carrier Claims Records:
#     --> Visit Occurrence
#     --> Visit Cost
#     --> Procedure Occurrence
#     --> Drug Exposure
#     --> Device Exposure
#     --> Device Exposure Cost
#     --> Condition Occurrence
#     --> Measurement Occurrence
#     --> Observation
#     --> Care Site
#     --> Provider
# -----------------------------------

def process_carrier_records(beneficiary):
    drug_exp_fd = file_control.get_Descriptor('drug_exposure')
    drug_cost_fd = file_control.get_Descriptor('drug_cost')
    proc_occur_fd = file_control.get_Descriptor('procedure_occurrence')
    proc_cost_fd = file_control.get_Descriptor('procedure_cost')
    cond_occur_fd = file_control.get_Descriptor('condition_occurrence')
    death_fd = file_control.get_Descriptor('death')
    care_site_fd = file_control.get_Descriptor('care_site')
    provider_fd = file_control.get_Descriptor('provider')
    measurement_fd = file_control.get_Descriptor('measurement_occurrence')
    observation_fd = file_control.get_Descriptor('observation')
    device_fd = file_control.get_Descriptor('device_exposure')
    visit_occur_fd = file_control.get_Descriptor('visit_occurrence')
    visit_cost_fd = file_control.get_Descriptor('visit_cost')

    for raw_rec in beneficiary.carrier_records:
        rec = CarrierClaim(raw_rec)
        if rec.CLM_FROM_DT == '':
            continue

        # initialize both care_site_id and provider_id to null as some institution might not have PRVDR_NUM and some NPI might be null.
        care_site_id = ""
        provider_id  = ""
        #-- get care_site_id (a unique number generated by the program) for the given TAX_NUM
        for cc_line in rec.CarrierClaimLine_list:
            # initialize both care_site_id and provider_id to null as some institution might not have PRVDR_NUM and some NPI might be null.
            care_site_id = ''
            provider_id  = ''
            if cc_line.TAX_NUM != '':
                save_TAX_NUM = cc_line.TAX_NUM
                care_site_id = get_CareSite(cc_line.TAX_NUM)
                write_care_site(care_site_fd, care_site_id,
                               place_of_service_concept_id=OMOP_CONSTANTS.CARRIER_CLAIMS_PLACE_OF_SERVICE,
                               care_site_source_value=cc_line.TAX_NUM,
                               place_of_service_source_value=OMOP_CONSTANTS.CARRIER_CLAIMS_PLACE_OF_SERVICE_SOURCE)
            #-- get provider_id (a unique number generated by the program) for the given NPI. Each NPI will have its own provider_id
            if cc_line.PRF_PHYSN_NPI != '':
                npi = cc_line.PRF_PHYSN_NPI
                provider_id = get_Provider(npi)
                write_provider_record(provider_fd, npi, provider_id, care_site_id, cc_line.PRF_PHYSN_NPI)
        #-- get visit id. Person id + CLM_FROM_DT + CLM_THRU_DT + TAX_NUM make the key for a particular visit
        current_visit_id = visit_occurrence_ids[rec.DESYNPUF_ID,rec.CLM_FROM_DT,rec.CLM_THRU_DT,rec.TAX_NUM]
        for (vocab,code) in ([(OMOP_CONSTANTS.ICD_9_VOCAB_ID,x) for x in rec.ICD9_DGNS_CD_list] +
                             [(OMOP_CONSTANTS.HCPCS_VOCABULARY_ID, x) for x in rec.HCPCS_CD_list] +
                             [(OMOP_CONSTANTS.ICD_9_VOCAB_ID, x) for x in  rec.LINE_ICD9_DGNS_CD_list]):

            if rec.CLM_FROM_DT != '':
                if (vocab,code) in source_code_concept_dict:
                    for sccd in source_code_concept_dict[vocab,code]:
                        target_concept_id = sccd.target_concept_id
                        source_concept_id = sccd.source_concept_id
                        destination_file = sccd.destination_file

                        if destination_file == DESTINATION_FILE_PROCEDURE:
                            write_procedure_occurrence(proc_occur_fd, beneficiary.person_id,
                                                       procedure_concept_id=target_concept_id,
                                                       from_date=rec.CLM_FROM_DT,
                                                       procedure_type_concept_id=OMOP_CONSTANTS.OUTPAT_PROCEDURE_1ST_POSITION,
                                                       procedure_source_value=code,
                                                       procedure_source_concept_id=source_concept_id,
                                                       provider_id=provider_id,
                                                       modifier_concept_id=0,
                                                       visit_occurrence_id=current_visit_id)

                            #-- procedure cost. If there is an entry in procedure occurence, then only procedure cost should be updated.
                            current_procedure_occurence_id = table_ids.last_procedure_occurrence_id - 1   # after writing procedure occurence, id is increased by 1 and hence subtracted 1 to get the same id.
                            for cc_line in rec.CarrierClaimLine_list:
                                if cc_line.has_nonzero_amount():
                                    proc_cost_fd.write('{0},'.format(table_ids.last_procedure_cost_id))
                                    proc_cost_fd.write('{0},'.format(current_procedure_occurence_id))
                                    proc_cost_fd.write('{0},'.format(OMOP_CONSTANTS.CURRENCY_US_DOLLAR))               # currency_concept_id
                                    proc_cost_fd.write(',')                                                            # paid_copay
                                    proc_cost_fd.write('{0},'.format(cc_line.LINE_COINSRNC_AMT))                       # paid_coinsurance
                                    proc_cost_fd.write('{0},'.format(cc_line.LINE_BENE_PTB_DDCTBL_AMT))                # paid_toward_deductible
                                    proc_cost_fd.write('{0},'.format(cc_line.LINE_NCH_PMT_AMT))                        # paid_by_payer
                                    proc_cost_fd.write('{0},'.format(cc_line.LINE_BENE_PRMRY_PYR_PD_AMT))              # paid_by_coordination_benefits

                                    amt = 0
                                    try:
                                        amt = float(cc_line.LINE_BENE_PTB_DDCTBL_AMT) + float(cc_line.LINE_COINSRNC_AMT)
                                    except:
                                        pass
                                    proc_cost_fd.write('{0:2},'.format(amt))                                         # total_out_of_pocket

                                    proc_cost_fd.write('{0},'.format(cc_line.LINE_ALOWD_CHRG_AMT))                   # total_paid
                                    proc_cost_fd.write(',')                                                          # revenue_code_concept_id
                                    ##
                                    ## need to lookup
                                    ##
                                    proc_cost_fd.write(',')                                                          # payer_plan_period_id Changed to space as payer_plan_period file is not created
                                    #proc_cost_fd.write('')                                                          # revenue_code_source_value
                                    proc_cost_fd.write('\n')
                                    proc_cost_fd.increment_recs_written(1)
                                    table_ids.last_procedure_cost_id += 1

                        elif destination_file == DESTINATION_FILE_CONDITION:
                            write_condition_occurrence(cond_occur_fd,beneficiary.person_id,
                                                       condition_concept_id=target_concept_id,
                                                       from_date=rec.CLM_FROM_DT, thru_date=rec.CLM_THRU_DT,
                                                       condition_type_concept_id=OMOP_CONSTANTS.OUTPAT_CONDITION_1ST_POSITION,
                                                       condition_source_value=code,
                                                       condition_source_concept_id=source_concept_id,
                                                       provider_id=provider_id,
                                                       visit_occurrence_id=current_visit_id)

                        elif destination_file == DESTINATION_FILE_DRUG:
                            write_drug_exposure(drug_exp_fd, beneficiary.person_id,
                                                drug_concept_id=target_concept_id,
                                                start_date=rec.CLM_FROM_DT,
                                                drug_type_concept_id=OMOP_CONSTANTS.DRUG_TYPE_PRESCRIPTION,
                                                quantity=None,
                                                days_supply=None,
                                                drug_source_value=code,
                                                drug_source_concept_id=source_concept_id,
                                                provider_id=provider_id,
                                                visit_occurrence_id=current_visit_id)

                        elif destination_file == DESTINATION_FILE_MEASUREMENT:
                            write_measurement(measurement_fd, beneficiary.person_id,
                                              measurement_concept_id=target_concept_id,
                                              measurement_date=rec.CLM_FROM_DT,
                                              measurement_type_concept_id=OMOP_CONSTANTS.MEASUREMENT_DERIVED_VALUE,
                                              measurement_source_value=code,
                                              measurement_source_concept_id=source_concept_id,
                                              provider_id=provider_id,
                                              visit_occurrence_id=current_visit_id)

                        elif destination_file == DESTINATION_FILE_OBSERVATION:
                            write_observation(observation_fd, beneficiary.person_id,
                                              observation_concept_id=target_concept_id,
                                              observation_date=rec.CLM_FROM_DT,
                                              observation_type_concept_id=OMOP_CONSTANTS.OBSERVATION_CHIEF_COMPLAINT,
                                              observation_source_value=code,
                                              observation_source_concept_id=source_concept_id,
                                              provider_id=provider_id,         #
                                              visit_occurrence_id=current_visit_id)

                        elif destination_file == DESTINATION_FILE_DEVICE:
                            write_device_exposure(device_fd, beneficiary.person_id,
                                                  device_concept_id=target_concept_id,
                                                  start_date=rec.CLM_FROM_DT,
                                                  end_date=rec.CLM_THRU_DT,
                                                  device_type_concept_id=OMOP_CONSTANTS.DEVICE_INFERRED_PROCEDURE_CLAIM,
                                                  device_source_value=code,
                                                  device_source_concept_id=source_concept_id,
                                                  provider_id=provider_id,
                                                  visit_occurrence_id=current_visit_id)

                #-- Write each unique visit to visit_occurrence file.
                    if current_visit_id not in visit_id_list:
                        write_visit_occurrence(visit_occur_fd,beneficiary.person_id,
                                                   visit_concept_id=OMOP_CONSTANTS.CARRIER_CLAIMS_VISIT_CONCEPT_ID,
                                                   from_date=rec.CLM_FROM_DT, thru_date=rec.CLM_THRU_DT,
                                                   visit_type_concept_id=OMOP_CONSTANTS.CARRIER_CLAIMS_VISIT_1ST_POSITION,
                                                   visit_source_value=rec.CLM_ID,
                                                   visit_source_concept_id=source_concept_id,
                                                   care_site_id=care_site_id,
                                                   provider_id=provider_id,
                                                   visit_occurrence_id=current_visit_id)
                        visit_id_list.add(current_visit_id)

                else:
                    dfile = 'CarrierClaim--- unmapped ' + str(vocab) + ' code: ' + str(code) + ' DESYNPUF_ID: ' + rec.DESYNPUF_ID + '\n'
                    unmapped_log.write(dfile)

#---------------------------------
def write_header_records():
    headers = {
        'person' :
            'person_id,gender_concept_id,year_of_birth,month_of_birth,day_of_birth,time_of_birth,race_concept_id,ethnicity_concept_id,'
            'location_id,provider_id,care_site_id,person_source_value,gender_source_value,gender_source_concept_id,race_source_value,'
            'race_source_concept_id,ethnicity_source_value,ethnicity_source_concept_id',

        'observation':
            'observation_id,person_id,observation_concept_id,observation_date,observation_time,observation_type_concept_id,value_as_number,'
            'value_as_string,value_as_concept_id,qualifier_concept_id,unit_concept_id,provider_id,visit_occurrence_id,observation_source_value,'
            'observation_source_concept_id,unit_source_value,qualifier_source_value',

        'observation_period':
            'observation_period_id,person_id,observation_period_start_date,observation_period_end_date,period_type_concept_id',

        'specimen':
            'specimen_id,person_id,specimen_concept_id,specimen_type_concept_id,specimen_date,specimen_time,quantity,'
            'unit_concept_id,anatomic_site_concept_id,disease_status_concept_id,specimen_source_id,specimen_source_value,unit_source_value,'
            'anatomic_site_source_value,disease_status_source_value',

        'death':
            'person_id,death_date,death_type_concept_id,cause_concept_id,cause_source_value,cause_source_concept_id',

        'visit_occurrence':
            'visit_occurrence_id,person_id,visit_concept_id,visit_start_date,visit_start_time,visit_end_date,visit_end_time,'
            'visit_type_concept_id,provider_id,care_site_id,visit_source_value,visit_source_concept_id',

        'visit_cost':
            'visit_cost_id,visit_occurrence_id,currency_concept_id,paid_copay,paid_coinsurance,paid_toward_deductible,'
            'paid_by_payer,paid_by_coordination_benefits,total_out_of_pocket,total_paid,payer_plan_period_id',

        'condition_occurrence':
            'condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_end_date,condition_type_concept_id,'
            'stop_reason,provider_id,visit_occurrence_id,condition_source_value,condition_source_concept_id',

        'procedure_occurrence':
            'procedure_occurrence_id,person_id,procedure_concept_id,procedure_date,procedure_type_concept_id,modifier_concept_id,'
            'quantity,provider_id,visit_occurrence_id,procedure_source_value,procedure_source_concept_id,qualifier_source_value',

        'procedure_cost':
            'procedure_cost_id,procedure_occurrence_id,currency_concept_id,paid_copay,paid_coinsurance,paid_toward_deductible,'
            'paid_by_payer,paid_by_coordination_benefits,total_out_of_pocket,total_paid,revenue_code_concept_id,payer_plan_period_id,revenue_code_source_value',

        'drug_exposure':
            'drug_exposure_id,person_id,drug_concept_id,drug_exposure_start_date,drug_exposure_end_date,drug_type_concept_id,'
            'stop_reason,refills,quantity,days_supply,sig,route_concept_id,effective_drug_dose,dose_unit_concept_id,'
            'lot_number,provider_id,visit_occurrence_id,drug_source_value,drug_source_concept_id,route_source_value,dose_unit_source_value',

        'drug_cost':
            'drug_cost_id,drug_exposure_id,currency_concept_id,paid_copay,paid_coinsurance,paid_toward_deductible,paid_by_payer,paid_by_coordination_of_benefits,'
            'total_out_of_pocket,total_paid,ingredient_cost,dispensing_fee,average_wholesale_price,payer_plan_period_id',

        'device_exposure':
            'device_exposure_id,person_id,device_concept_id,device_exposure_start_date,device_exposure_end_date,device_type_concept_id,'
            'unique_device_id,quantity,provider_id,visit_occurrence_id,device_source_value,device_source_concept_id',

        'device_cost':
            'device_cost_id,device_exposure_id,currency_concept_id,paid_copay,paid_coinsurance,paid_toward_deductible,'
            'paid_by_payer,paid_by_coordination_benefits,total_out_of_pocket,total_paid,payer_plan_period_id',

        'measurement_occurrence':
            'measurement_id,person_id,measurement_concept_id,measurement_date,measurement_time,measurement_type_concept_id,operator_concept_id,'
            'value_as_number,value_as_concept_id,unit_concept_id,range_low,range_high,provider_id,visit_occurrence_id,measurement_source_value,'
            'measurement_source_concept_id,unit_source_value,value_source_value',

        'location':
            'location_id,address_1,address_2,city,state,zip,county,location_source_value',

        'care_site':
            'care_site_id,care_site_name,place_of_service_concept_id,location_id,care_site_source_value,place_of_service_source_value',

        'provider':
            'provider_id,provider_name,NPI,DEA,specialty_concept_id,care_site_id,year_of_birth,gender_concept_id,provider_source_value,'
            'specialty_source_value,specialty_source_concept_id,gender_source_value,gender_source_concept_id',

        'payer_plan_period':
            'payer_plan_period_id,person_id,payer_plan_period_start_date,payer_plan_period_end_date,payer_source_value,'
            'plan_source_value,family_source_value',
    }

    for token in sorted(file_control.descriptor_list(which='output')):
        fd = file_control.get_Descriptor(token)
        fd.write(headers[token] + '\n')
        fd.increment_recs_written(1)


#---------------------------------
#Dead code
#---------------------------------
'''
def dump_beneficiary_records(fout, rec):
    fout.write('-'*80+'\n')
    for rec in ben.carrier_records:

        fout.write('[carrier] {0}\n'.format(rec))
        cc = CarrierClaim(rec)
        fout.write('[CarrierClaim]\n')
        fout.write('\t CLM_ID       ={0}\n'.format(cc.CLM_ID))
        fout.write('\t CLM_FROM_DT  ={0}\n'.format(cc.CLM_FROM_DT))
        fout.write('\t CLM_THRU_DT  ={0}\n'.format(cc.CLM_THRU_DT))
        for cd in cc.ICD9_DGNS_CD_list:
            fout.write('\t\t {0} \n'.format(cd))
        for ix,line in enumerate(cc.CarrierClaimLine_list):
            fout.write('\t\t' + str(ix) + ' ' + '-'*30+'\n')
            fout.write('\t\t PRF_PHYSN_NPI              ={0} \n'.format(line.PRF_PHYSN_NPI))
            fout.write('\t\t TAX_NUM                    ={0} \n'.format(line.TAX_NUM))
            fout.write('\t\t HCPCS_CD                   ={0} \n'.format(line.HCPCS_CD))
            fout.write('\t\t LINE_NCH_PMT_AMT           ={0} \n'.format(line.LINE_NCH_PMT_AMT))
            fout.write('\t\t LINE_BENE_PTB_DDCTBL_AMT   ={0} \n'.format(line.LINE_BENE_PTB_DDCTBL_AMT))
            fout.write('\t\t LINE_BENE_PRMRY_PYR_PD_AMT ={0} \n'.format(line.LINE_BENE_PRMRY_PYR_PD_AMT))
            fout.write('\t\t LINE_COINSRNC_AMT          ={0} \n'.format(line.LINE_COINSRNC_AMT))
            fout.write('\t\t LINE_ALOWD_CHRG_AMT        ={0} \n'.format(line.LINE_ALOWD_CHRG_AMT))
            fout.write('\t\t LINE_PRCSG_IND_CD          ={0} \n'.format(line.LINE_PRCSG_IND_CD))
            fout.write('\t\t LINE_ICD9_DGNS_CD          ={0} \n'.format(line.LINE_ICD9_DGNS_CD))

    for rec in ben.inpatient_records:
        fout.write('[inpatient] {0}\n'.format(rec))
        ip = InpatientClaim(rec)
        fout.write('[InpatientClaim]\n')
        fout.write('\t CLM_ID       ={0}\n'.format(ip.CLM_ID))
        fout.write('\t SEGMENT      ={0}\n'.format(ip.SEGMENT))
        fout.write('\t CLM_FROM_DT  ={0}\n'.format(ip.CLM_FROM_DT))
        fout.write('\t ICD9_DGNS_CD_list \n')
        for cd in ip.ICD9_DGNS_CD_list:
            fout.write('\t\t {0} \n'.format(cd))

    for rec in ben.outpatient_records:
        fout.write('[outpatient] {0}\n'.format(rec))
        op = OutpatientClaim(rec)
        fout.write('[OutpatientClaim]\n')
        fout.write('\t CLM_ID       ={0}\n'.format(op.CLM_ID))
        fout.write('\t SEGMENT      ={0}\n'.format(op.SEGMENT))
        fout.write('\t CLM_FROM_DT  ={0}\n'.format(op.CLM_FROM_DT))
        fout.write('\t ICD9_DGNS_CD_list \n')
        for cd in op.ICD9_DGNS_CD_list:
            fout.write('\t\t {0} \n'.format(cd))

    for rec in ben.prescription_records:
        fout.write('[prescription] {0}\n'.format(rec))
        rx = PrescriptionDrug(rec)
        fout.write('[PrescriptionDrug]\n')
        fout.write('\t PDE_ID           ={0}\n'.format(rx.PDE_ID))
        fout.write('\t SRVC_DT          ={0}\n'.format(rx.SRVC_DT))
        fout.write('\t PROD_SRVC_ID     ={0}\n'.format(rx.PROD_SRVC_ID))
        fout.write('\t QTY_DSPNSD_NUM   ={0}\n'.format(rx.QTY_DSPNSD_NUM))
        fout.write('\t DAYS_SUPLY_NUM   ={0}\n'.format(rx.DAYS_SUPLY_NUM))
        fout.write('\t PTNT_PAY_AMT     ={0}\n'.format(rx.PTNT_PAY_AMT))
        fout.write('\t TOT_RX_CST_AMT   ={0}\n'.format(rx.TOT_RX_CST_AMT))
'''


def process_beneficiary(bene):
    bene.LoadClaimData(file_control)
    write_person_record(bene)
    write_payer_plan_period_record(bene)
    write_location_record(bene)
    determine_visits(bene)
    write_observation_period_records(bene)
    write_death_records(file_control.get_Descriptor('death'), bene,
                        death_type_concept_id=OMOP_CONSTANTS.DEATH_TYPE_PAYER_ENR_STATUS,
                        cause_source_concept_id=0)

    write_drug_records(bene)
    process_inpatient_records(bene)
    process_outpatient_records(bene)
    process_carrier_records(bene)
    file_control.flush_all()

#---------------------------------
#Dead code
#---------------------------------
'''
def dump_source_concept_codes():
    rec_types = {'icd9':0, 'icd9proc':0, 'hcpcs':0, 'cpt':0, 'ndc':0}
    recs_in = recs_out = 0
    code_file_out = os.path.join(BASE_OUTPUT_DIRECTORY, 'codes_1.txt')

    icd9_codes = {}
    hcpcs_codes = {}
    cpt_codes = {}
    ndc_codes = {}

    with open(code_file_out, 'w') as fout_codes:
        def write_code_rec(DESYNPUF_ID, record_number, record_type, code_type, code_value):
            fout_codes.write("{0},{1},{2},{3},{4}\n".format(DESYNPUF_ID, record_number, record_type, code_type, code_value))
            rec_types[code_type] += 1

        def check_carrier_claims():
            global recs_in
            global recs_out
            with open('/Data/OHDSI/CMS_SynPuf/DE1_1/DE1_0_2008_to_2010_Carrier_Claims_Sample_1AB.csv.srt','rU') as fin:
                for raw_rec in fin:
                    recs_in += 1
                    if recs_in % 50000 == 0:
                        print 'carrier-claims, recs_in=', recs_in

                    # print '[{0}] {1}'.format(recs_in, rec[:-1])
                    # fout_codes.write('[{0}] {1}\n'.format(recs_in, raw_rec[:-1]))
                    # if recs_in > 100: break
                    if "DESYNPUF_ID" in raw_rec: continue

                    rec = CarrierClaim((raw_rec[:-1]).split(','))
                    for src_code in rec.ICD9_DGNS_CD_list:
                        if src_code in icd9_codes:
                            icd9_codes[src_code] += 1
                        else:
                            icd9_codes[src_code] = 1
                            fout_codes.write("{0},{1},cc,icd9-1,{2}\n".format(rec.DESYNPUF_ID, recs_in, src_code))
                            recs_out += 1
                            rec_types['icd9'] += 1

                    for src_code in rec.HCPCS_CD_list:
                        if src_code in hcpcs_codes:
                            hcpcs_codes[src_code] += 1
                        else:
                            hcpcs_codes[src_code] = 1
                            fout_codes.write("{0},{1},cc,hcpcs,{2}\n".format(rec.DESYNPUF_ID, recs_in, src_code))
                            recs_out += 1
                            rec_types['hcpcs'] += 1

                    for src_code in rec.LINE_ICD9_DGNS_CD_list:
                        if src_code in icd9_codes:
                            icd9_codes[src_code] += 1
                        else:
                            icd9_codes[src_code] = 1
                            fout_codes.write("{0},{1},cc,icd9,{2}\n".format(rec.DESYNPUF_ID, recs_in, src_code))
                            recs_out += 1
                            rec_types['icd9'] += 1
            fout_codes.flush()

        def check_inpatient_claims():
            global recs_in
            global recs_out
            with open('/Data/OHDSI/CMS_SynPuf/DE1_1/DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv','rU') as fin:
                record_type = 'ip'
                for raw_rec in fin:
                    recs_in += 1
                    if recs_in % 10000 == 0:
                        print 'inpatient-claims, recs_in=', recs_in

                    # print '[{0}] {1}'.format(recs_in, rec[:-1])
                    # fout_codes.write('[{0}] {1}\n'.format(recs_in, raw_rec[:-1]))
                    # if recs_in > 100: break
                    if "DESYNPUF_ID" in raw_rec: continue

                    rec = InpatientClaim((raw_rec[:-1]).split(','))
                    for src_code in rec.ICD9_DGNS_CD_list:
                        if src_code in icd9_codes:
                            icd9_codes[src_code] += 1
                        else:
                            icd9_codes[src_code] = 1
                            write_code_rec(rec.DESYNPUF_ID, recs_in, record_type, code_type='icd9', code_value=src_code)
                            recs_out += 1

                    for src_code in rec.HCPCS_CD_list:
                        if src_code in hcpcs_codes:
                            hcpcs_codes[src_code] += 1
                        else:
                            hcpcs_codes[src_code] = 1
                            write_code_rec(rec.DESYNPUF_ID, recs_in, record_type, code_type='hcpcs', code_value=src_code)
                            recs_out += 1

                    for src_code in rec.ICD9_PRCDR_CD_list:
                        if src_code in icd9_codes:
                            icd9_codes[src_code] += 1
                        else:
                            icd9_codes[src_code] = 1
                            write_code_rec(rec.DESYNPUF_ID, recs_in, record_type, code_type='icd9proc', code_value=src_code)
                            recs_out += 1

        def check_outpatient_claims():
            global recs_in
            global recs_out
            with open('/Data/OHDSI/CMS_SynPuf/DE1_1/DE1_0_2008_to_2010_Outpatient_Claims_Sample_1.csv','rU') as fin:
                record_type = 'op'
                for raw_rec in fin:
                    recs_in += 1
                    if recs_in % 10000 == 0:
                        print 'outpatient-claims, recs_in=', recs_in

                    # print '[{0}] {1}'.format(recs_in, rec[:-1])
                    # fout_codes.write('[{0}] {1}\n'.format(recs_in, raw_rec[:-1]))
                    # if recs_in > 100: break
                    if "DESYNPUF_ID" in raw_rec: continue

                    rec = OutpatientClaim((raw_rec[:-1]).split(','))
                    for src_code in rec.ICD9_DGNS_CD_list:
                        if src_code in icd9_codes:
                            icd9_codes[src_code] += 1
                        else:
                            icd9_codes[src_code] = 1
                            write_code_rec(rec.DESYNPUF_ID, recs_in, record_type, code_type='icd9', code_value=src_code)
                            recs_out += 1

                    for src_code in rec.HCPCS_CD_list:
                        if src_code in hcpcs_codes:
                            hcpcs_codes[src_code] += 1
                        else:
                            hcpcs_codes[src_code] = 1
                            write_code_rec(rec.DESYNPUF_ID, recs_in, record_type, code_type='hcpcs', code_value=src_code)
                            recs_out += 1

                    for src_code in rec.ICD9_PRCDR_CD_list:
                        if src_code in icd9_codes:
                            icd9_codes[src_code] += 1
                        else:
                            icd9_codes[src_code] = 1
                            write_code_rec(rec.DESYNPUF_ID, recs_in, record_type, code_type='icd9proc', code_value=src_code)
                            recs_out += 1

                    if len(rec.ADMTNG_ICD9_DGNS_CD) > 0:
                        src_code = rec.ADMTNG_ICD9_DGNS_CD
                        if src_code in icd9_codes:
                            icd9_codes[src_code] += 1
                        else:
                            icd9_codes[src_code] = 1
                            write_code_rec(rec.DESYNPUF_ID, recs_in, record_type, code_type='icd9', code_value=src_code)
                            recs_out += 1

        def check_prescription_drug():
            global recs_in
            global recs_out
            with open('/Data/OHDSI/CMS_SynPuf/DE1_1/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_1.csv','rU') as fin:
                record_type = 'rx'
                for raw_rec in fin:
                    recs_in += 1
                    if recs_in % 10000 == 0:
                        print 'prescription-drugs, recs_in=', recs_in

                    # print '[{0}] {1}'.format(recs_in, rec[:-1])
                    # fout_codes.write('[{0}] {1}\n'.format(recs_in, raw_rec[:-1]))
                    # if recs_in > 100: break
                    if "DESYNPUF_ID" in raw_rec: continue

                    rec = PrescriptionDrug((raw_rec[:-1]).split(','))
                    if len(rec.PROD_SRVC_ID) > 0:
                        ndc = rec.PROD_SRVC_ID
                        if ndc in ndc_codes:
                            ndc_codes[ndc] += 1
                        else:
                            ndc_codes[ndc] = 1
                            write_code_rec(rec.DESYNPUF_ID, recs_in, record_type, code_type='ndc', code_value=ndc)
                            recs_out += 1

        check_carrier_claims()
        check_inpatient_claims()
        check_outpatient_claims()
        check_prescription_drug()

    code_summary_file = os.path.join(BASE_OUTPUT_DIRECTORY, 'code_summary.txt')
    with open(code_summary_file, 'w') as fout:
        for label, dct in [ ('ndc',   ndc_codes),
                            ('hcpcs', hcpcs_codes),
                            ('cpt',   cpt_codes),
                            ('icd9',  icd9_codes)]:
            for code, recs in dct.items():
                fout.write("{0},{1},{2}\n".format(label, code, recs))

    print '--done: recs-in=',recs_in,', out=', recs_out

    for type, count in rec_types.items():
        print type,count
'''
#---------------------------------
# start of the program
#---------------------------------
if __name__ == '__main__':
    if not os.path.exists(BASE_OUTPUT_DIRECTORY): os.makedirs(BASE_OUTPUT_DIRECTORY)
    if not os.path.exists(BASE_ETL_CONTROL_DIRECTORY): os.makedirs(BASE_ETL_CONTROL_DIRECTORY)

    parser = argparse.ArgumentParser(description='Enter Sample Number')
    parser.add_argument('sample_number', type=int, default=1)
    args = parser.parse_args()
    current_sample_number = args.sample_number
    SAMPLE_RANGE = [current_sample_number]

    current_stats_filename = os.path.join(BASE_OUTPUT_DIRECTORY,'etl_stats.txt_{0}'.format(current_sample_number))
    if os.path.exists(current_stats_filename): os.unlink(current_stats_filename)

    log_stats('CMS_ETL starting')
    log_stats('BASE_SYNPUF_INPUT_DIRECTORY     =' + BASE_SYNPUF_INPUT_DIRECTORY)
    log_stats('BASE_OUTPUT_DIRECTORY           =' + BASE_OUTPUT_DIRECTORY)
    log_stats('BASE_ETL_CONTROL_DIRECTORY      =' + BASE_ETL_CONTROL_DIRECTORY)

    file_control = FileControl(BASE_SYNPUF_INPUT_DIRECTORY, BASE_OUTPUT_DIRECTORY, SYNPUF_DIR_FORMAT, current_sample_number)
    file_control.delete_all_output()

    print '-'*80
    print '-- all files present....'
    print '-'*80

    #Set up initial identifier counters
    table_ids = Table_ID_Values()
    table_ids_filename = os.path.join(BASE_ETL_CONTROL_DIRECTORY, 'etl_synpuf_last_table_ids.txt')
    if os.path.exists(table_ids_filename):
        table_ids.Load(table_ids_filename, log_stats)
    # Build mappings between SynPUF codes and OMOP Vocabulary concept_ids
    build_maps()
    bene_dump_filename = os.path.join(BASE_OUTPUT_DIRECTORY,'beneficiary_dump_{0}.txt'.format(current_sample_number))
    omop_unmapped_code_file = os.path.join(BASE_ETL_CONTROL_DIRECTORY,'unmapped_code_log.txt')
    unmapped_log = open(omop_unmapped_code_file, 'a+')

    # Build the object to manage access to all the files
    write_header_records()

    with open(bene_dump_filename,'w') as fout:
        beneficiary_fd = file_control.get_Descriptor('beneficiary')

        log_stats('-'*80)
        log_stats('reading beneficiary file -> '+ beneficiary_fd.complete_pathname)
        log_stats('last_person_id starting value   -> ' + str(table_ids.last_person_id))

        recs_in = 0
        rec = ''
        save_DESYNPUF_ID = ''
        unique_DESYNPUF_ID_count = 0
        bene = None
        try:
            with beneficiary_fd.open() as fin:
                # Skip header record
                rec = fin.readline()

                for rec in fin:
                    recs_in += 1
                    if recs_in % 10000 == 0: print 'beneficiary recs_in: ', recs_in

                    rec = rec.split(',')
                    DESYNPUF_ID = rec[BENEFICIARY_SUMMARY_RECORD.DESYNPUF_ID]
                    SP_STATE_CODE = rec[BENEFICIARY_SUMMARY_RECORD.SP_STATE_CODE]
                    BENE_COUNTY_CD = rec[BENEFICIARY_SUMMARY_RECORD.BENE_COUNTY_CD]
                    # count on this header record field being in every file
                    if '"DESYNPUF_ID"' in rec:
                        continue

                    # check for bene break
                    if DESYNPUF_ID != save_DESYNPUF_ID:
                        if not bene is None:
                            process_beneficiary(bene)

                        unique_DESYNPUF_ID_count += 1
                        save_DESYNPUF_ID = DESYNPUF_ID
                        bene = Beneficiary(DESYNPUF_ID, table_ids.last_person_id, SP_STATE_CODE, BENE_COUNTY_CD)
                        table_ids.last_person_id += 1

                    #accumulate for the current bene
                    bene.AddYearData(rec)

                if not bene is None:
                    process_beneficiary(bene)

        except BaseException:
            print '** ERROR reading beneficiary file, record number ', recs_in, '\n record-> ', rec
            raise

        beneficiary_fd.increment_recs_read(recs_in)
        log_stats('last_person_id ending value -> ' + str(table_ids.last_person_id))
        log_stats('Done: total records read ={0}, unique IDs={1}'.format(recs_in, unique_DESYNPUF_ID_count))

    file_control.close_all()

    #- save look up tables & last-used-ids
    persist_lookup_tables()
    table_ids.Save(table_ids_filename)

    log_stats('CMS_ETL done')
    log_stats('Input Records------')
    for token in sorted(file_control.descriptor_list(which='input')):
        fd = file_control.get_Descriptor(token)
        log_stats('\tFile: {0:50}, records_read={1:10}'.format(fd.token, fd.records_read))

    log_stats('Output Records------')
    for token in sorted(file_control.descriptor_list(which='output')):
        fd = file_control.get_Descriptor(token)
        if fd.records_written > 1:
            log_stats('\tFile: {0:50}, records_written={1:10}'.format(fd.token, fd.records_written))


    print '** done **'
