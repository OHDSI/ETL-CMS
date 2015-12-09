import csv,os,os.path,sys
from time import strftime
import argparse
import dotenv
import math
from constants import OMOP_CONSTANTS, OMOP_MAPPING_RECORD, BENEFICIARY_SUMMARY_RECORD, OMOP_CONCEPT_RECORD, OMOP_CONCEPT_RELATIONSHIP_RECORD
from utility_classes import Table_ID_Values
from beneficiary import Beneficiary
from FileControl import FileControl
from SynPufFiles import PrescriptionDrug, InpatientClaim, OutpatientClaim, CarrierClaim

def icd9fix(s):
    if(len(s) == 0):
        return('')
    if(s[0]=='V'):
        if(len(s) == 3):
            return(s)
        return(s[0:3] + "." + s[3:])
    if(s[0]=='E'):
        if(len(s) == 4):
            return(s)
        return(s[0:4] + "." + s[4:])
    if(len(s) == 3):
        return(s)
    if(len(s) >= 4):
        return(s[0:3] + "." + s[3:])
    return('')

# ------------------------
# TODO: finish enough for backend testing
# TODO: refine& correct logic
# TODO: polish for updating to OHDSI (doc strings, testing, comments, pylint, etc)
#
# ------------------------

# ------------------------
# This python script creates the OMOP CDM v5 tables from the CMS SynPuf (Synthetic Public Use Files).
# ------------------------
#
#  Input Required:
#       OMOP Vocabulary v5 Concept  file
#           BASE_OMOP_INPUT_DIRECTORY    /  'CONCEPT.csv'
#
#       SynPuf data files
#           BASE_SYNPUF_INPUT_DIRECTORY
#                             /  DE1_<sample_number>
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
#     Lookup tables in pickle format:
#           BASE_SYNPUF_INPUT_DIRECTORY  /  icd_9_map_proc_decimals.pck
#           BASE_SYNPUF_INPUT_DIRECTORY  /  ndc_dict.pck
#           BASE_SYNPUF_INPUT_DIRECTORY  /  icd_9_proc_dict.pck
#           BASE_SYNPUF_INPUT_DIRECTORY  /  icd_9_diag_dict.pck
#
#     Last-used concept_IDs for CDM v5 tables
#           BASE_OUTPUT_DIRECTORY       /  etl_synpuf_last_table_ids.txt
#
#     SynPuf Beneficiary Files with year prefix
#           BASE_SYNPUF_INPUT_DIRECTORY
#                                /  DE1_<sample_number> /
#                                /  DE1_0_comb_Beneficiary_Summary_File_Sample_<sample_number>.csv
#
#     OMOP CDM v5 Tables
#           BASE_OUTPUT_DIRECTORY       /  DE1_<sample_number>_person.csv
#                                          DE1_<sample_number>_observation_period.csv
#                                          DE1_<sample_number>_death.csv
#                                          DE1_<sample_number>_drug_exposure.csv
#                                          DE1_<sample_number>_condition_occurrence.csv
#                                          DE1_<sample_number>_procedure_occurrence.csv
#
#                                       ** Various debug and log files
#
# ------------------------

# ------------------------
#  2015-02-05  C. Dougherty         Created
# ------------------------

dotenv.load_dotenv(".env")

# -----------------------------------
# - Configuration
# -----------------------------------
# ---------------------------------

# Edit your .env file to change which directories to use in the ETL process

# Maximum number of records to be processed before quitting
MAX_RECS                        = int(os.environ['MAX_RECS']) or 100000000

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
# beneficiary_dict = {}
cdm_counts_dict = {}

vocabulary_id_list = {}
source_code_concept_dict = {}
concept_relationship_dict = {}
current_stats_filename = ''

icd9_codes_death = ['7616', '798', '7980', '7981', '7982','7989', 'E9131', 'E978']

next_care_site_id = 0
provider_id_care_site_id = {}

next_provider_id = 0
npi_provider_id = {}

domain_destination_file_list = {
    'Condition'             : DESTINATION_FILE_CONDITION,
    'Condition/Meas'        : DESTINATION_FILE_MEASUREMENT,
    'Condition/Obs'         : DESTINATION_FILE_OBSERVATION,
    'Condition/Procedure'   : DESTINATION_FILE_PROCEDURE,
    'Device'                : DESTINATION_FILE_DEVICE,
    'Device/Obs'            : DESTINATION_FILE_OBSERVATION,
    'Device/Procedure'      : DESTINATION_FILE_PROCEDURE,
    'Measurement'           : DESTINATION_FILE_MEASUREMENT,
    'Obs/Procedure'         : DESTINATION_FILE_PROCEDURE,
    'Observation'           : DESTINATION_FILE_OBSERVATION,
    'Procedure'             : DESTINATION_FILE_PROCEDURE
    }

# -----------------------------------
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
# -----------------------------------
def get_date_YYYY_MM_DD(date_YYYYMMDD):
    if len(date_YYYYMMDD) == 0:
        return ''
    return '{0}-{1}-{2}'.format(date_YYYYMMDD[0:4], date_YYYYMMDD[4:6], date_YYYYMMDD[6:8])

# -----------------------------------
# -----------------------------------
def get_CareSite(provider_num):
    global next_care_site_id
    if provider_num not in provider_id_care_site_id:
        next_care_site_id += 1
        provider_id_care_site_id[provider_num] = next_care_site_id
    return provider_id_care_site_id[provider_num]

# -----------------------------------
# -----------------------------------
def get_Provider(npi):
    global next_provider_id
    if npi not in npi_provider_id:
        next_provider_id += 1
        npi_provider_id[npi] = next_provider_id
    return npi_provider_id[npi]

# -----------------------------------
# This function produces dictionaries that give mappings between SynPUF codes and OMOP concept_ids
# -----------------------------------
def build_maps():
    log_stats('-'*80)
    log_stats('build_maps starting...')

    #----------------
    # load existing provider_id_care_site_id
    #----------------
    recs_in = 0

    global next_care_site_id
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
                    provider_id_care_site_id[provider_num] = care_site_id
                    if care_site_id > next_care_site_id: next_care_site_id = int(care_site_id)
        log_stats('done, recs_in={0}, len provider_id_care_site_id={1}'.format(recs_in, len(provider_id_care_site_id)))
    else:
        log_stats('No existing provider_id_care_site_file found (looked for ->' + provider_id_care_site_file + ')')

    #----------------
    # load existing npi_provider_id
    #----------------
    recs_in = 0

    global next_provider_id
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
                    npi_provider_id[npi] = provider_id
                    if provider_id > next_provider_id: next_provider_id = int(provider_id)
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

    #The HCPCS vocabulary is a combination of HCPCS codes from CONCEPTS.csv and CPT4 codes from the following file
    omop_concept_cpt4_debug_file = os.path.join(BASE_OUTPUT_DIRECTORY,'concept_debug_cpt4_log.txt')
    omop_concept_cpt4_file = os.path.join(BASE_OMOP_INPUT_DIRECTORY,'concept_cpt4.csv')
    
    # use the mini version with the concepts we want pre-selected
    # omop_concept_file = os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT_mini.csv')
    
    # omop_concept_file_mini = os.path.join(BASE_OUTPUT_DIRECTORY,'CONCEPT_mini.csv')
    

    global vocabulary_id_list


    recs_in = 0
    recs_skipped = 0

    log_stats('Reading omop_concept_relationship_file   -> ' + omop_concept_relationship_file)
    log_stats('Writing to log file              -> ' + omop_concept_relationship_debug_file)

    with open(omop_concept_relationship_file,'r') as fin, \
         open(omop_concept_relationship_debug_file, 'w') as fout_log:
        for rec in fin:
            recs_in += 1
            if recs_in % 100000 == 0: print 'omop concept relationship recs=',recs_in
            flds = (rec[:-1]).split('\t')
            if len(flds) == OMOP_CONCEPT_RELATIONSHIP_RECORD.fieldCount:
                concept_id1 = flds[OMOP_CONCEPT_RELATIONSHIP_RECORD.CONCEPT_ID_1]
                concept_id2 = flds[OMOP_CONCEPT_RELATIONSHIP_RECORD.CONCEPT_ID_2]
                relationship_id = flds[OMOP_CONCEPT_RELATIONSHIP_RECORD.RELATIONSHIP_ID]
                invalid_reason = flds[OMOP_CONCEPT_RELATIONSHIP_RECORD.INVALID_REASON]
    
                if concept_id1 != '' and concept_id2 != '' and relationship_id == "Maps to": # and invalid_reason == '':
                    concept_relationship_dict[concept_id1] = concept_id2
                else:
                    recs_skipped = recs_skipped + 1

        log_stats('Done, omop concept recs_in            = ' + str(recs_in))
        log_stats('recs_skipped                          = ' + str(recs_skipped))
        log_stats('len source_code_concept_dict           = ' + str(len(source_code_concept_dict)))


    recs_in = 0
    recs_skipped = 0
    merged_recs=0
    recs_checked=0

    log_stats('Reading omop_concept_cpt4_file   -> ' + omop_concept_file)
    log_stats('Writing to log file              -> ' + omop_concept_cpt4_debug_file)
    #TODO: there is an overlap of 41 2-character codes that are the same between CPT4 and HCPCS,
    #but map to different OMOP concepts. Need to determine which should prevail. Whichever prevails should call one of the next 2 code blocks first.

    #Read the CPT4 codes which are kept in a separate file, and add them to the HCPCS vocabulary
    with open(omop_concept_cpt4_file,'r') as fin, \
         open(omop_concept_cpt4_debug_file, 'w') as fout_log:
        for rec in fin:
            recs_in += 1
            if recs_in % 100000 == 0: print 'omop concept cpt4 recs=',recs_in
            flds = (rec[:-1]).split('\t')
            if len(flds) == OMOP_CONCEPT_RECORD.fieldCount:
                concept_id = flds[OMOP_CONCEPT_RECORD.CONCEPT_ID]
                concept_code = original_concept_code = flds[OMOP_CONCEPT_RECORD.CONCEPT_CODE]
                #concept_name  = flds[OMOP_CONCEPT_RECORD.CONCEPT_NAME]
                vocabulary_id = flds[OMOP_CONCEPT_RECORD.VOCABULARY_ID]
                domain_id = flds[OMOP_CONCEPT_RECORD.DOMAIN_ID]
                invalid_reason = flds[OMOP_CONCEPT_RECORD.INVALID_REASON]
    
                status = ''
                if concept_id != '':
                    if vocabulary_id in [OMOP_CONSTANTS.CPT4_VOCABULARY_ID]:
                        vocabulary_id = OMOP_CONSTANTS.HCPCS_VOCABULARY_ID
                        recs_checked += 1
                        if  not concept_relationship_dict.has_key(concept_id):
                            status = "No self-map from OMOP (CPT4) to OMOP (CPT4) [adding] for " + str(concept_id)
                            concept_relationship_dict[concept_id] = concept_id
                        elif len(concept_code) == 5:
                            if vocabulary_id not in vocabulary_id_list: vocabulary_id_list[vocabulary_id] =0
                            vocabulary_id_list[vocabulary_id] += 1
                            if domain_id in domain_destination_file_list:
                                destination_file = domain_destination_file_list[domain_id]
                            if (vocabulary_id, concept_code) not in source_code_concept_dict:
                                source_code_concept_dict[vocabulary_id,concept_code] = SourceCodeConcept(concept_code, original_concept_code, concept_relationship_dict[concept_id], destination_file)
                        else:
                            recs_skipped += 1
                            status = 'CPT4 concept code not 5 digits'
                if status != '':
                    fout_log.write(status + ': \t')
                    # for fld in line: fout_log.write(fld + '\t')
                    fout_log.write(rec + '\n')

        log_stats('Done, omop concept recs_in            = ' + str(recs_in))
        log_stats('recs_checked                          = ' + str(recs_checked))
        log_stats('recs_skipped                          = ' + str(recs_skipped))
        log_stats('merged_recs                           = ' + str(merged_recs))
        log_stats('len source_code_concept_dict           = ' + str(len(source_code_concept_dict)))


    log_stats('Reading omop_concept_file        -> ' + omop_concept_file)
    log_stats('Writing to log file              -> ' + omop_concept_debug_file)
        
    with open(omop_concept_file,'r') as fin, \
         open(omop_concept_debug_file, 'w') as fout_log:
         # open(omop_concept_file_mini, 'w') as fout_mini:
        for rec in fin:
            recs_in += 1
            # if recs_in > 5000:break
            if recs_in % 100000 == 0: print 'omop concept recs=',recs_in
            flds = (rec[:-1]).split('\t')
    
            if len(flds) == OMOP_CONCEPT_RECORD.fieldCount:
                concept_id = flds[OMOP_CONCEPT_RECORD.CONCEPT_ID]
                concept_code = original_concept_code = flds[OMOP_CONCEPT_RECORD.CONCEPT_CODE]
                #concept_name  = flds[OMOP_CONCEPT_RECORD.CONCEPT_NAME]
                vocabulary_id = flds[OMOP_CONCEPT_RECORD.VOCABULARY_ID]
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
                            recs_skipped += 1
                            if( vocabulary_id == OMOP_CONSTANTS.ICD_9_VOCAB_ID):
                                status = "No map from OMOP (ICD9) to OMOP (SNOMED) for " + str(concept_id)
                            if( vocabulary_id == OMOP_CONSTANTS.HCPCS_VOCABULARY_ID):
                                status = "No map from OMOP (HCPCS) to OMOP (???) for " + str(concept_id)
                            if( vocabulary_id == OMOP_CONSTANTS.NDC_VOCABULARY_ID):
                                status = "No map from OMOP (NCD) to OMOP (RxNorm) for " + str(concept_id)
                        #elif invalid_reason != '':
                        #    recs_skipped += 1
                        #    status = 'invalid_reason not blank'
                        else:
                            if vocabulary_id not in vocabulary_id_list: vocabulary_id_list[vocabulary_id] = 0
                            vocabulary_id_list[vocabulary_id] += 1
                            # fout_mini.write(rec)
                            if vocabulary_id == OMOP_CONSTANTS.NDC_VOCABULARY_ID:
                                destination_file = DESTINATION_FILE_DRUG
                            elif domain_id in domain_destination_file_list:
                                destination_file = domain_destination_file_list[domain_id]
                            if (vocabulary_id, concept_code) not in source_code_concept_dict:
                                source_code_concept_dict[vocabulary_id,concept_code] = SourceCodeConcept(concept_code, original_concept_code, concept_relationship_dict[concept_id], destination_file)
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


    #----------------
    # Load the mapping xref file built from OMOP Vocabulary:
    #----------------
    # omop_mapping_file = os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT.csv')
    # log_stats('Reading omop_mapping_file        -> ' + omop_mapping_file)

    # global vocabulary_id_list
    # recs_in = 0

    # with open(omop_mapping_file,'rU') as fin:
    #     for rec in fin:
    #         recs_in += 1
    #         if recs_in % 100000 == 0: print 'omop mapping recs=',recs_in
    #         flds = (rec[:-1]).split('\t')
    #         destination_file = ''
    #         if len(flds) == OMOP_MAPPING_RECORD.fieldCount:
    #             source_concept_code = flds[OMOP_MAPPING_RECORD.SOURCE_CONCEPT_CODE]    
    #             source_concept_id = flds[OMOP_MAPPING_RECORD.SOURCE_CONCEPT_ID]        
    #             target_concept_id = flds[OMOP_MAPPING_RECORD.TARGET_CONCEPT_ID]        
    #             target_vocabulary_id = flds[OMOP_MAPPING_RECORD.TARGET_VOCABULARY_ID]  
    #             target_domain_id = flds[OMOP_MAPPING_RECORD.TARGET_DOMAIN_ID]          
    #             if target_vocabulary_id not in vocabulary_id_list: vocabulary_id_list[target_vocabulary_id] =0
    #             vocabulary_id_list[target_vocabulary_id] += 1
    #             if target_vocabulary_id == OMOP_CONSTANTS.NDC_VOCABULARY_ID:
    #                 destination_file = DESTINATION_FILE_DRUG
    #             elif target_domain_id in domain_destination_file_list:
    #                 destination_file = domain_destination_file_list[target_domain_id]
    #             if source_concept_code not in source_code_concept_dict:
    #                 source_code_concept_dict[source_concept_code] = SourceCodeConcept(source_concept_code, source_concept_id, target_concept_id, destination_file)

    #      for voc in sorted(vocabulary_id_list):
    #         # print voc, vocabulary_id_list[voc]
    #         log_stats('{0} \t\t {1}'.format(voc, vocabulary_id_list[voc]))

    #  log_stats('build_maps done')

    
# -----------------------------------
# -----------------------------------
def persist_lookup_tables():
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

# # -----------------------------------
# #  Load the beneficiary data
# # -----------------------------------
# def load_beneficiary_table(sample_number):
#     # global beneficiary_dict
#     beneficiary_fd = file_control.get_Descriptor('beneficiary')
#
#     log_stats('-'*80)
#     log_stats('load_beneficiary_table starting')
#     log_stats('input file -> '+ beneficiary_fd.complete_pathname)
#     log_stats('last_person_id starting value   -> ' + str(table_ids.last_person_id))
#
#     recs_in = 0
#     rec = ''
#     try:
#         with beneficiary_fd.open() as fin:
#             # don't skip 1st record since the file is now sorted
#             # rec = fin.readline()
#             for rec in fin:
#                 recs_in += 1
#                 if recs_in % 50000 == 0: print 'beneficiary recs_in: ', recs_in
#                 if recs_in > MAX_RECS:break
#                 rec = rec.split(',')
#                 DESYNPUF_ID = rec[BENEFICIARY_SUMMARY_RECORD.DESYNPUF_ID]
#                 # count on this header record field being in every file
#                 if '"DESYNPUF_ID"' in rec:
#                     continue
#                 if DESYNPUF_ID not in beneficiary_dict:
#                     bene = Beneficiary(DESYNPUF_ID, table_ids.last_person_id)
#                     beneficiary_dict[DESYNPUF_ID] = bene
#                     table_ids.last_person_id += 1
#                 bene.AddYearData(rec)
#     except BaseException:
#         print '** ERROR reading beneficiary file, record number ', recs_in, '\n record-> ', rec
#         raise
#
#     beneficiary_fd.increment_recs_read(recs_in)
#     log_stats('last_person_id ending value -> ' + str(table_ids.last_person_id))
#     log_stats('Done: total records read ={0}, unique IDs={1}'.format(recs_in, len(beneficiary_dict)))
#
# -----------------------------------
# Perform some simple data validation
# -----------------------------------
def check_beneficiary_table(sample_number):
    log_stats('-'*80)
    missing_year_count =0

    missing_years = {}
    log_stats('check_beneficiary_table starting')
    filename_log = os.path.join(BASE_OUTPUT_DIRECTORY,SYNPUF_DIR_FORMAT.format(sample_number),'{0}_beneficiary_log.txt'.format(sample_number))
    filename_log_2 = os.path.join(BASE_OUTPUT_DIRECTORY,SYNPUF_DIR_FORMAT.format(sample_number),'{0}_beneficiary_log_2.txt'.format(sample_number))
    with open(filename_log, 'w') as f_out, open(filename_log_2, 'w') as f_out_2:
        for DESYNPUF_ID in beneficiary_dict:
            ben = beneficiary_dict[DESYNPUF_ID]

            status = ''
            y_1 = None
            y_2 = None
            y_3 = None

            #--- covered years
            if '2008' in ben.year_data_list:
                y_1 = ben.year_data_list['2008']
            else:
                status += " year-2008 missing;"

            if '2009' in ben.year_data_list:
                y_2 = ben.year_data_list['2009']
            else:
                status += " year-2009 missing;"

            if '2010' in ben.year_data_list:
                y_3 = ben.year_data_list['2010']
            else:
                 status += " year-2010 missing;"

            if not (y_1 and y_2 and y_3):
                missing_year_count +=1

            if status not in missing_years: missing_years[status]=0
            missing_years[status]+=1

            #--- differences
            status = []
            if y_1 is not None and y_2 is not None: status += y_1.compare(y_2)
            if y_2 is not None and y_3 is not None: status += y_2.compare(y_3)
            for s in status:
                if s != '': f_out.write('[{0}] {1}\n'.format(ben.DESYNPUF_ID, s))

            #--- dump coverage
            f_out_2.write('[{0}] ------------- \n'.format(ben.DESYNPUF_ID))
            if y_1 is not None: f_out_2.write('\t {0}\n'.format(y_1.dump()))
            if y_2 is not None: f_out_2.write('\t {0}\n'.format(y_2.dump()))
            if y_3 is not None: f_out_2.write('\t {0}\n'.format(y_3.dump()))

            for (start_date, end_date) in ben.ObservationPeriodList():
                f_out_2.write('\t {0}\t{1}\n'.format(start_date, end_date))

    log_stats('check_beneficiary_table done')
    log_stats('missing_year_count' + str(missing_year_count))
    for k in missing_years:
        log_stats('[' + k + ']  ' + str(missing_years[k]))


# -----------------------------------
# quick-n-dirty: just identify unique dates
# TODO: Implement visit logic from specs
# -----------------------------------
def determine_visits(bene):
    # each unique date gets a visit id
    dates = set()
    for raw_rec in bene.prescription_records:
        rec = PrescriptionDrug(raw_rec)
        dates.add(rec.SRVC_DT)

    for raw_rec in bene.inpatient_records:
        rec = InpatientClaim(raw_rec)
        dates.add(rec.CLM_FROM_DT)

    for raw_rec in bene.outpatient_records:
        rec = OutpatientClaim(raw_rec)
        dates.add(rec.CLM_FROM_DT)

    for raw_rec in bene.carrier_records:
        rec = CarrierClaim(raw_rec)
        dates.add(rec.CLM_FROM_DT)

    visit_id = table_ids.last_visit_occurrence_id
    bene.visit_dates = {}
    for ix, date in enumerate(sorted(dates)):
        visit_id += ix
        bene.visit_dates[date] = visit_id
    table_ids.last_visit_occurrence_id = visit_id + 1

# -----------------------------------
# CDM v5 Person
# -----------------------------------
def write_person_record(beneficiary):
    person_fd = file_control.get_Descriptor('person')
    yd = beneficiary.LatestYearData()
    if yd is None: return

    person_fd.write('{0},'.format(beneficiary.person_id))                                   # person_id
    if int(yd.BENE_SEX_IDENT_CD) == 1:                                                      # gender_concept_id
        person_fd.write('{0},'.format(OMOP_CONSTANTS.GENDER_MALE))
    else:
        person_fd.write('{0},'.format(OMOP_CONSTANTS.GENDER_FEMALE))
    person_fd.write('{0},'.format(yd.BENE_BIRTH_DT[0:4]))                                    # year_of_birth
    person_fd.write('{0},'.format(yd.BENE_BIRTH_DT[4:6]))                                    # month_of_birth
    person_fd.write('{0},'.format(yd.BENE_BIRTH_DT[6:8]))                                    # day_of_birth
    person_fd.write(',')                                                                     # time_of_birth
    print ("yd.BENE_RACE_CD: " + str(yd.BENE_RACE_CD)) 
    if int(yd.BENE_RACE_CD) == 1:                                                            # race_concept_id and ethnicity_concept_id
        person_fd.write('{0},'.format(OMOP_CONSTANTS.RACE_WHITE))
        person_fd.write('{0},'.format(OMOP_CONSTANTS.ETHNICITY_NON_HISPANIC))
    elif int(yd.BENE_RACE_CD) == 2:
        person_fd.write('{0},'.format(OMOP_CONSTANTS.RACE_BLACK))
        person_fd.write('{0},'.format(OMOP_CONSTANTS.ETHNICITY_NON_HISPANIC))
    elif int(yd.BENE_RACE_CD) == 3:
        person_fd.write('{0},'.format(OMOP_CONSTANTS.RACE_OTHER))
        person_fd.write('{0},'.format(OMOP_CONSTANTS.ETHNICITY_NON_HISPANIC))
    else:
        person_fd.write('{0},'.format(OMOP_CONSTANTS.RACE_NON_WHITE))
        person_fd.write('{0},'.format(OMOP_CONSTANTS.ETHNICITY_HISPANIC))

    #todo: xlate bene_count_cd, sp_state_code to location-id
    person_fd.write(',')                                                                    # location_id
    person_fd.write(',')                                                                    # provider_id
    person_fd.write(',')                                                                    # care_site_id
    person_fd.write('{0},'.format(beneficiary.DESYNPUF_ID))                                             # person_source_value
    #todo: ? specs says 'xlate to human-readable text'
    person_fd.write('{0},'.format(yd.BENE_SEX_IDENT_CD))                                    # gender_source_value
    person_fd.write(',')                                                                    # gender_source_concept_id
    #todo: ? specs says 'xlate to human-readable text'
    person_fd.write('{0},'.format(yd.BENE_RACE_CD))                                         # race_source_value
    person_fd.write(',')                                                                    # race_source_concept_id
    #todo: ? specs says 'xlate to human-readable text'
    person_fd.write('{0},'.format(yd.BENE_RACE_CD))                                         # ethnicity_source_value
    person_fd.write('')                                                                     # ethnicity_source_concept_id
    person_fd.write('\n')
    person_fd.increment_recs_written(1)

# -----------------------------------
# Observation Period
# -----------------------------------
def write_observation_period_records(beneficiary):
    obs_period_fd = file_control.get_Descriptor('observation_period')
    for (start_date, end_date) in beneficiary.ObservationPeriodList():
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
def write_death_records(death_fd, beneficiary, death_type_concept_id, cause_surce_concept_id):
    yd = beneficiary.LatestYearData()
    if yd is not None and yd.BENE_DEATH_DT != '':
        death_fd.write('{0},'.format(beneficiary.person_id))
        death_fd.write('{0},'.format(get_date_YYYY_MM_DD(yd.BENE_DEATH_DT)))
        death_fd.write('{0},'.format(death_type_concept_id))
        ##
        ## check values
        ##
        death_fd.write('0,')                                                    # cause_concept_id
        death_fd.write(',')                                                     # cause_source_value
        death_fd.write('{0}'.format(cause_surce_concept_id))
        death_fd.write('\n')
        death_fd.increment_recs_written(1)

# -----------------------------------
# Drug Exposure
# -----------------------------------
def write_drug_exposure(drug_exp_fd, person_id, drug_concept_id, start_date, drug_type_concept_id,
                        quantity, days_supply, drug_source_concept_id, drug_source_value, visit_occurrence_id):

        drug_exp_fd.write('{0},'.format(table_ids.last_drug_exposure_id))
        drug_exp_fd.write('{0},'.format(person_id))
        drug_exp_fd.write('{0},'.format(drug_concept_id))
        drug_exp_fd.write('{0},'.format(get_date_YYYY_MM_DD(start_date)))              # drug_exposure_start_date
        drug_exp_fd.write(',')                                                          # drug_exposure_end_date
        drug_exp_fd.write('{0},'.format(drug_type_concept_id))
        drug_exp_fd.write(',')                                                          # stop_reason
        drug_exp_fd.write(',')                                                          # refills

        ## ugh: clean this up! datamodel expects int; cms has float
        drug_exp_fd.write('{0:.0f},'.format(math.ceil(float(quantity))))
        drug_exp_fd.write('{0},'.format(days_supply))
        drug_exp_fd.write(',')                                                          # sig
        drug_exp_fd.write(',')                                                          # route_concept_id
        drug_exp_fd.write(',')                                                          # effective_drug_dose
        drug_exp_fd.write(',')                                                          # dose_unit_concept_ id
        drug_exp_fd.write(',')                                                          # lot_number
        drug_exp_fd.write(',')                                                          # provider_id
        drug_exp_fd.write('{0},'.format(visit_occurrence_id))
        drug_exp_fd.write('{0},'.format(drug_source_value))
        drug_exp_fd.write('{0},'.format(drug_source_concept_id))
        drug_exp_fd.write(',')                                                          # route_source_value
        drug_exp_fd.write('')                                                           # dose_unit_source_value
        drug_exp_fd.write('\n')
        drug_exp_fd.increment_recs_written(1)
        table_ids.last_drug_exposure_id += 1

# -----------------------------------
# Device Exposure
# -----------------------------------
def write_device_exposure(device_fd, person_id, device_concept_id, start_date, end_date, device_type_concept_id,
                          device_source_value, device_source_concept_id, visit_occurrence_id):
    device_fd.write('{0},'.format(table_ids.last_device_exposure_id))
    device_fd.write('{0},'.format(person_id))
    device_fd.write('{0},'.format(device_concept_id))
    device_fd.write('{0},'.format(get_date_YYYY_MM_DD(start_date)))
    device_fd.write('{0},'.format(get_date_YYYY_MM_DD(end_date)))
    # TODO: fill in the gaps...
    device_fd.write('{0},'.format(device_type_concept_id))
    device_fd.write(',')        # unique_device_id
    device_fd.write(',')        # quantity
    device_fd.write(',')        # provider_id
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
        ndc_code = rec.PROD_SRVC_ID
        # if ndc_code == "OTHER":                 	# Some entries have an NDC code of "OTHER", discarding these
        #     continue
        # elif ndc_code not in source_code_concept_dict:
        #     # recs_NDC_not_found_skipped+=1
        #     # if ndc_code not in ndcs_not_found: ndcs_not_found[ndc_code]=0
        #     # ndcs_not_found[ndc_code]+=1
        #     continue

        #todo: use standard OMOP concepts for unmapped
        drug_source_concept_id = 0
        drug_concept_id = 0
        if (OMOP_CONSTANTS.NDC_VOCABULARY_ID,ndc_code) in source_code_concept_dict:
            drug_source_concept_id = source_code_concept_dict[OMOP_CONSTANTS.NDC_VOCABULARY_ID,ndc_code].source_concept_id
            drug_concept_id = source_code_concept_dict[OMOP_CONSTANTS.NDC_VOCABULARY_ID,ndc_code].target_concept_id

        write_drug_exposure(drug_exp_fd, beneficiary.person_id,
                            drug_concept_id=drug_concept_id,
                            start_date=rec.SRVC_DT,
                            drug_type_concept_id=OMOP_CONSTANTS.DRUG_TYPE_PRESCRIPTION,
                            quantity=math.ceil(float(rec.QTY_DSPNSD_NUM)),
                            days_supply=rec.DAYS_SUPLY_NUM,
                            drug_source_concept_id=drug_source_concept_id,
                            drug_source_value=ndc_code,
                            visit_occurrence_id=beneficiary.get_visit_id(rec.SRVC_DT))

        #----------------------
        # drug cost
        #----------------------
        drug_cost_fd.write('{0},'.format(table_ids.last_drug_cost_id))
        drug_cost_fd.write('{0},'.format(table_ids.last_drug_exposure_id))
        drug_cost_fd.write('{0},'.format(OMOP_CONSTANTS.CURRENCY_US_DOLLAR))
        drug_cost_fd.write(',')                                          # paid_copay
        drug_cost_fd.write('{0},'.format(rec.PTNT_PAY_AMT))              # paid_coinsurance
        drug_cost_fd.write(',')                                          # paid_toward_deductible
        drug_cost_fd.write(',')                                          # paid_by_payer
        drug_cost_fd.write(',')                                          # paid_by_coordination_of_benefits
        drug_cost_fd.write(',')                                          # total_out_of_pocket
        #todo: track down where the \n is coming from!
        drug_cost_fd.write('{0},'.format(rec.TOT_RX_CST_AMT[:-1]))            # total_paid
        # drug_cost_fd.write(',')
        drug_cost_fd.write(',')                                          # ingredient_cost
        drug_cost_fd.write(',')                                          # dispensing_fee
        drug_cost_fd.write(',')                                          # average_wholesale_price
        drug_cost_fd.write('')                                           # payer_plan_period_id                  ##### LOOKUP in PAYER-PLAN-PERIOD-TBL
        drug_cost_fd.write('\n')
        drug_cost_fd.increment_recs_written(1)
        table_ids.last_drug_cost_id += 1

# -----------------------------------
# Provider file
# -----------------------------------
def write_provider_record(provider_fd, npi, care_site_id, specialty_concept_id=''):
    provider_id = get_Provider(npi)
    provider_fd.write('{0},'.format(provider_id))
    provider_fd.write(',')                                                            # provider_name
    provider_fd.write('{0},'.format(npi))
    provider_fd.write(',')                                                            # dea
    provider_fd.write('{0},'.format(specialty_concept_id))
    provider_fd.write('{0},'.format(care_site_id))
    provider_fd.write(',')                                                            # year_of_birth
    provider_fd.write('1,')                                                           # gender_concept_id
    provider_fd.write(',')                                                            # provider_source_value
    provider_fd.write(',')                                                            # specialty_source_value
    provider_fd.write('2,')                                                           # specialty_source_concept_id
    provider_fd.write(',')                                                            # gender_source_value
    provider_fd.write('3')                                                            # gender_source_concept_id
    provider_fd.write('\n')
    provider_fd.increment_recs_written(1)

# -----------------------------------
# Condition Occurence file
# -----------------------------------
def write_condition_occurrence(cond_occur_fd, person_id, condition_concept_id,
                              from_date, thru_date, condition_type_concept_id,
                              condition_source_value, condition_source_concept_id, visit_occurrence_id):
    cond_occur_fd.write('{0},'.format(table_ids.last_condition_occurrence_id))
    cond_occur_fd.write('{0},'.format(person_id))
    cond_occur_fd.write('{0},'.format(condition_concept_id))
    cond_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(from_date)))
    cond_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(thru_date)))
    cond_occur_fd.write('{0},'.format(condition_type_concept_id))
    cond_occur_fd.write(',')                                          # stop_reason
    cond_occur_fd.write(',')                                          # provider_id
    cond_occur_fd.write('{0},'.format(visit_occurrence_id))
    cond_occur_fd.write('{0},'.format(condition_source_value))
    cond_occur_fd.write('{0}'.format(condition_source_concept_id))
    cond_occur_fd.write('\n')
    cond_occur_fd.increment_recs_written(1)
    table_ids.last_condition_occurrence_id += 1

# -----------------------------------
# Procedure Occurence file
# -----------------------------------
def write_procedure_occurrence(proc_occur_fd, person_id, procedure_concept_id,
                              from_date, procedure_type_concept_id,
                              procedure_source_value, procedure_source_concept_id, visit_occurrence_id):
    proc_occur_fd.write('{0},'.format(table_ids.last_procedure_occurrence_id))
    proc_occur_fd.write('{0},'.format(person_id))
    proc_occur_fd.write('{0},'.format(procedure_concept_id))
    proc_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(from_date)))              # procedure_date
    proc_occur_fd.write('{0},'.format(procedure_type_concept_id))
    proc_occur_fd.write(',')                                          # modifier_concept_id
    proc_occur_fd.write(',')                                          # quantity
    proc_occur_fd.write(',')                                          # provider_id
    proc_occur_fd.write('{0},'.format(visit_occurrence_id))
    proc_occur_fd.write('{0},'.format(procedure_source_value))
    proc_occur_fd.write('{0},'.format(procedure_source_concept_id))
    proc_occur_fd.write('')                                           # qualifier_source_value
    proc_occur_fd.write('\n')
    proc_occur_fd.increment_recs_written(1)
    table_ids.last_procedure_occurrence_id += 1

# -----------------------------------
# Measurement file
# -----------------------------------
def write_measurement(measurement_fd, person_id, measurement_concept_id,
                      measurement_date, measurement_type_concept_id,
                      measurement_source_value, measurement_source_concept_id, visit_occurrence_id):
    measurement_fd.write('{0},'.format(table_ids.last_measurement_id))
    measurement_fd.write('{0},'.format(person_id))
    measurement_fd.write('{0},'.format(measurement_concept_id))
    measurement_fd.write('{0},'.format(get_date_YYYY_MM_DD(measurement_date)))
    # TODO: fill in the gaps...
    measurement_fd.write(',')        # measurement_time
    measurement_fd.write('{0},'.format(measurement_type_concept_id))
    measurement_fd.write(',')        # operator_concept_id
    measurement_fd.write(',')        # value_as_number
    measurement_fd.write(',')        # value_as_concept_id
    measurement_fd.write(',')        # unit_concept_id
    measurement_fd.write(',')        # range_low
    measurement_fd.write(',')        # range_high
    measurement_fd.write(',')        # provider_id
    measurement_fd.write('{0},'.format(visit_occurrence_id))
    measurement_fd.write('{0},'.format(measurement_source_value))
    measurement_fd.write('{0},'.format(measurement_source_concept_id))
    measurement_fd.write(',')        # unit_source_value
    measurement_fd.write('')        # value_source_value
    measurement_fd.write('\n')
    measurement_fd.increment_recs_written(1)
    table_ids.last_measurement_id += 1

# -----------------------------------
# Observation file
# -----------------------------------
def write_observation(observation_fd, person_id, observation_concept_id,
                      observation_date, observation_type_concept_id,
                      observation_source_value, observation_source_concept_id, visit_occurrence_id):
    observation_fd.write('{0},'.format(table_ids.last_observation_id))
    observation_fd.write('{0},'.format(person_id))
    observation_fd.write('{0},'.format(observation_concept_id))
    observation_fd.write('{0},'.format(get_date_YYYY_MM_DD(observation_date)))
    # TODO: fill in the gaps...
    observation_fd.write(',')        # observation_time
    observation_fd.write('{0},'.format(observation_type_concept_id))
    observation_fd.write(',')        # value_as_number
    observation_fd.write(',')        # value_as_string
    observation_fd.write(',')        # value_as_concept_id
    observation_fd.write(',')        # qualifier_concept_id
    observation_fd.write(',')        # unit_concept_id
    observation_fd.write(',')        # provider_id
    observation_fd.write('{0},'.format(visit_occurrence_id))
    observation_fd.write('{0},'.format(observation_source_value))
    observation_fd.write('{0},'.format(observation_source_concept_id))
    observation_fd.write(',')        # unit_source_value
    observation_fd.write('')        # qualifier_source_value
    observation_fd.write('\n')
    observation_fd.increment_recs_written(1)
    table_ids.last_observation_id += 1


# -----------------------------------
# Care Site file
# -----------------------------------
def write_care_site(care_site_fd, provider_number, place_of_service_concept_id, care_site_source_value, place_of_service_source_value):

    if provider_number not in provider_id_care_site_id:
        care_site_fd.write('{0},'.format(get_CareSite(provider_number)))
        care_site_fd.write(',')                                                   # care_site_name
        care_site_fd.write('{0},'.format(place_of_service_concept_id))
        care_site_fd.write(',')                                                   # location_id
        care_site_fd.write('{0},'.format(care_site_source_value))
        care_site_fd.write('{0}'.format(place_of_service_source_value))
        care_site_fd.write('\n')
        care_site_fd.increment_recs_written(1)

# -----------------------------------
# From Inpatient Records:
#     --> Death
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

    for raw_rec in beneficiary.inpatient_records:
        rec = InpatientClaim(raw_rec)
        i = 0
        for (vocab,code) in ([(OMOP_CONSTANTS.ICD_9_VOCAB_ID, icd9fix(x)) for x in rec.ICD9_DGNS_CD_list] +
                             [(OMOP_CONSTANTS.ICD_9_VOCAB_ID, icd9fix(x)) for x in rec.ICD9_PRCDR_CD_list] +
                             [(OMOP_CONSTANTS.HCPCS_VOCABULARY_ID, x) for x in rec.HCPCS_CD_list]):

            if rec.CLM_FROM_DT != '':
                #todo: use standard OMOP concepts for unmapped
                source_concept_id = 0
                target_concept_id = 0
                destination_file = DESTINATION_FILE_CONDITION

                if (vocab,code) in source_code_concept_dict:
                    source_concept_id = source_code_concept_dict[vocab,code].source_concept_id
                    target_concept_id = source_code_concept_dict[vocab,code].target_concept_id
                    destination_file = source_code_concept_dict[vocab,code].destination_file
                if destination_file == DESTINATION_FILE_PROCEDURE:
                    write_procedure_occurrence(proc_occur_fd, beneficiary.person_id,
                                               procedure_concept_id=source_concept_id,
                                               from_date=rec.CLM_FROM_DT,
                                               procedure_type_concept_id=OMOP_CONSTANTS.INPAT_PROCEDURE_1ST_POSITION + i,
                                            ## todo: do we keep the dotted or no-dot version?
                                               procedure_source_value=code,
                                               procedure_source_concept_id=source_concept_id,
                                               visit_occurrence_id=beneficiary.get_visit_id(rec.CLM_FROM_DT))

                elif destination_file == DESTINATION_FILE_CONDITION:
                    write_condition_occurrence(cond_occur_fd,beneficiary.person_id,
                                               condition_concept_id=target_concept_id,
                                               from_date=rec.CLM_FROM_DT, thru_date=rec.CLM_THRU_DT,
                                               condition_type_concept_id=OMOP_CONSTANTS.INPAT_CONDITION_1ST_POSITION + i,
                                               condition_source_value=code,
                                               condition_source_concept_id=source_concept_id,
                                               visit_occurrence_id=beneficiary.get_visit_id(rec.CLM_FROM_DT))

                elif destination_file == DESTINATION_FILE_DRUG:
                    write_drug_exposure(drug_exp_fd, beneficiary.person_id,
                                        drug_concept_id=target_concept_id,
                                        start_date=rec.CLM_FROM_DT,
                                        drug_type_concept_id=OMOP_CONSTANTS.DRUG_TYPE_PRESCRIPTION,
                                        quantity=None,
                                        days_supply=None,
                                        drug_source_value=code,
                                        drug_source_concept_id=source_concept_id,
                                        visit_occurrence_id=beneficiary.get_visit_id(rec.CLM_FROM_DT))

                elif destination_file == DESTINATION_FILE_MEASUREMENT:
                    write_measurement(measurement_fd, beneficiary.person_id,
                                      measurement_concept_id=target_concept_id,
                                      measurement_date=rec.CLM_FROM_DT,
                                      measurement_type_concept_id=OMOP_CONSTANTS.MEASUREMENT_DERIVED_VALUE,
                                      measurement_source_value=code,
                                      measurement_source_concept_id=source_concept_id,
                                      visit_occurrence_id=beneficiary.get_visit_id(rec.CLM_FROM_DT))

                elif destination_file == DESTINATION_FILE_OBSERVATION:
                    write_observation(observation_fd, beneficiary.person_id,
                                      observation_concept_id=source_concept_id,
                                      observation_date=rec.CLM_FROM_DT,
                                      observation_type_concept_id=OMOP_CONSTANTS.OBSERVATION_CHIEF_COMPLAINT,
                                      observation_source_value=code,
                                      observation_source_concept_id=source_concept_id,
                                      visit_occurrence_id=beneficiary.get_visit_id(rec.CLM_FROM_DT))

                elif destination_file == DESTINATION_FILE_DEVICE:
                    write_device_exposure(device_fd, beneficiary.person_id,
                                          device_concept_id=source_concept_id,
                                          start_date=rec.CLM_FROM_DT,
                                          end_date=rec.CLM_THRU_DT,
                                          device_type_concept_id=OMOP_CONSTANTS.DEVICE_INFERRED_PROCEDURE_CLAIM,
                                          device_source_value=code,
                                          device_source_concept_id=source_concept_id,
                                          visit_occurrence_id=beneficiary.get_visit_id(rec.CLM_FROM_DT))
                #check for death
                if code in icd9_codes_death:
                    write_death_records(death_fd, beneficiary,
                                        death_type_concept_id=OMOP_CONSTANTS.DEATH_TYPE_CONDITION,
                                        cause_surce_concept_id=code)

        #-- care site / provider
        care_site_id = 0
        if rec.PRVDR_NUM != '':
            write_care_site(care_site_fd, provider_number=rec.PRVDR_NUM,
                           place_of_service_concept_id=OMOP_CONSTANTS.INPATIENT_PLACE_OF_SERVICE,
                           care_site_source_value=rec.PRVDR_NUM,
                           place_of_service_source_value=OMOP_CONSTANTS.INPATIENT_PLACE_OF_SERVICE_SOURCE)
        #-- provider
        for npi in (rec.AT_PHYSN_NPI, rec.OP_PHYSN_NPI, rec.OT_PHYSN_NPI):
            if npi != '':
                write_provider_record(provider_fd, npi, care_site_id, specialty_concept_id='11')

# -----------------------------------
# From Outpatient Records:
#     --> Death
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

    for raw_rec in beneficiary.outpatient_records:
        rec = OutpatientClaim(raw_rec)
        # blech...index math to derive conceptid !          ############ FIX #########
        # ip_proc_offset = len(rec.ICD9_DGNS_CD_list)
        i = 0
        
        for (vocab,code) in ( ([] if rec.ADMTNG_ICD9_DGNS_CD == "" else [(OMOP_CONSTANTS.ICD_9_VOCAB_ID,icd9fix(rec.ADMTNG_ICD9_DGNS_CD))]) +
                            [(OMOP_CONSTANTS.ICD_9_VOCAB_ID,icd9fix(x)) for x in rec.ICD9_DGNS_CD_list] +
                            [(OMOP_CONSTANTS.ICD_9_VOCAB_ID,icd9fix(x)) for x in rec.ICD9_PRCDR_CD_list] +
                            [(OMOP_CONSTANTS.HCPCS_VOCABULARY_ID,x) for x in rec.HCPCS_CD_list]):
            if rec.CLM_FROM_DT != '':
                #todo: use standard OMOP concepts for unmapped
                source_concept_id = 0
                target_concept_id = 0
                destination_file = DESTINATION_FILE_CONDITION
                if (vocab,code) in source_code_concept_dict:
                    source_concept_id = source_code_concept_dict[vocab,code].source_concept_id
                    target_concept_id = source_code_concept_dict[vocab,code].target_concept_id
                    destination_file = source_code_concept_dict[vocab,code].destination_file
                if destination_file == DESTINATION_FILE_PROCEDURE:
                    write_procedure_occurrence(proc_occur_fd, beneficiary.person_id,
                                               procedure_concept_id=target_concept_id,
                                               from_date=rec.CLM_FROM_DT,
                                               procedure_type_concept_id=OMOP_CONSTANTS.OUTPAT_PROCEDURE_1ST_POSITION + i,
                                               procedure_source_value=code,
                                               procedure_source_concept_id=source_concept_id,
                                               visit_occurrence_id=beneficiary.get_visit_id(rec.CLM_FROM_DT))

                elif destination_file == DESTINATION_FILE_CONDITION:
                    write_condition_occurrence(cond_occur_fd,beneficiary.person_id,
                                               condition_concept_id=target_concept_id,
                                               from_date=rec.CLM_FROM_DT, thru_date=rec.CLM_THRU_DT,
                                               condition_type_concept_id=OMOP_CONSTANTS.OUTPAT_CONDITION_1ST_POSITION + i,
                                               condition_source_value=code,
                                               condition_source_concept_id=source_concept_id,
                                               visit_occurrence_id=beneficiary.get_visit_id(rec.CLM_FROM_DT))

                elif destination_file == DESTINATION_FILE_DRUG:
                    write_drug_exposure(drug_exp_fd, beneficiary.person_id,
                                        drug_concept_id=target_concept_id,
                                        start_date=rec.CLM_FROM_DT,
                                        drug_type_concept_id=OMOP_CONSTANTS.DRUG_TYPE_PRESCRIPTION,
                                        quantity=None,
                                        days_supply=None,
                                        drug_source_value=code,
                                        drug_source_concept_id=source_concept_id,
                                        visit_occurrence_id=beneficiary.get_visit_id(rec.CLM_FROM_DT))

                elif destination_file == DESTINATION_FILE_MEASUREMENT:
                    write_measurement(measurement_fd, beneficiary.person_id,
                                      measurement_concept_id=target_concept_id,
                                      measurement_date=rec.CLM_FROM_DT,
                                      measurement_type_concept_id=OMOP_CONSTANTS.MEASUREMENT_DERIVED_VALUE,
                                      measurement_source_value=code,
                                      measurement_source_concept_id=source_concept_id,
                                      visit_occurrence_id=beneficiary.get_visit_id(rec.CLM_FROM_DT))

                elif destination_file == DESTINATION_FILE_OBSERVATION:
                    write_observation(observation_fd, beneficiary.person_id,
                                      observation_concept_id=target_concept_id,
                                      observation_date=rec.CLM_FROM_DT,
                                      observation_type_concept_id=OMOP_CONSTANTS.OBSERVATION_CHIEF_COMPLAINT,
                                      observation_source_value=code,
                                      observation_source_concept_id=source_concept_id,
                                      visit_occurrence_id=beneficiary.get_visit_id(rec.CLM_FROM_DT))

                elif destination_file == DESTINATION_FILE_DEVICE:
                    write_device_exposure(device_fd, beneficiary.person_id,
                                          device_concept_id=target_concept_id,
                                          start_date=rec.CLM_FROM_DT,
                                          end_date=rec.CLM_THRU_DT,
                                          device_type_concept_id=OMOP_CONSTANTS.DEVICE_INFERRED_PROCEDURE_CLAIM,
                                          device_source_value=code,
                                          device_source_concept_id=source_concept_id,
                                          visit_occurrence_id=beneficiary.get_visit_id(rec.CLM_FROM_DT))

                #check for death
                if code in icd9_codes_death:
                    write_death_records(death_fd, beneficiary,
                                        death_type_concept_id=OMOP_CONSTANTS.DEATH_TYPE_CONDITION,
                                        cause_surce_concept_id=code)

        #-- care site / provider
        care_site_id = 0
        if rec.PRVDR_NUM != '':
            write_care_site(care_site_fd, provider_number=rec.PRVDR_NUM,
                           place_of_service_concept_id=OMOP_CONSTANTS.OUTPATIENT_PLACE_OF_SERVICE,
                           care_site_source_value=rec.PRVDR_NUM,
                           place_of_service_source_value=OMOP_CONSTANTS.OUTPATIENT_PLACE_OF_SERVICE_SOURCE)

        #-- provider
        for npi in (rec.AT_PHYSN_NPI, rec.OP_PHYSN_NPI, rec.OT_PHYSN_NPI):
            if npi != '':
                write_provider_record(provider_fd, npi, care_site_id, specialty_concept_id='22')

# -----------------------------------
# From Carrier Claims Records:
#     --> Death
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

    for raw_rec in beneficiary.carrier_records:
        rec = CarrierClaim(raw_rec)
        # blech...index math to derive conceptid !          ############ FIX #########
        # ip_proc_offset = len(rec.ICD9_DGNS_CD_list)
        i = 0
        for (vocab,code) in ([(OMOP_CONSTANTS.ICD_9_VOCAB_ID, icd9fix(x)) for x in rec.ICD9_DGNS_CD_list] +
                            [(OMOP_CONSTANTS.HCPCS_VOCABULARY_ID, x) for x in rec.HCPCS_CD_list] +
                            [(OMOP_CONSTANTS.ICD_9_VOCAB_ID, icd9fix(x)) for x in  rec.LINE_ICD9_DGNS_CD_list]):

            if rec.CLM_FROM_DT != '':
                #todo: use standard OMOP concepts for unmapped
                source_concept_id = 0
                target_concept_id = 0
                destination_file = DESTINATION_FILE_CONDITION
                if (vocab,code) in source_code_concept_dict:
                    source_concept_id = source_code_concept_dict[vocab,code].source_concept_id
                    target_concept_id = source_code_concept_dict[vocab,code].target_concept_id
                    destination_file = source_code_concept_dict[vocab,code].destination_file
                if destination_file == DESTINATION_FILE_PROCEDURE:
                    write_procedure_occurrence(proc_occur_fd, beneficiary.person_id,
                                               procedure_concept_id=target_concept_id,
                                               from_date=rec.CLM_FROM_DT,
                                               procedure_type_concept_id=OMOP_CONSTANTS.OUTPAT_PROCEDURE_1ST_POSITION + i,
                                               procedure_source_value=code,
                                               procedure_source_concept_id=source_concept_id,
                                               visit_occurrence_id=beneficiary.get_visit_id(rec.CLM_FROM_DT))

                elif destination_file == DESTINATION_FILE_CONDITION:
                    write_condition_occurrence(cond_occur_fd,beneficiary.person_id,
                                               condition_concept_id=target_concept_id,
                                               from_date=rec.CLM_FROM_DT, thru_date=rec.CLM_THRU_DT,
                                               condition_type_concept_id=OMOP_CONSTANTS.OUTPAT_CONDITION_1ST_POSITION + i,
                                               condition_source_value=code,
                                               condition_source_concept_id=source_concept_id,
                                               visit_occurrence_id=beneficiary.get_visit_id(rec.CLM_FROM_DT))

                elif destination_file == DESTINATION_FILE_DRUG:
                    write_drug_exposure(drug_exp_fd, beneficiary.person_id,
                                        drug_concept_id=target_concept_id,
                                        start_date=rec.CLM_FROM_DT,
                                        drug_type_concept_id=OMOP_CONSTANTS.DRUG_TYPE_PRESCRIPTION,
                                        quantity=None,
                                        days_supply=None,
                                        drug_source_value=code,
                                        drug_source_concept_id=source_concept_id,
                                        visit_occurrence_id=beneficiary.get_visit_id(rec.CLM_FROM_DT))


                elif destination_file == DESTINATION_FILE_MEASUREMENT:
                    write_measurement(measurement_fd, beneficiary.person_id,
                                      measurement_concept_id=target_concept_id,
                                      measurement_date=rec.CLM_FROM_DT,
                                      measurement_type_concept_id=OMOP_CONSTANTS.MEASUREMENT_DERIVED_VALUE,
                                      measurement_source_value=code,
                                      measurement_source_concept_id=source_concept_id,
                                      visit_occurrence_id=beneficiary.get_visit_id(rec.CLM_FROM_DT))

                elif destination_file == DESTINATION_FILE_OBSERVATION:
                    write_observation(observation_fd, beneficiary.person_id,
                                      observation_concept_id=target_concept_id,
                                      observation_date=rec.CLM_FROM_DT,
                                      observation_type_concept_id=OMOP_CONSTANTS.OBSERVATION_CHIEF_COMPLAINT,
                                      observation_source_value=code,
                                      observation_source_concept_id=source_concept_id,
                                      visit_occurrence_id=beneficiary.get_visit_id(rec.CLM_FROM_DT))

                elif destination_file == DESTINATION_FILE_DEVICE:
                    write_device_exposure(device_fd, beneficiary.person_id,
                                          device_concept_id=target_concept_id,
                                          start_date=rec.CLM_FROM_DT,
                                          end_date=rec.CLM_THRU_DT,
                                          device_type_concept_id=OMOP_CONSTANTS.DEVICE_INFERRED_PROCEDURE_CLAIM,
                                          device_source_value=code,
                                          device_source_concept_id=source_concept_id,
                                          visit_occurrence_id=beneficiary.get_visit_id(rec.CLM_FROM_DT))


                #check for death
                if code in icd9_codes_death:
                    write_death_records(death_fd, beneficiary,
                                        death_type_concept_id=OMOP_CONSTANTS.DEATH_TYPE_CONDITION,
                                        cause_surce_concept_id=code)

        #-- care site / provider
        for cc_line in rec.CarrierClaimLine_list:
            care_site_id = 0
            #
            # check: only write new care_sides ?
            #
            if cc_line.TAX_NUM != '':
                write_care_site(care_site_fd, provider_number=cc_line.TAX_NUM,
                               place_of_service_concept_id=OMOP_CONSTANTS.OUTPATIENT_PLACE_OF_SERVICE,
                               care_site_source_value=cc_line.TAX_NUM,
                               place_of_service_source_value=OMOP_CONSTANTS.OUTPATIENT_PLACE_OF_SERVICE_SOURCE)

            if cc_line.PRF_PHYSN_NPI != '':
                write_provider_record(provider_fd, cc_line.PRF_PHYSN_NPI, care_site_id, specialty_concept_id='33')

            # #-- procedure cost
            if cc_line.has_nonzero_amount():
                proc_cost_fd.write('{0},'.format(table_ids.last_procedure_cost_id))
                proc_cost_fd.write('{0},'.format(table_ids.last_procedure_occurrence_id))
                proc_cost_fd.write('{0},'.format(OMOP_CONSTANTS.CURRENCY_US_DOLLAR))               # currency_concept_id
                proc_cost_fd.write(',')                                                            # paid_copy
                proc_cost_fd.write('{0},'.format(cc_line.LINE_COINSRNC_AMT))                                    # paid_coinsurance
                proc_cost_fd.write('{0},'.format(cc_line.LINE_BENE_PTB_DDCTBL_AMT))                             # paid_toward_deductible
                proc_cost_fd.write('{0},'.format(cc_line.LINE_NCH_PMT_AMT))                                     # paid_by_payer
                proc_cost_fd.write('{0},'.format(cc_line.LINE_BENE_PRMRY_PYR_PD_AMT))                           # paid_by_coordination_benefits

                amt = 0
                try:
                    amt = float(cc_line.LINE_BENE_PTB_DDCTBL_AMT) + float(cc_line.LINE_COINSRNC_AMT)
                except:
                    pass
                proc_cost_fd.write('{0:2},'.format(amt))                                            # total_out_of_pocket

                proc_cost_fd.write('{0},'.format(cc_line.LINE_ALOWD_CHRG_AMT))                                 # total_paid
                proc_cost_fd.write('0,')                                                          # revenue_code_concept_id
                ##
                ## need to lookup
                ##
                proc_cost_fd.write('0,')                                                          # payer_plan_period_id
                proc_cost_fd.write('')                                                           # revenue_code_source_value
                proc_cost_fd.write('\n')
                proc_cost_fd.increment_recs_written(1)
                table_ids.last_procedure_cost_id += 1

#---------------------------------
def write_header_records():
    headers = {
        'person' :
            'person_id,gender_concept_id,year_of_birth,month_of_birth,day_of_birth,time_of_birth,race_concept_id,ethnicity_concept_id,'
            'location_id,provider_id,care_site_id,person_source_value,gender_source_value,gender_source_concept_id,race_source_value,'
            'race_source_concept_id,ethnicity_source_value,ethnicity_source_concept_id',

        'observation':
            'observation_id, person_id, observation_concept_id, observation_date, observation_time, observation_type_concept_id, value_as_number,'
            'value_as_string, value_as_concept_id, qualifier_concept_id, unit_concept_id, provider_id, visit_occurrence_id, observation_source_value,'
            'observation_source_concept_id, unit_source_value, qualifier_source_value',

        'observation_period':
            'observation_period_id,person_id, observation_period_start_date, observation_period_end_date, period_type_concept_id',

        'specimen':
            'specimen_id, person_id, specimen_concept_id, specimen_type_concept_id, specimen_date, specimen_time, quantity,'
            'unit_concept_id, anatomic_site_concept_id, disease_status_concept_id, specimen_source_id, specimen_source_value, unit_source_value,'
            'anatomic_site_source_value, disease_status_source_value',

        'death':
            'person_id, death_date, death_type_concept_id, cause_concept_id, cause_source_value, cdm_counts_dict',

        'visit_occurrence':
            'visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_start_time, visit_end_date, visit_end_time,'
            'visit_type_concept_id, provider_id, care_site_id, visit_source_value, visit_source_concept_id',

        'visit_cost':
            'visit_cost_id, visit_occurrence_id, currency_concept_id, paid_copay, paid_coinsurance, paid_toward_deductible,'
            'paid_by_payer, paid_by_coordination_benefits, total_out_of_pocket, total_paid, payer_plan_period_id',

        'condition_occurrence':
            'condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_end_date, condition_type_concept_id,'
            'stop_reason, provider_id, visit_occurrence_id, condition_source_value, condition_source_concept_id',

        'procedure_occurrence':
            'procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, procedure_type_concept_id, modifier_concept_id,'
            'quantity, provider_id, visit_occurrence_id, procedure_source_value, procedure_source_concept_id, qualifier_source_value',

        'procedure_cost':
            'procedure_cost_id, procedure_occurrence_id, currency_concept_id, paid_copay, paid_coinsurance, paid_toward_deductible,'
            'paid_by_payer, paid_by_coordination_benefits, total_out_of_pocket, total_paid, revenue_code_concept_id, payer_plan_period_id, revenue_code_source_value',

        'drug_exposure':
            'drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date, drug_type_concept_id,'
            'stop_reason, refills, quantity, days_supply, sig, route_concept_id, effective_drug_dose, dose_unit_concept_id,'
            'lot_number, provider_id, visit_occurrence_id, drug_source_value, drug_source_concept_id, route_source_value, dose_unit_source_value',

        'drug_cost':
            'drug_cost_id, drug_exposure_id, currency_concept_id, paid_copay, paid_coinsurance, paid_toward_deductible, paid_by_payer, paid_by_coordination_of_benefits,'
            'total_out_of_pocket, total_paid, ingredient_cost, dispensing_fee, average_wholesale_price, payer_plan_period_id',

        'device_exposure':
            'device_exposure_id, person_id, device_concept_id, device_exposure_start_date, device_exposure_end_date, device_type_concept_id,'
            'unique_device_id, quantity, provider_id, visit_occurrence_id, device_source_value, device_source_concept_id',

        'device_cost':
            'device_cost_id, device_exposure_id, currency_concept_id, paid_copay, paid_coinsurance, paid_toward_deductible,'
            'paid_by_payer, paid_by_coordination_benefits, total_out_of_pocket, total_paid, payer_plan_period_id',

        'measurement_occurrence':
            'measurement_id, person_id, measurement_concept_id, measurement_date, measurement_time, measurement_type_concept_id, operator_concept_id,'
            'value_as_number, value_as_concept_id, unit_concept_id, range_low, range_high, provider_id, visit_occurrence_id, measurement_source_value,'
            'measurement_source_concept_id, unit_source_value, value_source_value',

        'location':
            'location_id, address_1, address_2, city, state, zip, county, location_source_value',

        'care_site':
            'care_site_id, care_site_name, place_of_service_concept_id, location_id, care_site_source_value, place_of_service_source_value',

        'provider':
            'provider_id, provider_name, NPI, DEA, specialty_concept_id, care_site_id, year_of_birth, gender_concept_id, provider_source_value,'
            'specialty_source_value, specialty_source_concept_id, gender_source_value, gender_source_concept_id',

        'payer_plan_period':
            'payer_plan_period_id, person_id, payer_plan_period_start_date, payer_plan_period_end_date, payer_source_value, '
            'plan_source_value, family_source_value',
    }

    for token in sorted(file_control.descriptor_list(which='output')):
        fd = file_control.get_Descriptor(token)
        fd.write(headers[token] + '\n')
        fd.increment_recs_written(1)


#---------------------------------
#---------------------------------
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



def process_beneficiary(bene):
    # print '-'*80
    # print '--> ', bene.DESYNPUF_ID
    bene.LoadClaimData(file_control)

    write_person_record(bene)
    write_observation_period_records(bene)
    write_death_records(file_control.get_Descriptor('death'), bene,
                        death_type_concept_id=OMOP_CONSTANTS.DEATH_TYPE_PAYER_ENR_STATUS,
                        cause_surce_concept_id=0)

    determine_visits(bene)

    write_drug_records(bene)
    process_inpatient_records(bene)
    process_outpatient_records(bene)
    process_carrier_records(bene)

    # dump_beneficiary_records(fout, rec)
    file_control.flush_all()

#---------------------------------
#---------------------------------
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

#---------------------------------
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
#    sys.exit()

    #- get any existing Last-Table-IDs
    table_ids = Table_ID_Values()
    table_ids_filename = os.path.join(BASE_ETL_CONTROL_DIRECTORY, 'etl_synpuf_last_table_ids.txt')
    if os.path.exists(table_ids_filename):
        table_ids.Load(table_ids_filename, log_stats)

    # Build mappings between SynPUF codes and OMOP Vocabulary concept_ids
    build_maps()
    bene_dump_filename = os.path.join(BASE_OUTPUT_DIRECTORY,'beneficiary_dump_{0}.txt'.format(current_sample_number))

    # Build the object to manage access to all the files
    write_header_records()

    with open(bene_dump_filename,'w') as fout:
        # for ix,DESYNPUF_ID in enumerate(sorted(beneficiary_dict.keys())):

        # global beneficiary_dict
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
                # don't skip 1st record since the file is now sorted
                # rec = fin.readline()
                for rec in fin:
                    recs_in += 1
                    if recs_in % 50000 == 0: print 'beneficiary recs_in: ', recs_in
                    if recs_in > MAX_RECS:break

                    rec = rec.split(',')
                    DESYNPUF_ID = rec[BENEFICIARY_SUMMARY_RECORD.DESYNPUF_ID]
                    # count on this header record field being in every file
                    if '"DESYNPUF_ID"' in rec:
                        continue
                    # if DESYNPUF_ID not in beneficiary_dict:
                    #     bene = Beneficiary(DESYNPUF_ID, table_ids.last_person_id)
                    #     beneficiary_dict[DESYNPUF_ID] = bene
                    #     table_ids.last_person_id += 1
                    # bene.AddYearData(rec)

                    # check for bene break
                    if DESYNPUF_ID != save_DESYNPUF_ID:
                        if not bene is None:
                            process_beneficiary(bene)
                            if recs_in % 1000 == 0:
                                # print '[{0} {1} of {2}'.format(get_timestamp(), ix,len(beneficiary_dict))
                                print '[{0}] [{1}] {2:20} ip={3:5}, op={4:5}, cc={5:5}, rx={6:}'.format(get_timestamp(),
                                                                                                        recs_in, DESYNPUF_ID, len(bene.inpatient_records),len(bene.outpatient_records),len(bene.carrier_records), len(bene.prescription_records))

                        unique_DESYNPUF_ID_count += 1
                        save_DESYNPUF_ID = DESYNPUF_ID
                        bene = Beneficiary(DESYNPUF_ID, table_ids.last_person_id)
                        table_ids.last_person_id += 1
                    # else:
                    #accumulate for the current bene
                    bene.AddYearData(rec)

                # eof: handle buffered bene
                if not bene is None:
                    process_beneficiary(bene)

        except BaseException:
            print '** ERROR reading beneficiary file, record number ', recs_in, '\n record-> ', rec
            raise

        beneficiary_fd.increment_recs_read(recs_in)
        log_stats('last_person_id ending value -> ' + str(table_ids.last_person_id))
        # log_stats('Done: total records read ={0}, unique IDs={1}'.format(recs_in, len(beneficiary_dict)))
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


