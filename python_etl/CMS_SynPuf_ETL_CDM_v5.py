import csv,os,os.path,sys
from time import strftime
import argparse
import dotenv
from constants import OMOP_CONSTANTS, OMOP_CONCEPT_RECORD, BENEFICIARY_SUMMARY_RECORD
from utility_classes import Table_ID_Values
from beneficiary import Beneficiary
from FileControl import FileControl
from SynPufFiles import PrescriptionDrug, InpatientClaim, OutpatientClaim, CarrierClaim

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
MAX_RECS                        = os.environ['MAX_RECS'] or 100

# Path to the directory containing the downloaded SynPUF files
BASE_SYNPUF_INPUT_DIRECTORY     = os.environ['BASE_SYNPUF_INPUT_DIRECTORY']

# Path to the directory containing the OMOP Vocabulary v5 files (can be downloaded from http://www.ohdsi.org/web/athena/)
BASE_OMOP_INPUT_DIRECTORY       = os.environ['BASE_OMOP_INPUT_DIRECTORY']

# Path to the directory where CDM-compatible CSV files should be saved
BASE_OUTPUT_DIRECTORY           = os.environ['BASE_OUTPUT_DIRECTORY']

DESTINATION_FILE_DRUG               = 'drug'
DESTINATION_FILE_CONDITION          = 'condition'
DESTINATION_FILE_PROCEDURE          = 'procedure'
DESTINATION_FILE_OBSERVATION        = 'observation'
DESTINATION_FILE_MEASUREMENT        = 'measurement'
DESTINATION_FILE_DEVICE             = 'device'

class SourceCodeConcept(object):
    def __init__(self, source_code, original_source_code, concept_id, destination_file):
        self.source_code = source_code
        self.original_source_code = original_source_code        ## save the icd9 with the dot
        self.concept_id = concept_id
        self.destination_file = destination_file

# -----------------------------------
# Globals
# -----------------------------------
file_control = None
table_ids = None
beneficiary_dict = {}
cdm_counts_dict = {}

vocabulary_id_list = {}
source_code_concept_dict = {}
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

    provider_id_care_site_file = os.path.join(BASE_SYNPUF_INPUT_DIRECTORY,'provider_id_care_site.txt')
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
                    if care_site_id > next_care_site_id: next_care_site_id = care_site_id
        log_stats('done, recs_in={0}, len provider_id_care_site_id={1}'.format(recs_in, len(provider_id_care_site_id)))
    else:
        log_stats('No existing provider_id_care_site_file found (looked for ->' + provider_id_care_site_file + ')')

    #----------------
    # load existing npi_provider_id
    #----------------
    recs_in = 0

    global next_provider_id
    global npi_provider_id

    npi_provider_id_file = os.path.join(BASE_SYNPUF_INPUT_DIRECTORY,'npi_provider_id.txt')
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
                    if provider_id > next_provider_id: next_provider_id = provider_id
        log_stats('done, recs_in={0}, len npi_provider_id={1}'.format(recs_in, len(npi_provider_id_file)))
    else:
        log_stats('No existing npi_provider_id_file found (looked for ->' + npi_provider_id_file + ')')

    #----------------
    # Load the OMOP v5 Concept file to build the source code to conceptID xref.
    # NOTE: This version of the flat file had embedded newlines. This code handles merging the split
    #       records. This may not be needed when the final OMOP v5 Concept file is produced.
    #----------------
    omop_concept_debug_file = os.path.join(BASE_OUTPUT_DIRECTORY,'concept_debug_log.txt')
    omop_concept_file = os.path.join(BASE_OMOP_INPUT_DIRECTORY,'CONCEPT.csv')
    log_stats('Reading omop_concept_file        -> ' + omop_concept_file)
    log_stats('Writing to log file              -> ' + omop_concept_debug_file)

    global vocabulary_id_list

    #- need to use universal newlines
    with open(omop_concept_file, 'rbU') as f_omop_concept_file, open(omop_concept_debug_file, 'w') as fout_log:
        concept_csv = csv.reader(f_omop_concept_file)
        concept_csv_header = concept_csv.next()

        recs_in=0
        merged_recs=0
        recs_skipped=0
        recs_checked=0
        #-- handle "broken" records
        save_rec = ''
        try:
            for line in concept_csv:
                concept_id = concept_code = original_concept_code = vocabulary_id = domain_id = destination_file = invalid_reason = ''
                recs_in += 1
                if recs_in % 100000 == 0: print 'omop concept recs=',recs_in
                if len(line) == OMOP_CONCEPT_RECORD.fieldCount:
                    concept_id = line[OMOP_CONCEPT_RECORD.CONCEPT_ID]
                    concept_code = original_concept_code = line[OMOP_CONCEPT_RECORD.CONCEPT_CODE]
                    #concept_name  = line[OMOP_CONCEPT_RECORD.CONCEPT_NAME]
                    vocabulary_id = line[OMOP_CONCEPT_RECORD.VOCABULARY_ID]
                    domain_id = line[OMOP_CONCEPT_RECORD.DOMAIN_ID]
                    invalid_reason = line[OMOP_CONCEPT_RECORD.INVALID_REASON]
                elif save_rec != '':
                    if len(save_rec) + len(line) ==  OMOP_CONCEPT_RECORD.fieldCount + 1:
                        new_row = save_rec + line[1:]
                        line = new_row
                        merged_recs+=1
                        save_rec = ''
                        concept_id = line[OMOP_CONCEPT_RECORD.CONCEPT_ID]
                        concept_code = original_concept_code = line[OMOP_CONCEPT_RECORD.CONCEPT_CODE]
                        #concept_name  = line[OMOP_CONCEPT_RECORD.CONCEPT_NAME]
                        vocabulary_id = line[OMOP_CONCEPT_RECORD.VOCABULARY_ID]
                        domain_id = line[OMOP_CONCEPT_RECORD.DOMAIN_ID]
                        invalid_reason = line[OMOP_CONCEPT_RECORD.INVALID_REASON]
                else:
                    save_rec = line

                status=''
                if concept_id != '':
                    if vocabulary_id in [OMOP_CONSTANTS.ICD_9_DIAGNOSIS_VOCAB_ID,
                                         OMOP_CONSTANTS.ICD_9_PROCEDURES_VOCAB_ID,
                                         OMOP_CONSTANTS.CPT4_VOCABULARY_ID,
                                         OMOP_CONSTANTS.HCPCS_VOCABULARY_ID,
                                         OMOP_CONSTANTS.NDC_VOCABULARY_ID]:
                        recs_checked += 1
                        if vocabulary_id not in vocabulary_id_list: vocabulary_id_list[vocabulary_id] =0
                        vocabulary_id_list[vocabulary_id] += 1

                        if invalid_reason == '':
                            if vocabulary_id == OMOP_CONSTANTS.NDC_VOCABULARY_ID:
                                destination_file = DESTINATION_FILE_DRUG
                            elif domain_id in domain_destination_file_list:
                                destination_file = domain_destination_file_list[domain_id]
                                # store the dotless form
                                if vocabulary_id in [OMOP_CONSTANTS.ICD_9_DIAGNOSIS_VOCAB_ID, OMOP_CONSTANTS.ICD_9_PROCEDURES_VOCAB_ID]:
                                    concept_code = concept_code.replace('.','')
                            if concept_code not in source_code_concept_dict:
                                 source_code_concept_dict[concept_code] = SourceCodeConcept(concept_code, original_concept_code, concept_id, destination_file)
                        else:
                            recs_skipped += 1
                            status = 'invalid_reason not blank'
                if status != '':
                    fout_log.write(status + ': \t')
                    for fld in line: fout_log.write(fld + '\t')
                    fout_log.write('\n')
        except csv.Error as e:
            sys.exit('line %d: %s' % (concept_csv.line_num, e))

        log_stats('Done, omop concept recs_in            = ' + str(recs_in))
        log_stats('recs_checked                          = ' + str(recs_checked))
        log_stats('recs_skipped                          = ' + str(recs_skipped))
        log_stats('merged_recs                           = ' + str(merged_recs))
        log_stats('len source_code_concept_dict           = ' + str(len(source_code_concept_dict)))

        for voc in sorted(vocabulary_id_list):
            print voc, vocabulary_id_list[voc]
    log_stats('build_maps done')
    return


# -----------------------------------
# -----------------------------------
def persist_lookup_tables():
    recs_out = 0
    provider_id_care_site_file = os.path.join(BASE_SYNPUF_INPUT_DIRECTORY,'provider_id_care_site.txt')
    log_stats('writing  provider_id_care_site_file ->' + provider_id_care_site_file)
    with open(provider_id_care_site_file,'w') as fout:
        for provider_num, care_site_id in provider_id_care_site_id.items():
            fout.write('{0}\t{1}\n'.format(provider_num, care_site_id))
            recs_out += 1
    log_stats('done, recs_out={0}, len provider_id_care_site_id={1}'.format(recs_out, len(provider_id_care_site_id)))

    recs_out = 0
    npi_provider_id_file = os.path.join(BASE_SYNPUF_INPUT_DIRECTORY,'npi_provider_id.txt')
    log_stats('writing  npi_provider_id_file ->' + npi_provider_id_file)
    with open(npi_provider_id_file,'w') as fout:
        for npi, provider_id in npi_provider_id.items():
            fout.write('{0}\t{1}\n'.format(npi, provider_id))
            recs_out += 1
    log_stats('done, recs_out={0}, len npi_provider_id={1}'.format(recs_out, len(npi_provider_id)))


# -----------------------------------
# - Combine 3 beneficiary files into 1, with the year prefixed.
#  This assumes the SynPuf data is always the 3 years 2008, 2009, and 2010
# -----------------------------------
def combine_beneficiary_files(sample_directory, output_directory, sample_number):
    log_stats('-'*80)
    log_stats('combine_beneficiary_files starting: sample_number=' + str(sample_number))

    output_bene_filename = os.path.join(output_directory ,
                        'DE1_{0}'.format(sample_number),
                        'DE1_0_comb_Beneficiary_Summary_File_Sample_{0}.csv'.format(sample_number))

    #if os.path.exists(output_bene_filename):
    #    'Combined file exists; skipping .. ',output_bene_filename
    #    return

    log_stats('Writing to ->' + output_bene_filename)
    total_recs_in=0
    total_recs_out=0

    with open(output_bene_filename, 'w') as f_out:
        for year in ['2008','2009','2010']:
            input_bene_filename = os.path.join(sample_directory,
                            'DE1_{0}'.format(sample_number),
                            'DE1_0_{0}_Beneficiary_Summary_File_Sample_{1}.csv'.format(year,sample_number))
            log_stats('Reading    ->' + input_bene_filename)
            recs_in=0
            with open(input_bene_filename, 'r') as f_in:
                line = f_in.readline()  # header
                if year == '2008':
                    f_out.write('year' + ',' + line)
                for line in f_in:
                    recs_in+=1
                    if recs_in % 25000 == 0: print 'Year-{0}: records read ={1}, total written={2}'.format(year,recs_in, total_recs_out)
                    f_out.write(year + ',' + line)
                    total_recs_out+=1
            log_stats('Year-{0}: total records read ={1}'.format(year,recs_in))
            total_recs_in+=recs_in

    log_stats('Done: total records read ={0}, total records written={1}'.format(total_recs_in, total_recs_out))

# -----------------------------------
#  Load the beneficiary data
# -----------------------------------
def load_beneficiary_table(sample_number):
    global beneficiary_dict
    beneficiary_fd = file_control.get_Descriptor('beneficiary')

    log_stats('-'*80)
    log_stats('load_beneficiary_table starting')
    log_stats('input file -> '+ beneficiary_fd.complete_pathname)
    log_stats('last_person_id starting value   -> ' + str(table_ids.last_person_id))

    recs_in = 0
    with beneficiary_fd.open() as fin:
        rec = fin.readline()
        for rec in fin:
            recs_in += 1
            if recs_in % 50000 == 0: print 'beneficiary recs_in: ', recs_in
            if recs_in > MAX_RECS:break
            rec = rec.split(',')
            DESYNPUF_ID = rec[BENEFICIARY_SUMMARY_RECORD.DESYNPUF_ID]
            if DESYNPUF_ID not in beneficiary_dict:
                bene = Beneficiary(DESYNPUF_ID, table_ids.last_person_id)
                beneficiary_dict[DESYNPUF_ID] = bene
                table_ids.last_person_id += 1
            bene.AddYearData(rec)

    beneficiary_fd.increment_recs_read(recs_in)
    log_stats('last_person_id ending value -> ' + str(table_ids.last_person_id))
    log_stats('Done: total records read ={0}, unique IDs={1}'.format(recs_in, len(beneficiary_dict)))

# -----------------------------------
# Perform some simple data validation
# -----------------------------------
def check_beneficiary_table(sample_number):
    log_stats('-'*80)
    missing_year_count =0

    missing_years = {}
    log_stats('check_beneficiary_table starting')
    filename_log = os.path.join(BASE_OUTPUT_DIRECTORY,'DE1_{0}'.format(sample_number),'{0}_beneficiary_log.txt'.format(sample_number))
    filename_log_2 = os.path.join(BASE_OUTPUT_DIRECTORY,'DE1_{0}'.format(sample_number),'{0}_beneficiary_log_2.txt'.format(sample_number))
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

    person_fd.write(',')                                                                    # location_id
    person_fd.write(',')                                                                    # provider_id
    person_fd.write(',')                                                                    # care_site_id
    person_fd.write('{0},'.format(DESYNPUF_ID))                                             # person_source_value
    person_fd.write('{0},'.format(yd.BENE_SEX_IDENT_CD))                                    # gender_source_value
    person_fd.write(',')                                                                    # gender_source_concept_id
    person_fd.write('{0},'.format(yd.BENE_RACE_CD))                                         # race_source_value
    person_fd.write(',')                                                                    # race_source_concept_id
    person_fd.write('{0},'.format(yd.BENE_RACE_CD))                                         # ethnicity_source_value
    person_fd.write('')                                                                     # ethnicity_source_concept_id
    person_fd.write('\n')

    person_fd.increment_recs_written(1)

# -----------------------------------
# Observation Period
# -----------------------------------
def write_observation_period_records(beneficiary):
    obs_period_fd = file_control.get_Descriptor('observation')
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
# Death (from beneficiary record)
# -----------------------------------
def write_death_records(beneficiary):
    death_fd = file_control.get_Descriptor('death')

    yd = beneficiary.LatestYearData()
    if yd is not None and yd.BENE_DEATH_DT != '':
        death_fd.write('{0},'.format(beneficiary.person_id))
        death_fd.write('{0},'.format(get_date_YYYY_MM_DD(yd.BENE_DEATH_DT)))
        death_fd.write('{0},'.format(OMOP_CONSTANTS.DEATH_TYPE_PAYER_ENR_STATUS))
        death_fd.write('0,')                                                    # cause_concept_id
        death_fd.write(',')                                                     # cause_source_value
        ##
        ## check values
        ##
        death_fd.write('0')                                                    # cause_surce_concept_id
        death_fd.write('\n')
        death_fd.increment_recs_written(1)

# -----------------------------------
# Prescription Drug File -> Drug Exposure; Drug Cost
# -----------------------------------
def write_drug_records(beneficiary):
    drug_exp_fd = file_control.get_Descriptor('drug_exposure')
    drug_cost_fd = file_control.get_Descriptor('drug_cost')

    for raw_rec in beneficiary.prescription_records:
        rec = PrescriptionDrug(raw_rec)
        ndc_code = rec.PROD_SRVC_ID

        if ndc_code == "OTHER":                 	# Some entries have an NDC code of "OTHER", discarding these
            continue
        elif ndc_code not in source_code_concept_dict:
            # recs_NDC_not_found_skipped+=1
            # if ndc_code not in ndcs_not_found: ndcs_not_found[ndc_code]=0
            # ndcs_not_found[ndc_code]+=1
            continue

        drug_concept_id = source_code_concept_dict[ndc_code].concept_id

        drug_exp_fd.write('{0},'.format(table_ids.last_drug_exposure_id))
        drug_exp_fd.write('{0},'.format(beneficiary.person_id))
        drug_exp_fd.write('{0},'.format(drug_concept_id))
        drug_exp_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.SRVC_DT)))
        drug_exp_fd.write(',')        # drug_exposure_end_date
        drug_exp_fd.write('{0},'.format(OMOP_CONSTANTS.DRUG_TYPE_PRESCRIPTION))
        drug_exp_fd.write(',')        # stop_reason
        drug_exp_fd.write(',')        # refills
        drug_exp_fd.write('{0},'.format(rec.QTY_DSPNSD_NUM))
        drug_exp_fd.write('{0},'.format(rec.DAYS_SUPLY_NUM))
        drug_exp_fd.write(',')        # sig
        drug_exp_fd.write(',')        # route_concept_id
        drug_exp_fd.write(',')        # effective_drug_dose
        drug_exp_fd.write(',')        # dose_unit_concept_ id
        drug_exp_fd.write(',')        # lot_number
        drug_exp_fd.write(',')        # provider_id
        drug_exp_fd.write(',')        # visit_occurrence_id               *******
        drug_exp_fd.write(',')        # drug_source_value
        drug_exp_fd.write(',')        # route_source_value
        drug_exp_fd.write('\n')
        drug_exp_fd.increment_recs_written(1)
        table_ids.last_drug_exposure_id += 1

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
        drug_cost_fd.write('{0},'.format(rec.TOT_RX_CST_AMT))            # total_paid
        drug_cost_fd.write(',')                                          # ingredient_cost
        drug_cost_fd.write(',')                                          # dispensing_fee
        drug_cost_fd.write(',')                                          # average_wholesale_price
        drug_cost_fd.write(',')                                           # payer_plan_period_id                  ##### LOOKUP in PAYER-PLAN-PERIOD-TBL
        drug_cost_fd.write('\n')
        drug_cost_fd.increment_recs_written(1)
        table_ids.last_drug_cost_id += 1

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
        # blech...index math to derive conceptid !          ############ FIX #########
        # ip_proc_offset = len(rec.ICD9_DGNS_CD_list)
        i = 0
        for code in rec.ICD9_DGNS_CD_list + rec.ICD9_PRCDR_CD_list + rec.HCPCS_CD_list:
            if code in source_code_concept_dict:
                source_code_concept = source_code_concept_dict[code]
                if source_code_concept.destination_file == DESTINATION_FILE_PROCEDURE:
                    proc_occur_fd.write('{0},'.format(table_ids.last_procedure_occurrence_id))
                    proc_occur_fd.write('{0},'.format(beneficiary.person_id))
                    proc_occur_fd.write('{0},'.format(source_code_concept.concept_id))                           # procedure_concept_id
                    proc_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_FROM_DT)))
                    # procedure_type_concept_id
                    # Inpatient header - nth position from OMOP Vocab v4.4
                    proc_occur_fd.write('{0},'.format(OMOP_CONSTANTS.INPAT_PROCEDURE_1ST_POSITION + i))
                    proc_occur_fd.write(',')                                          # qualifier_concept_id
                    proc_occur_fd.write(',')                                          # quantity
                    proc_occur_fd.write(',')                                          # provider_id
                    proc_occur_fd.write(',')                                          # visit_occurrence_id
                    proc_occur_fd.write('{0},'.format(source_code_concept.original_source_code))                          # procedure_source_value
                    proc_occur_fd.write(',')                                          # procedure_source_concept_id
                    proc_occur_fd.write('')                                           # qualifier_source_value
                    proc_occur_fd.write('\n')
                    proc_occur_fd.increment_recs_written(1)
                    table_ids.last_procedure_occurrence_id += 1

                elif source_code_concept.destination_file == DESTINATION_FILE_CONDITION:
                    cond_occur_fd.write('{0},'.format(table_ids.last_condition_occurrence_id))
                    cond_occur_fd.write('{0},'.format(beneficiary.person_id))
                    cond_occur_fd.write('{0},'.format(source_code_concept.concept_id))                  # condition_concept_id
                    cond_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_FROM_DT)))
                    cond_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_THRU_DT)))

                    # condition_type_concept_id
                    # Inpatient header - nth position from OMOP Vocab v4.4
                    cond_occur_fd.write('{0},'.format(OMOP_CONSTANTS.INPAT_CONDITION_1ST_POSITION + i))
                    cond_occur_fd.write(',')                                          # stop_reason
                    cond_occur_fd.write(',')                                          # provider_id
                    cond_occur_fd.write(',')                                          # visit_occurrence_id                   ######
                    cond_occur_fd.write('{0},'.format(source_code_concept.original_source_code))                          # condition_source_value
                    cond_occur_fd.write(',')                                          # condition_source_concept_id
                    cond_occur_fd.write('\n')
                    cond_occur_fd.increment_recs_written(1)
                    table_ids.last_condition_occurrence_id += 1

                elif source_code_concept.destination_file == DESTINATION_FILE_DRUG:
                    drug_exp_fd.write('{0},'.format(table_ids.last_drug_exposure_id))
                    drug_exp_fd.write('{0},'.format(beneficiary.person_id))
                    drug_exp_fd.write('{0},'.format(source_code_concept.concept_id))
                    drug_exp_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_FROM_DT)))
                    drug_exp_fd.write(',')        # drug_exposure_end_date
                    drug_exp_fd.write('{0},'.format(OMOP_CONSTANTS.DRUG_TYPE_PRESCRIPTION))
                    drug_exp_fd.write(',')        # stop_reason
                    drug_exp_fd.write(',')        # refills
                    drug_exp_fd.write(',')        # quantity
                    drug_exp_fd.write(',')        # days supply
                    drug_exp_fd.write(',')        # sig
                    drug_exp_fd.write(',')        # route_concept_id
                    drug_exp_fd.write(',')        # effective_drug_dose
                    drug_exp_fd.write(',')        # dose_unit_concept_ id
                    drug_exp_fd.write(',')        # lot_number
                    drug_exp_fd.write(',')        # provider_id
                    drug_exp_fd.write(',')        # visit_occurrence_id               *******
                    drug_exp_fd.write(',')        # drug_source_value
                    drug_exp_fd.write(',')        # route_source_value
                    drug_exp_fd.write('\n')
                    drug_exp_fd.increment_recs_written(1)
                    table_ids.last_drug_exposure_id += 1

                elif source_code_concept.destination_file == DESTINATION_FILE_MEASUREMENT:

                # measurement_id, person_id, measurement_concept_id, measurement_date,
                # measurement_time, measurement_type_concept_id, operator_concept_id,value_as_number,
                # value_as_concept_id, unit_concept_id, range_low, range_high,
                # provider_id, visit_occurrence_id, measurement_source_value,
                # measurement_source_concept_id, unit_source_value, value_source_value
                    measurement_fd.write('{0},'.format(table_ids.last_measurement_id))
                    measurement_fd.write('{0},'.format(beneficiary.person_id))
                    measurement_fd.write('{0},'.format(source_code_concept.concept_id))                  # condition_concept_id
                    measurement_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_FROM_DT)))
                    measurement_fd.write('\n')
                    measurement_fd.increment_recs_written(1)
                    table_ids.last_measurement_id += 1

                elif source_code_concept.destination_file == DESTINATION_FILE_OBSERVATION:
                # observation_id, person_id, observation_concept_id, observation_date, observation_time,
                # observation_type_concept_id, value_as_number,value_as_string, value_as_concept_id, qualifier_concept_id,
                # unit_concept_id, provider_id, visit_occurrence_id, observation_source_value,observation_source_concept_id,
                # unit_source_value, qualifier_source_value
                    observation_fd.write('{0},'.format(table_ids.last_observation_id))
                    observation_fd.write('{0},'.format(beneficiary.person_id))
                    observation_fd.write('{0},'.format(source_code_concept.concept_id))                  # condition_concept_id
                    observation_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_FROM_DT)))
                    observation_fd.write('\n')
                    observation_fd.increment_recs_written(1)
                    table_ids.last_observation_id += 1

                elif source_code_concept.destination_file == DESTINATION_FILE_DEVICE:
                # device_exposure_id, person_id, device_concept_id, device_exposure_start_date, device_exposure_end_date,
                # device_type_concept_id,unique_device_id, quantity, provider_id, visit_occurrence_id, device_source_value, device_source_concept_id
                    device_fd.write('{0},'.format(table_ids.last_device_exposure_id))
                    device_fd.write('{0},'.format(beneficiary.person_id))
                    device_fd.write('{0},'.format(source_code_concept.concept_id))                  # condition_concept_id
                    device_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_FROM_DT)))
                    device_fd.write('\n')
                    device_fd.increment_recs_written(1)
                    table_ids.last_device_exposure_id += 1

                #check for death
                if code in icd9_codes_death:
                    yd = beneficiary.LatestYearData()
                    death_fd.write('{0},'.format(beneficiary.person_id))
                    death_fd.write('{0},'.format(get_date_YYYY_MM_DD(yd.BENE_DEATH_DT)))
                    death_fd.write('{0},'.format(OMOP_CONSTANTS.DEATH_TYPE_CONDITION))
                    death_fd.write('0,')                                                    # cause_concept_id
                    death_fd.write(',')                                                     # cause_source_value
                    ##
                    ## check value
                    ##
                    death_fd.write('0')                                                    # cause_surce_concept_id
                    death_fd.write('\n')
                    death_fd.increment_recs_written(1)

        #-- care site / provider
        care_site_id = 0
        if rec.PRVDR_NUM != '':
            care_site_id = get_CareSite(rec.PRVDR_NUM)
            care_site_fd.write('{0},'.format(care_site_id))
            care_site_fd.write(',')                                                                 # care_site_name
            care_site_fd.write('{0},'.format(OMOP_CONSTANTS.INPATIENT_PLACE_OF_SERVICE))            # place_of_service_concept_id
            care_site_fd.write(',')                                                                 # location_id
            care_site_fd.write('{0},'.format(rec.PRVDR_NUM))                                        # care_site_source_value
            care_site_fd.write('{0}'.format(OMOP_CONSTANTS.INPATIENT_PLACE_OF_SERVICE_SOURCE))      # place_of_service_source_value
            care_site_fd.write('\n')
            care_site_fd.increment_recs_written(1)

        #-- provider
        for npi in (rec.AT_PHYSN_NPI, rec.OP_PHYSN_NPI, rec.OT_PHYSN_NPI):
            if npi != '':
                provider_id = get_Provider(npi)
                provider_fd.write('{0},'.format(provider_id))
                provider_fd.write(',')                                                            # provider_name
                provider_fd.write('{0},'.format(npi))                                             # npi
                provider_fd.write(',')                                                            # dea
                provider_fd.write('0,')                                                           # specialty_concept_id
                provider_fd.write('{0},'.format(care_site_id))
                provider_fd.write(',')                                                            # year_of_birth
                provider_fd.write('0,')                                                           # gender_concept_id
                provider_fd.write(',')                                                            # provider_source_value
                provider_fd.write(',')                                                            # specialty_source_value
                provider_fd.write('0,')                                                           # specialty_source_concept_id
                provider_fd.write(',')                                                            # gender_source_value
                provider_fd.write('0')                                                           # gender_source_concept_id
                provider_fd.write('\n')
                provider_fd.increment_recs_written(1)


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
        for code in list(rec.ADMTNG_ICD9_DGNS_CD) + rec.ICD9_DGNS_CD_list + rec.ICD9_PRCDR_CD_list + rec.HCPCS_CD_list:
            if code in source_code_concept_dict:
                source_code_concept = source_code_concept_dict[code]
                if source_code_concept.destination_file == DESTINATION_FILE_PROCEDURE:
                    proc_occur_fd.write('{0},'.format(table_ids.last_procedure_occurrence_id))
                    proc_occur_fd.write('{0},'.format(beneficiary.person_id))
                    proc_occur_fd.write('{0},'.format(source_code_concept.concept_id))                           # procedure_concept_id

                    proc_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_FROM_DT)))

                    # procedure_type_concept_id
                    # outpatient header - nth position from OMOP Vocab v4.4
                    proc_occur_fd.write('{0},'.format(OMOP_CONSTANTS.OUTPAT_PROCEDURE_1ST_POSITION + i))
                    proc_occur_fd.write(',')                                          # qualifier_concept_id
                    proc_occur_fd.write(',')                                          # quantity
                    proc_occur_fd.write(',')                                          # provider_id
                    proc_occur_fd.write(',')                                          # visit_occurrence_id
                    proc_occur_fd.write('{0},'.format(source_code_concept.original_source_code))                          # procedure_source_value
                    proc_occur_fd.write(',')                                          # procedure_source_concept_id
                    proc_occur_fd.write('')                                           # qualifier_source_value
                    proc_occur_fd.write('\n')
                    proc_occur_fd.increment_recs_written(1)
                    table_ids.last_procedure_occurrence_id += 1

                elif source_code_concept.destination_file == DESTINATION_FILE_CONDITION:
                    cond_occur_fd.write('{0},'.format(table_ids.last_condition_occurrence_id))
                    cond_occur_fd.write('{0},'.format(beneficiary.person_id))
                    cond_occur_fd.write('{0},'.format(source_code_concept.concept_id))                           # condition_concept_id
                    cond_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_FROM_DT)))
                    cond_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_THRU_DT)))

                    # condition_type_concept_id
                    # outpatient header - nth position from OMOP Vocab v4.4
                    cond_occur_fd.write('{0},'.format(OMOP_CONSTANTS.OUTPAT_CONDITION_1ST_POSITION + i))
                    cond_occur_fd.write(',')                                          # stop_reason
                    cond_occur_fd.write(',')                                          # provider_id
                    cond_occur_fd.write(',')                                          # visit_occurrence_id                   ######
                    cond_occur_fd.write('{0},'.format(source_code_concept.original_source_code))                          # condition_source_value
                    cond_occur_fd.write(',')                                          # condition_source_concept_id
                    cond_occur_fd.write('\n')
                    cond_occur_fd.increment_recs_written(1)
                    table_ids.last_condition_occurrence_id += 1

                elif source_code_concept.destination_file == DESTINATION_FILE_DRUG:
                    drug_exp_fd.write('{0},'.format(table_ids.last_drug_exposure_id))
                    drug_exp_fd.write('{0},'.format(beneficiary.person_id))
                    drug_exp_fd.write('{0},'.format(source_code_concept.concept_id))
                    drug_exp_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_FROM_DT)))
                    drug_exp_fd.write(',')        # drug_exposure_end_date
                    drug_exp_fd.write('{0},'.format(OMOP_CONSTANTS.DRUG_TYPE_PRESCRIPTION))
                    drug_exp_fd.write(',')        # stop_reason
                    drug_exp_fd.write(',')        # refills
                    drug_exp_fd.write(',')        # quantity
                    drug_exp_fd.write(',')        # days supply
                    drug_exp_fd.write(',')        # sig
                    drug_exp_fd.write(',')        # route_concept_id
                    drug_exp_fd.write(',')        # effective_drug_dose
                    drug_exp_fd.write(',')        # dose_unit_concept_ id
                    drug_exp_fd.write(',')        # lot_number
                    drug_exp_fd.write(',')        # provider_id
                    drug_exp_fd.write(',')        # visit_occurrence_id               *******
                    drug_exp_fd.write(',')        # drug_source_value
                    drug_exp_fd.write(',')        # route_source_value
                    drug_exp_fd.write('\n')
                    drug_exp_fd.increment_recs_written(1)
                    table_ids.last_drug_exposure_id += 1

                elif source_code_concept.destination_file == DESTINATION_FILE_MEASUREMENT:
                    measurement_fd.write('{0},'.format(table_ids.last_measurement_id))
                    measurement_fd.write('{0},'.format(beneficiary.person_id))
                    measurement_fd.write('{0},'.format(source_code_concept.concept_id))                  # condition_concept_id
                    measurement_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_FROM_DT)))
                    measurement_fd.write('\n')
                    measurement_fd.increment_recs_written(1)
                    table_ids.last_measurement_id += 1

                elif source_code_concept.destination_file == DESTINATION_FILE_OBSERVATION:
                    observation_fd.write('{0},'.format(table_ids.last_observation_id))
                    observation_fd.write('{0},'.format(beneficiary.person_id))
                    observation_fd.write('{0},'.format(source_code_concept.concept_id))                  # condition_concept_id
                    observation_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_FROM_DT)))
                    observation_fd.write('\n')
                    observation_fd.increment_recs_written(1)
                    table_ids.last_observation_id += 1

                elif source_code_concept.destination_file == DESTINATION_FILE_DEVICE:
                    device_fd.write('{0},'.format(table_ids.last_device_exposure_id))
                    device_fd.write('{0},'.format(beneficiary.person_id))
                    device_fd.write('{0},'.format(source_code_concept.concept_id))                  # condition_concept_id
                    device_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_FROM_DT)))
                    device_fd.write('\n')
                    device_fd.increment_recs_written(1)
                    table_ids.last_device_exposure_id += 1

                #check for death
                if code in icd9_codes_death:
                    yd = beneficiary.LatestYearData()
                    death_fd.write('{0},'.format(beneficiary.person_id))
                    death_fd.write('{0},'.format(get_date_YYYY_MM_DD(yd.BENE_DEATH_DT)))
                    death_fd.write('{0},'.format(OMOP_CONSTANTS.DEATH_TYPE_CONDITION))
                    death_fd.write('0,')                                                    # cause_concept_id
                    death_fd.write(',')                                                     # cause_source_value
                    ##
                    ## check value
                    ##
                    death_fd.write('0')                                                    # cause_surce_concept_id
                    death_fd.write('\n')
                    death_fd.increment_recs_written(1)

        #-- care site / provider
        care_site_id = 0
        if rec.PRVDR_NUM != '':
            care_site_id = get_CareSite(rec.PRVDR_NUM)
            care_site_fd.write('{0},'.format(care_site_id))
            care_site_fd.write(',')                                                                 # care_site_name
            care_site_fd.write('{0},'.format(OMOP_CONSTANTS.OUTPATIENT_PLACE_OF_SERVICE))            # place_of_service_concept_id
            care_site_fd.write(',')                                                                 # location_id
            care_site_fd.write('{0},'.format(rec.PRVDR_NUM))                                   # care_site_source_value
            care_site_fd.write('{0}'.format(OMOP_CONSTANTS.OUTPATIENT_PLACE_OF_SERVICE_SOURCE))     # place_of_service_source_value
            care_site_fd.write('\n')
            care_site_fd.increment_recs_written(1)

        #-- provider
        for npi in (rec.AT_PHYSN_NPI, rec.OP_PHYSN_NPI, rec.OT_PHYSN_NPI):
            if npi != '':
                provider_id = get_Provider(npi)
                provider_fd.write('{0},'.format(provider_id))
                provider_fd.write(',')                                                            # provider_name
                provider_fd.write('{0},'.format(npi))
                provider_fd.write(',')                                                            # dea
                provider_fd.write('0,')                                                           # specialty_concept_id
                provider_fd.write('{0},'.format(care_site_id))
                provider_fd.write(',')                                                            # year_of_birth
                provider_fd.write('0,')                                                           # gender_concept_id
                provider_fd.write(',')                                                            # provider_source_value
                provider_fd.write(',')                                                            # specialty_source_value
                provider_fd.write('0,')                                                           # specialty_source_concept_id
                provider_fd.write(',')                                                            # gender_source_value
                provider_fd.write('0')                                                            # gender_source_concept_id
                provider_fd.write('\n')
                provider_fd.increment_recs_written(1)

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
        for code in rec.ICD9_DGNS_CD_list  + rec.HCPCS_CD_list + rec.LINE_ICD9_DGNS_CD_list:
            if code in source_code_concept_dict:
                source_code_concept = source_code_concept_dict[code]
                if source_code_concept.destination_file == DESTINATION_FILE_PROCEDURE:
                    proc_occur_fd.write('{0},'.format(table_ids.last_procedure_occurrence_id))
                    proc_occur_fd.write('{0},'.format(beneficiary.person_id))
                    proc_occur_fd.write('{0},'.format(source_code_concept.concept_id))                           # procedure_concept_id
                    proc_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_FROM_DT)))
                    # procedure_type_concept_id
                    # carrier header - nth position from OMOP Vocab v4.4
                    proc_occur_fd.write('{0},'.format(OMOP_CONSTANTS.OUTPAT_PROCEDURE_1ST_POSITION + i))
                    proc_occur_fd.write(',')                                          # qualifier_concept_id
                    proc_occur_fd.write(',')                                          # quantity
                    proc_occur_fd.write(',')                                          # provider_id
                    proc_occur_fd.write(',')                                          # visit_occurrence_id
                    proc_occur_fd.write('{0},'.format(source_code_concept.original_source_code))                          # procedure_source_value
                    proc_occur_fd.write(',')                                          # procedure_source_concept_id
                    proc_occur_fd.write('')                                           # qualifier_source_value
                    proc_occur_fd.write('\n')
                    proc_occur_fd.increment_recs_written(1)
                    table_ids.last_procedure_occurrence_id += 1

                elif source_code_concept.destination_file == DESTINATION_FILE_CONDITION:
                    cond_occur_fd.write('{0},'.format(table_ids.last_condition_occurrence_id))
                    cond_occur_fd.write('{0},'.format(beneficiary.person_id))
                    cond_occur_fd.write('{0},'.format(source_code_concept.concept_id))                           # condition_concept_id
                    cond_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_FROM_DT)))
                    cond_occur_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_THRU_DT)))
                    # condition_type_concept_id
                    # carrier header - nth position from OMOP Vocab v4.4
                    cond_occur_fd.write('{0},'.format(OMOP_CONSTANTS.OUTPAT_CONDITION_1ST_POSITION + i))
                    cond_occur_fd.write(',')                                          # stop_reason
                    cond_occur_fd.write(',')                                          # provider_id
                    cond_occur_fd.write(',')                                          # visit_occurrence_id                   ######
                    cond_occur_fd.write('{0},'.format(source_code_concept.original_source_code))                          # condition_source_value
                    cond_occur_fd.write(',')                                          # condition_source_concept_id
                    cond_occur_fd.write('\n')
                    cond_occur_fd.increment_recs_written(1)
                    table_ids.last_condition_occurrence_id += 1

                elif source_code_concept.destination_file == DESTINATION_FILE_DRUG:
                    drug_exp_fd.write('{0},'.format(table_ids.last_drug_exposure_id))
                    drug_exp_fd.write('{0},'.format(beneficiary.person_id))
                    drug_exp_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_FROM_DT)))
                    drug_exp_fd.write(',')        # drug_exposure_end_date
                    drug_exp_fd.write('{0},'.format(OMOP_CONSTANTS.DRUG_TYPE_PRESCRIPTION))
                    drug_exp_fd.write(',')        # stop_reason
                    drug_exp_fd.write(',')        # refills
                    drug_exp_fd.write(',')        # quantity
                    drug_exp_fd.write(',')        # days_supply
                    drug_exp_fd.write(',')        # sig
                    drug_exp_fd.write(',')        # route_concept_id
                    drug_exp_fd.write(',')        # effective_drug_dose
                    drug_exp_fd.write(',')        # dose_unit_concept_ id
                    drug_exp_fd.write(',')        # lot_number
                    drug_exp_fd.write(',')        # provider_id
                    drug_exp_fd.write(',')        # visit_occurrence_id               *******
                    drug_exp_fd.write(',')        # drug_source_value
                    drug_exp_fd.write('{0},'.format(source_code_concept.concept_id))
                    drug_exp_fd.write(',')        # route_source_value
                    drug_exp_fd.write('\n')
                    drug_exp_fd.increment_recs_written(1)
                    table_ids.last_drug_exposure_id += 1

                elif source_code_concept.destination_file == DESTINATION_FILE_MEASUREMENT:
                    measurement_fd.write('{0},'.format(table_ids.last_measurement_id))
                    measurement_fd.write('{0},'.format(beneficiary.person_id))
                    measurement_fd.write('{0},'.format(source_code_concept.concept_id))                  # condition_concept_id
                    measurement_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_FROM_DT)))
                    measurement_fd.write('\n')
                    measurement_fd.increment_recs_written(1)
                    table_ids.last_measurement_id += 1

                elif source_code_concept.destination_file == DESTINATION_FILE_OBSERVATION:
                    observation_fd.write('{0},'.format(table_ids.last_observation_id))
                    observation_fd.write('{0},'.format(beneficiary.person_id))
                    observation_fd.write('{0},'.format(source_code_concept.concept_id))                  # condition_concept_id
                    observation_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_FROM_DT)))
                    observation_fd.write('\n')
                    observation_fd.increment_recs_written(1)
                    table_ids.last_observation_id += 1

                elif source_code_concept.destination_file == DESTINATION_FILE_DEVICE:
                    device_fd.write('{0},'.format(table_ids.last_device_exposure_id))
                    device_fd.write('{0},'.format(beneficiary.person_id))
                    device_fd.write('{0},'.format(source_code_concept.concept_id))                  # condition_concept_id
                    device_fd.write('{0},'.format(get_date_YYYY_MM_DD(rec.CLM_FROM_DT)))
                    device_fd.write('\n')
                    device_fd.increment_recs_written(1)
                    table_ids.last_device_exposure_id += 1

                #check for death
                if code in icd9_codes_death:
                    yd = beneficiary.LatestYearData()
                    death_fd.write('{0},'.format(beneficiary.person_id))
                    death_fd.write('{0},'.format(get_date_YYYY_MM_DD(yd.BENE_DEATH_DT)))
                    death_fd.write('{0},'.format(OMOP_CONSTANTS.DEATH_TYPE_CONDITION))
                    death_fd.write('0,')                                                    # cause_concept_id
                    death_fd.write(',')                                                     # cause_source_value
                    ##
                    ## check value
                    ##
                    death_fd.write('0')                                                    # cause_surce_concept_id
                    death_fd.write('\n')
                    death_fd.increment_recs_written(1)

        #-- care site / provider
        for cc_line in rec.CarrierClaimLine_list:
            care_site_id = 0
            if cc_line.TAX_NUM != '':
                care_site_id = get_CareSite(cc_line.TAX_NUM)
                care_site_fd.write('{0},'.format(care_site_id))
                care_site_fd.write('{0},'.format(table_ids.last_procedure_occurrence_id))             #### CHANGE !!!
                care_site_fd.write(',')                                                               # care_site_name
                care_site_fd.write('{0},'.format(OMOP_CONSTANTS.OUTPATIENT_PLACE_OF_SERVICE))         # place_of_service_concept_id
                care_site_fd.write(',')                                                               # location_id
                care_site_fd.write('{0},'.format(cc_line.TAX_NUM))                                    # care_site_source_value
                care_site_fd.write('{0}'.format(OMOP_CONSTANTS.OUTPATIENT_PLACE_OF_SERVICE_SOURCE))   # place_of_service_source_value
                care_site_fd.write('\n')
                care_site_fd.increment_recs_written(1)

            if cc_line.PRF_PHYSN_NPI != '':
                provider_id = get_Provider(cc_line.PRF_PHYSN_NPI)
                provider_fd.write('{0},'.format(provider_id))
                provider_fd.write(',')                                                            # provider_name
                provider_fd.write('{0},'.format(cc_line.PRF_PHYSN_NPI))                           # npi
                provider_fd.write(',')                                                            # dea
                provider_fd.write('0,')                                                           # specialty_concept_id
                provider_fd.write('{0},'.format(care_site_id))
                provider_fd.write(',')                                                            # year_of_birth
                provider_fd.write('0,')                                                           # gender_concept_id
                provider_fd.write(',')                                                            # provider_source_value
                provider_fd.write(',')                                                            # specialty_source_value
                provider_fd.write('0,')                                                           # specialty_source_concept_id
                provider_fd.write(',')                                                            # gender_source_value
                provider_fd.write('0')                                                           # gender_source_concept_id
                provider_fd.write('\n')
                provider_fd.increment_recs_written(1)

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
                proc_cost_fd.write(',')                                                           # revenue_code_source_value
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



#---------------------------------
#---------------------------------
if __name__ == '__main__':
    if not os.path.exists(BASE_OUTPUT_DIRECTORY): os.makedirs(BASE_OUTPUT_DIRECTORY)

    parser = argparse.ArgumentParser(description='Enter Sample Number')
    parser.add_argument('sample_number', type=int)
    args = parser.parse_args()

    SAMPLE_RANGE = [args.sample_number]
    current_stats_filename = os.path.join(BASE_OUTPUT_DIRECTORY,'etl_stats.txt_{0}'.format(args.sample_number))
    if os.path.exists(current_stats_filename): os.unlink(current_stats_filename)

    log_stats('CMS_ETL starting')
    log_stats('BASE_SYNPUF_INPUT_DIRECTORY     =' + BASE_SYNPUF_INPUT_DIRECTORY)
    log_stats('BASE_OUTPUT_DIRECTORY           =' + BASE_OUTPUT_DIRECTORY)

    #- get any existing Last-Table-IDs
    table_ids = Table_ID_Values()
    table_ids_filename = os.path.join(BASE_OUTPUT_DIRECTORY, 'etl_synpuf_last_table_ids.txt')
    if os.path.exists(table_ids_filename):
        table_ids.Load(table_ids_filename, log_stats)

    # Build mappings between SynPUF codes and OMOP Vocabulary concept_ids
    build_maps()

    # Build the object to manage access to all the files
    file_control = FileControl(BASE_SYNPUF_INPUT_DIRECTORY, BASE_OUTPUT_DIRECTORY, args.sample_number)
    file_control.delete_all_output()

    load_beneficiary_table(args.sample_number)
    bene_dump_filename = os.path.join(BASE_OUTPUT_DIRECTORY,'beneficiary_dump_{0}.txt'.format(args.sample_number))

    write_header_records()

    with open(bene_dump_filename,'w') as fout:
        for ix,DESYNPUF_ID in enumerate(sorted(beneficiary_dict.keys())):
            # print '-'*80
            # print '--> ', DESYNPUF_ID
            ben = beneficiary_dict[DESYNPUF_ID]
            ben.LoadClaimData(file_control)

            print '[{0}]{1:20} ip={2:5}, op={3:5}, cc={4:5}, rx={5:}'.format(ix, DESYNPUF_ID, len(ben.inpatient_records),len(ben.outpatient_records),len(ben.carrier_records), len(ben.prescription_records))

            write_person_record(ben)
            write_observation_period_records(ben)
            write_death_records(ben)
            write_drug_records(ben)
            process_inpatient_records(ben)
            process_outpatient_records(ben)
            process_carrier_records(ben)

            # dump_beneficiary_records(fout, rec)

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
        log_stats('\tFile: {0:50}, records_written={1:10}'.format(fd.token, fd.records_written))


    print '** done **'
