# Python-based ETL of SynPUF data to CDMv5-compatible CSV files

This is an implementation of the SynPUF Extract-Transform-Load (ETL)
specification designed to generate a set of CDMv5-compatible CSV files
that can then be bulk-loaded into your RDBMS of choice.

UNM Improvements branch: The programs have been modified and tested
with CMS data DE_1 - DE_20 and are ready to be used by the general
public.

## Overview of Steps

0) Shortcut: download ready-to-go data

1) Install required software

2) Download SynPUF input data

3) Download CDMv5 Vocabulary files

4) Setup the .env file to specify file locations

5) Test ETL with DE\_0 CMS test data

6) Run ETL on CMS data

7) Load data into the database

8) Open issues and caveats with the ETL

Further instructions on how to set up the Postgres database can be found [here](postgres_instructions.md).

## 0. Shortcut: download ready-to-go data

### CDM V5.2
We have prepared a downloadable OMOP CDMv5.2 version.  The data can be retrieved from [Google Drive](https://drive.google.com/file/d/1xWmuVqlIaUsY08OgrKIt8WAsfaq_iCrG/view?usp=sharing).  The file is called synpuf_100k.tar.gz. It is approximately 3 GB in size. It contains synthetic v5.2 CDM data files for 100,000 persons (i.e. a sample of the 2M patients in SYNPUF). The .gz files in this file will need to be extracted and decompressed after download. The decompressed files have no file suffixes but they are comma delimited text files. There are no header records in the files. The CDM vocabulary version is "v5.0 05-NOV-17".
 
Here is an example PostgreSQL psql client copy command to load the concept file into the concept table in a schema called 'cdm'. 
 
\copy cdm.concept from '/download_directory_path/concept' null as '\\000' delimiter ','

The CDM V5.2 was built by using the original V5.0 BUILDER found in this GitHub and then running transform scripts found [here](https://github.com/OHDSI/ETL-CMS/tree/updateSynpufVersion/hand_conversion/V5.0_TO_V5.2_SCRIPT).

You are completed after this step.  The other steps are for the older version below or if you want to run yourself.

### CDM V5.0 (INITIAL VERSION) download ready-to-go data, download vocabulary files (step 3), skip to step 7
We have prepared downloadable OMOP CDMv5 .csv files of the database
tables that were created via steps 1-7 below. The data can be retrieved from
[ftp://ftp.ohdsi.org/synpuf](ftp://ftp.ohdsi.org/synpuf). The file
[synpuf_1.zip](ftp://ftp.ohdsi.org/synpuf/synpuf_1.zip) contains tables for the first 20th of the data (116,362 patients), and might be
suitable for smaller-scale testing. The remaining 19 .csv.gz files represent
the table data for all 20 parts (2,326,856 patients). Here are the direct links
and md5sums for the files:

- ``da0e310e7313e7b4894d1b1a15aee674``  [ftp://ftp.ohdsi.org/synpuf/synpuf_1.zip](ftp://ftp.ohdsi.org/synpuf/synpuf_1.zip)
- ``839c0df1f625bff74aba3fed07e4375f``  [ftp://ftp.ohdsi.org/synpuf/care_site.csv.gz](ftp://ftp.ohdsi.org/synpuf/care_site.csv.gz)
- ``fad02821bc7369385882b0fd403580e2``  [ftp://ftp.ohdsi.org/synpuf/condition_occurrence.csv.gz](ftp://ftp.ohdsi.org/synpuf/condition_occurrence.csv.gz)
- ``3419aaa30fc9ebc7a605be7c5cf654fb``  [ftp://ftp.ohdsi.org/synpuf/death.csv.gz](ftp://ftp.ohdsi.org/synpuf/death.csv.gz)
- ``4a5587d391763072c988d5c264d44b69``  [ftp://ftp.ohdsi.org/synpuf/device_cost.csv.gz](ftp://ftp.ohdsi.org/synpuf/device_cost.csv.gz)
- ``b60d19898934d17f0bc08e3a260e83f7``  [ftp://ftp.ohdsi.org/synpuf/device_exposure.csv.gz](ftp://ftp.ohdsi.org/synpuf/device_exposure.csv.gz)
- ``37901c540feef6b8a4179d0e18438dae``  [ftp://ftp.ohdsi.org/synpuf/drug_cost.csv.gz](ftp://ftp.ohdsi.org/synpuf/drug_cost.csv.gz)
- ``bbd07537a247aad7f690f71bfeabd6a6``  [ftp://ftp.ohdsi.org/synpuf/drug_exposure.csv.gz](ftp://ftp.ohdsi.org/synpuf/drug_exposure.csv.gz)
- ``40036fc2d6fe24378fd55158718e8a54``  [ftp://ftp.ohdsi.org/synpuf/location.csv.gz](ftp://ftp.ohdsi.org/synpuf/location.csv.gz)
- ``bbd3c060b7ba2454f5bdd8cae589ca61``  [ftp://ftp.ohdsi.org/synpuf/measurement_occurrence.csv.gz](ftp://ftp.ohdsi.org/synpuf/measurement_occurrence.csv.gz)
- ``36b9525a151c95e9119c19dc96a94f5c``  [ftp://ftp.ohdsi.org/synpuf/observation.csv.gz](ftp://ftp.ohdsi.org/synpuf/observation.csv.gz)
- ``1cb344499f316b929aec4f117700511a``  [ftp://ftp.ohdsi.org/synpuf/observation_period.csv.gz](ftp://ftp.ohdsi.org/synpuf/observation_period.csv.gz)
- ``55b81fab86dc088443e0189ba4b70fdb``  [ftp://ftp.ohdsi.org/synpuf/payer_plan_period.csv.gz](ftp://ftp.ohdsi.org/synpuf/payer_plan_period.csv.gz)
- ``3ab936bb7da41c4bc9c0dddf9daac42c``  [ftp://ftp.ohdsi.org/synpuf/person.csv.gz](ftp://ftp.ohdsi.org/synpuf/person.csv.gz)
- ``5927a6509ef27e5f52c7ec1c3d86cbc9``  [ftp://ftp.ohdsi.org/synpuf/procedure_cost.csv.gz](ftp://ftp.ohdsi.org/synpuf/procedure_cost.csv.gz)
- ``1812775a95484646c1fd92d515e3b516``  [ftp://ftp.ohdsi.org/synpuf/procedure_occurrence.csv.gz](ftp://ftp.ohdsi.org/synpuf/procedure_occurrence.csv.gz)
- ``110c5fd05bc155eaa755e2e55ac7d0bf``  [ftp://ftp.ohdsi.org/synpuf/provider.csv.gz](ftp://ftp.ohdsi.org/synpuf/provider.csv.gz)
- ``207057ec59a57edf7596b12d393b0f63``  [ftp://ftp.ohdsi.org/synpuf/specimen.csv.gz](ftp://ftp.ohdsi.org/synpuf/specimen.csv.gz)
- ``d48a8ab8155736d2a38c2feb7b82eb53``  [ftp://ftp.ohdsi.org/synpuf/visit_cost.csv.gz](ftp://ftp.ohdsi.org/synpuf/visit_cost.csv.gz)
- ``c1529ea8b4e4e092d0cdd2600ea61c75``  [ftp://ftp.ohdsi.org/synpuf/visit_occurrence.csv.gz](ftp://ftp.ohdsi.org/synpuf/visit_occurrence.csv.gz)


After retrieving the data you can perform step 3 to obtain the vocabulary files, then skip to steps 7-8 to create the CDMv5 tables and load the data. 


## 1. Install required software

The ETL process requires Python 2.7 with the python-dotenv package.

### Linux

Python 2.7 and python-pip must be installed if they are not already
present. If you are using a RedHat distribution the following commands
will work (you must have system administrator privileges):

``sudo yum install python``  
``sudo yum install python-pip``

The python-pip package enables a non-administrative user to install
python packages for their personal use. From the python_etl directory
run the following command to install the python-dotenv package:

``pip install -r requirements.txt``

### Windows + Cygwin

We have been able to run the ETL under Windows using cygwin, available at
<https://www.cygwin.com>. Be sure to install the following packages
with the cygwin installer:

python  
python-setuptools  

After that run the following in order to install pip:  
``easy_install-2.7 pip``

Then to install python-dotenv, run the following command within the python\_etl folder:  
``pip install -r requirements.txt``


## 2. Download SynPUF input data
The SynPUF data is divided into 20 parts (8 files per part), and the files for each part should be saved in respective directories DE_1 through DE_20.
They can either be downloaded with a python utility script or manually, described in the next two subsections.

### Download using python script:

In the ETL-CMS/scripts folder, there is a python program 'get_synpuf_files.py',
which can be run to fetch one or more of the 20 SynPUF data sets. Run as follows:

``python get_synpuf_files.py path/to/output/directory <SAMPLE> ... [SAMPLE] ``

Where each SAMPLE is a number from 1 to 20, representing the 20 parts of the CMS data. If you only wanted to obtain
samples 4 and 15, you would run:

``python get_synpuf_files.py path/to/output/directory 4 15``

To obtain all of the data, run:

``python get_synpuf_files.py path/to/output/directory 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20``

### Manual download:
Hyperlinks to the 20 parts can be found here:
<https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/DE_Syn_PUF.html>

For example for DE_1, create a directory called DE_1 and download the following files:

DE1\_0\_2008\_Beneficiary\_Summary\_File\_Sample\_1.zip  
DE1\_0\_2008\_to\_2010\_Carrier\_Claims\_Sample\_1A.zip  
DE1\_0\_2008\_to\_2010\_Carrier\_Claims\_Sample\_1B.zip  
DE1\_0\_2008\_to\_2010\_Inpatient\_Claims\_Sample\_1.zip  
DE1\_0\_2008\_to\_2010\_Outpatient\_Claims\_Sample\_1.zip  
DE1\_0\_2008\_to\_2010\_Prescription\_Drug\_Events\_Sample\_1.zip  
DE1\_0\_2009\_Beneficiary\_Summary\_File\_Sample\_1.zip  
DE1\_0\_2010\_Beneficiary\_Summary\_File\_Sample\_1.zip  

N.B.- If you are downloading the files manually from CMS website, you need to rename 'DE1_0_2008_to_2010_Carrier_Claims_Sample_11A.csv.zip'
to 'DE1_0_2008_to_2010_Carrier_Claims_Sample_11A.zip'.
Also, some zipped files have '.Copy.csv' file inside them. Rename those files from 'Copy.csv' to '.csv' after unzipping the zipped files.
If you use the download script, you don't have to do all of these manual steps. The script will take care of all these.

## 3. Download CDMv5 Vocabulary files
Download vocabulary files from <http://www.ohdsi.org/web/athena/>, ensuring that you select at minimum, the following vocabularies:
SNOMED, ICD9CM, ICD9Proc, CPT4, HCPCS, LOINC, RxNorm, and NDC.

- Unzip the resulting .zip file to a directory.
- Because CPT4 vocabulary is not part of CONCEPT.csv file, one must download it with the provided cpt4.jar program via:
``java -Dumls-user='XXXX' -Dumls-password='XXXX' -jar cpt4.jar 5``, which will append the CPT4 concepts to the CONCEPT.csv file. You will need to pass in your UMLS credentials in order for this command to work. 
- Note: This command works with Java version 10 or below. 

## 4. Setup the .env file to specify file locations
Edit the variables in the .env file which specify various directories used during the ETL process.
Example .env files are provided for Windows (.env.example.windows) and unix (.env.example.unix) runs,
differing only in path name constructs.

- Set BASE\_SYNPUF\_INPUT\_DIRECTORY to where the downloaded CMS
directories are contained, that is, the DE\_1 through DE\_20 directories.
- Set BASE\_OMOP\_INPUT\_DIRECTORY to the CDM v5 vocabulary directory, for example: /data/vocab_download_v5.
- Create a directory and set its path to BASE\_OUTPUT\_DIRECTORY. This
directory will contain all of the output files after running the ETL.
- Create a directory and set its path to
BASE\_ETL\_CONTROL\_DIRECTORY. This contains files used for
auto-incrementing record numbers and keeping track of physicians
and physician institutions over the 20 parts so that the seperate DE\_1 through
DE\_20 directories can be processed sequentially. These
files need to be deleted if you want to restart numbering.

## 5. Test ETL with DE_0 CMS test data
We have provided the directory named DE_0 inside the
python_etl/test_data directory. Copy this directory to your input
directory containing the DE_X directories. This directory has sample
input corresponding to 2 persons which can be used to check the
desired output. It is based on the hand-coded samples.

Run the ETL process on the files in this directory with:  
``python CMS_SynPuf_ETL_CDM_v5.py 0``  
and check for the output generated in the BASE\_OUTPUT\_DIRECTORY
directory.  A .csv file should be generated for each of the CDM v5 tables,
suitable for comparing against the hand-coded outputs.  Note at this
time, all of the tables have been implemented, but some might be empty (e.g visit_cost and device_cost) due to lack of data.
Clean out the control files in BASE\_ETL\_CONTROL\_DIRECTORY before running the next step.

## 6. Run ETL on CMS data

To process any of the DE_1 to DE_20 folders, run:

- ``python CMS_SynPuf_ETL_CDM_v5.py <sample number>``
    - Where ``<sample number>`` is the number of one of the samples you downloaded from CMS
    - e.g. ``python CMS_SynPuf_ETL_CDM_v5.py 4`` will run the ETL on the SynPUF data in the DE_4 directory
    - The resulting output files should be suitable for bulk loading into a CDM v5 database.

The runs cannot be done in parallel because counters and unique
physician and physician institution providers are detected and carried
over multiple runs (saved in BASE\_ETL\_CONTROL\_DIRECTORY). We
recommend running them sequentially from 1 through 20 to produce a
complete ETL of the approximately 2.33M patients. If you wanted only
1/20th of the data, you could run only sample number 1 and load the
resulting .csv files into your database.

N.B. - On average, the CMS_SynPuf_ETL_CDM_v5.py program takes approximately 45-60 minutes to process one input file (e.g. DE_1).  We executed the program
on an Intel Xeon CPU E3-1271 v3, with 16GB of memory and it took approximately 14 hours to process all 20 DE files.

Once each individual file has been processed, they all need to be concatenated into a single CSV. The provided ``merge.py`` can accomplish this:

```
python merge.py
```

All the paths are taking from the ``.env`` file that was set up previously.

## 7. Load data into the database
The PostgreSQL database was used for testing and we provided copies of the relevant PostgreSQL-compliant SQL code to create an OMOP CDMv5.0 database,
and load the data into PostgreSQL. As the common data model changes to 5.0.1 and beyond, this ETL would have to be updated,
and new copies of the relevant SQL code be retrieved from the [CommonDataModel repository](https://github.com/OHDSI/CommonDataModel), where
SQL code is maintained for PostgreSQL, Oracle, and SQL Server database construction. The instructions below are exclusively for PostgresSQL,
and would have to be adapted slightly for other databases.

To create tables in a PostgreSQL database or to load the .csv files created by ETL programs to a PostgreSQL database,
the queries can be executed in pgadmin or a PostgreSQL psql terminal.
 - to run the queries in pgadmin III, open the sql file in pgadmin III and click on the run button.
 - to run the queries in PostgreSQL terminal (psql), run ``psql``, then type the command ''\i XXXX.sql'' where XXXX.sql is an sql file. Alternately you can run an SQL file from the command line via ``psql -f XXXX.sql``.

### Prepare the database

These steps can all be executed directly via psql or within pgAdmin:

1. Login to the PostgreSQL database and create a new database. e.g. ``CREATE DATABASE ohdsi``

1. If you don't want to use the public schema, create a new empty schema.  e.g. ``CREATE SCHEMA synpuf5``

1. Create a separate empty schema for ACHILLES results e.g. ``CREATE SCHEMA results``

### Load and process the data

Code for loading and processing is contained in the `SQL` directory found at the root of this repository. Each file should be executed at the command line in the following manner (also found in `SQL/README.md` if you need a reminder):

```
psql 'dbname={dbname} user={username} options=--search_path={schema_name}' -f {filename.sql} -v data_dir={data_directory}
```

The arguments `dbname`, `username`, and `schema_name` will be the same for the entire process and should match the names used when preparing the database above. For brevity, the connection string will be replaced with `CONNECTION_STRING` in the example commands below. You may consider exporting this as an environment variable.

The `data_dir` argument varies depending on the script. Below, we'll use the variable names used in the `python_etl/.env` file, though you'll need to replace them manually (the scripts do not currently read from that file).

Do not chain all the SQL files together in a batch script, as you need to review the logs for errors and warnings before proceeding to the next step.

1. The `create_CDMv5_tables.sql` file has the queries to create the OMOP CDMv5 tables, also creating the vocabulary tables within the schema. Run it like so:

        psql 'CONNECTION_STRING' -f create_CDMv5_tables.sql

1. The `load_CDMv5_vocabulary.sql` file loads the vocabulary data into the database:
        
        psql 'CONNECTION_STRING' -f load_CDMv5_vocabulary.sql -v data_dir='BASE_OMOP_INPUT_DIRECTORY'

1. The `load_CDMv5_synpuf.sql` file will load the data from DE_1 to DE_20 into tables. This uses the consolidated `csv` files from the `merge.py` script above. The DE-specific files are no longer referenced.

        psql 'CONNECTION_STRING' -f load_CDMv5_synpuf.sql -v data_dir='BASE_OUTPUT_DIRECTORY'

1. The `create_CDMv5_constraints.sql` file will assign primary and foreign keys to all tables for more efficient querying. Make sure you have loaded all of  your data before running this step. If you add the constraints before loading the data, it will slow down the load process because the database needs to check the constraints before adding any record to the database:

        psql 'CONNECTION_STRING' -f create_CDMv5_constraints.sql


1. The `create_CDMv5_indices.sql` file will add additional indexes based on foreign keys and other frequently used fields to improve the query execution time:

        psql 'CONNECTION_STRING' -f create_CDMv5_indices.sql


### Create ERA tables
  Once you are done loading the data and creating indices, you can generate data for drug_era and condition_era tables as follows:

* condition_era: 

        psql 'CONNECTION_STRING' -f create_CDMv5_condition_era.sql


* drug_era: 

        psql 'CONNECTION_STRING' -f create_CDMv5_drug_era_non_stockpile.sql
        
N.B. - The queries to create drug_era and condition_era tables might take approx 48 hours.


## 8. Open issues and caveats with the ETL
a) As per OHDSI documentation for the [observation](http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:observation) and [measurement](http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:measurement) tables, the fields 'value_as_string', 'value_as_number', and 'value_as_concept_id' in both tables are not mandatory, but Achilles Heels gives an error when all of these 3 fields are NULL. Achilles Heels requires one of these fields
    should have non-NULL value. So, to fix this error, field 'value_as_concept_id' has been populated with '0' in both the measurement and observation output .csv files.

b) The concepts for Unknown Race, Non-white, and Other Race (8552, 9178, and 8522) have been deprecated, so race_concept_id in Person file has been populated with
    '0' for these deprecated concepts.

c) Only two ethnicity concepts (38003563, 38003564) are available.  38003563: Hispanic and  38003564: Non-Hispanic.

d) When a concept id has no mapping in the CONCEPT_RELATIONSHIP table:
- If there is no mapping from OMOP (ICD9) to OMOP (SNOMED) for an ICD9 concept id, target_concept_id for such ICD9 concept id is populated with '0' .
- If there is no self-mapping from OMOP (HCPCS/CPT4) to OMOP (HCPCS/CPT4) for an HCPCS/CPT4 concept id, target_concept_id for such HCPCS/CPT4 concept id is populated with '0' .
- If there is no mapping from OMOP (NDC) to OMOP (RxNorm) for an NDC concept id, target_concept_id for such NDC concept id is populated with '0'.

e) The source data contains concepts that appear in the CONCEPT.csv file but do not have relationship mappings to target vocabularies. For these, we create records with concept_id 0 and include the source_concept_id in the record. Achilles Heel will give warnings about these concepts for the Condition, Observation, Procedure, and Drug tables as follows. If condition_concept_id or observation_concept_id or procedure_concept_id or drug_concept_id is '0' respectively:
- WARNING: 400-Number of persons with at least one condition occurrence, by condition_concept_id; data with unmapped concepts
- WARNING: 800-Number of persons with at least one observation occurrence, by observation_concept_id; data with unmapped concepts
- WARNING: 600-Number of persons with at least one procedure occurrence, by procedure_concept_id; data with unmapped concepts
- WARNING: 700-Number of persons with at least one drug exposure, by drug_concept_id; data with unmapped concepts

f) About 6% of the records in the drug_exposure file have either days_supply or quantity or both set to '0' (e.g. days_supply = 10 & quantity=0 OR quantity=120 & days_supply=0). Though such
    values are present in the input file, they don't seem to be correct. Because of this, dosage calculations would result in division by zero, hence effective_drug_dose has not been calculated. For that reason we have also left the dose_era table empty. The CMS documentation says the following about both quantity and days_supply, "This variable was imputed/suppressed/coarsened as part of disclosure treatment. Analyses using this variable should be interpreted with caution. Analytic inferences to the Medicare population should not be made when using this variable.""

g) The locations provided in the DE_SynPUF data use [SSA codes](https://www.resdac.org/cms-data/variables/state-code-claim-ssa), and we mapped them to 2-letter state codes. However SSA codes for Puerto Rico('40') and
    Virgin Islands ('48') as well other extra-USA locations have been coded in source and target data as '54' representing Others, where Others= PUERTO RICO, VIRGIN ISLANDS, AFRICA, ASIA OR CALIFORNIA; INSTITUTIONAL PROVIDER OF SERVICES (IPS) ONLY, CANADA & ISLANDS, CENTRAL AMERICA AND WEST INDIES, EUROPE, MEXICO, OCEANIA, PHILIPPINES, SOUTH AMERICA, U.S. POSSESSIONS, AMERICAN SAMOA, GUAM, SAIPAN OR
    NORTHERN MARIANAS, TEXAS; INSTITUTIONAL PROVIDER OF SERVICES (IPS) ONLY, NORTHERN MARIANAS, GUAM, UNKNOWN.

h) As per OMOP CDMv5 [visit_cost](http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:visit_cost) documentation, the cost of the visit may contain just board and food,
    but could also include the entire cost of everything that was happening to the patient during the visit. As the input data doesn't have any specific data
    for visit cost, we are not writing any information to visit_cost file.

i) There is no specific data in the input DE_SynPUF files for device cost, so only a header line is written to the device_cost file.

j) Because each person can be covered by up to four payer_plans, we cannot uniquely assign a payer_plan_period_id to drugs or procedures within the drug_cost and procedure_cost files. We leave payer_plan_period_id as blank in those two files.The calculation for the fields payer_plan_period_start_date and payer_plan_period_end_date is based on the values of the following 4 fields of the input beneficiary files: BENE_HI_CVRAGE_TOT_MONS, BENE_SMI_CVRAGE_TOT_MONS, BENE_HMO_CVRAGE_TOT_MONS, and PLAN_CVRG_MOS_NUM, corresponding to the number of months a beneficiary was covered under each of up to 4 plans (Medicare Part A, Medicare Part B, HMO, and Medicare Part D). Every beneficiary can thus be covered by up to 4 plans, over the three years of data (2008-2010). The CDM requires the specification of start and end date of coverage, which we do not have. Thus we will make some (questionable) assumptions and create payer_plan_period_start_date and payer_plan_period_end_date records for each of the 4 plans using the information about the number of months a beneficiary was covered in a given year as follows:

- if the value of the fields is 12 in 2008, 2009, and 2010, payer_plan_period_start_date is set to '1/1/2008' and payer_plan_period_end_date is set to '12/31/2010'.
- if the value of the fields is 12 in 2008 and 2009 and is less than 12 in 2010, payer_plan_period_start_date is set to '1/1/2008' and payer_plan_period_end_date
  is set to '12/31/2009 + #months in 2010' (12/31/2009 if #months=0).
- if the value of the fields is 12 in 2008 and 2010 and is less than 12 in 2009, different payer_plan_period_start_date and payer_plan_period_end_date are set for
  these 3 years. Three payer_plan_period records are created with payer_plan_period_start_date and payer_plan_period_end_date set to: [('1/1/2008', '12/31/2008'),
  ('1/1/2009', '1/1/2009 + #months' no record is written if #months=0), ('1/1/2010', '12/31/2010')]
- if the value of the fields is 12 in 2008 and is less than 12 in 2009 and 2010, three payer_plan_period records are created with payer_plan_period_start_date and payer_plan_period_end_date set to: [('1/1/2008', '12/31/2008'),
  ('1/1/2009', '1/1/2009+ #months' no record is written if #months=0), ('1/1/2010', '1/1/2010+ #months' no record is written if #months=0)]
- if the value of the fields is 12 in 2009 and 2010 and is less than 12 in 2008, payer_plan_period_start_date is calculated by subtracting #months
  from '12/31/2008' and payer_plan_period_end_date is set to '12/31/2010'.
- if the value of the fields is 12 in 2009 and is less than 12 in 2008 and 2010, payer_plan_period_start_date is calculated by subtracting #months
  from 12/31/2008 and payer_plan_period_end_date is calculated by adding #months to '12/31/2009'
- if the value of the fields is 12 is 2010 and is less than 12 in 2008 and 2009, three payer_plan_period records are created with payer_plan_period_start_date and payer_plan_period_end_date
  set to: [('1/1/2008', '1/1/2008+ #months' no record is written if #months=0),
  ('1/1/2009', '1/1/2009+ #months' no record is written if #months=0), ('1/1/2010', '12/31/2010')]
- if the value of the fields is less than 12 in 2008, 2009, and 2010, three payer_plan_period records are created with payer_plan_period_start_date and payer_plan_period_end_date set to - [('1/1/2008', '1/1/2008+ #months' - no record is written if #months=0),
  ('1/1/2009', '1/1/2009+ #months' - no record is written if #months=0), ('1/1/2010', '1/1/2010+ #months' - no record is written if #months=0)]

k) Input files (DE_1-DE_20) have some ICD9/HCPCS/NDC codes that are
not defined in the concept file and therefore such records are not
processed by the program and are written to the
'unmapped_code_log.txt' file. This file is opened in append mode so
that if more than one input file is processed together, the program
should append unmapped codes from all input files instead of
overwriting. So the file 'unmapped_code_log.txt' must be deleted if
you want to rerun the program with the same input file. We list the
unmapped codes that occurred 50 or more times along with their
putative source vocabulary (ICD9/HCPCS/CPT4/NDC) below. Some appear to
be typos or invalid entries. Others may represent ICD9 codes that are
not part of ICD9CM. For instance the 04.xx codes are listed on some
non-US lists of ICD9 codes (see for example
[04.22](http://jgp.uhc.com.pl/doc/39.5/icd9/04.22.html)).

      Count Vocabulary  Code
      ----- ----------- ----
      54271 ICD9        XX000
      11697 HCPCS/CPT4  201E5
       5293 ICD9        0422
       5266 ICD9        0432
       5249 ICD9        0440
       5240 ICD9        0430
       5220 ICD9        0421
       5208 ICD9        0429
       5206 ICD9        0431
       5204 ICD9        0433
       5157 ICD9        0439
       5119 ICD9        0420
       2985 ICD9        OTHER
       1773 HCPCS/CPT4  0851
       1153 HCPCS/CPT4  99910
       1038 ICD9        30513
        993 ICD9        30512
        925 ICD9        30510
        897 ICD9        30511
        655 HCPCS/CPT4  90699
        406 HCPCS/CPT4  01
        327 HCPCS/CPT4  0841
        313 NDC         OTHER
        286 HCPCS/CPT4  0521
        270 HCPCS/CPT4  520
        234 HCPCS/CPT4  99998
        211 ICD9        9
        180 HCPCS/CPT4  00000
        125 ICD9        30040
        119 HCPCS/CPT4  XXXXX
        101 HCPCS/CPT4  J2000
         99 HCPCS/CPT4  A9170
         93 HCPCS/CPT4  X9999
         92 HCPCS/CPT4  A9160
         80 ICD9        72330
         71 HCPCS/CPT4  GOOO8
         71 HCPCS/CPT4  GO283
         68 ICD9        73930
         59 ICD9        4900
         54 HCPCS/CPT4  521
         50 ICD9        VO48
