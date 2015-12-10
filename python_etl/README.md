# Python-based ETL of SynPUF data to CDMv5-compatible CSV files

This is an implementation of the SynPUF Extract-Transform-Load (ETL)
specification designed to generate a set of CDMv5-compatible CSV files
that can then be bulk-loaded into your RDBMS of choice.

This implementation is under active development and is not at all
ready to be used by the general public. We are open-sourcing this code
now because several people have expressed interest in assisting in the
development of this ETL implementation and we will be using this
repository as a means to coordinate the efforts of those individuals.

## Overview of Steps
1. Install required software
2. Download SynPUF input data
3. Download CDMv5 Vocabulary files 
4. Setup the .env file to specify file locations
5. Test ETL with DE\_0 CMS test data
6. Run ETL on CMS data.

## 1. Install required software

The ETL process requires Python 2.7 with the python-dotenv
package.

### Linux

Python 2.7 and python-pip must be installed if they are not already
present. If you are using a RedHat distribution the following commands
will work (you must have system administrator privelidges):

``sudo yum install python``  
``sudo yum install python-pip``

The python-pip package enables a non-administrative user to install
python packages for their personal use. From the python\_etl directory
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
The SynPUF data is divided into 20 parts (8 files per part), and the files for each part should be saved in respective directories DE\_1 through DE\_20. They can either be downloaded with a python utility script or manually, described in the next two subsections.

### Download using python script:

In the ETL-CMS/scripts folder is a python program, get\_synpuf\_files.py,
which can be run to fetch one or more of the 20 SynPUF data sets. Run as follows:

``python get_synpuf_files.py path/to/output/directory <SAMPLE> ... [SAMPLE] ``

Where each SAMPLE is a number from 1 to 20, representing the 20 parts of the CMS data. If you only wanted to obtain
samples 4 and 15, you would run:

``python get_synpuf_files.py path/to/output/directory 4 15``

### Manual download:
Hyperlinks to the 20 parts can be found here:
<https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/DE_Syn_PUF.html>

For example for DE\_1, create a directory called DE\_1 and download the following files:

DE1\_0\_2008\_Beneficiary\_Summary\_File\_Sample\_1.zip  
DE1\_0\_2008\_to\_2010\_Carrier\_Claims\_Sample\_1A.zip  
DE1\_0\_2008\_to\_2010\_Carrier\_Claims\_Sample\_1B.zip  
DE1\_0\_2008\_to\_2010\_Inpatient\_Claims\_Sample\_1.zip  
DE1\_0\_2008\_to\_2010\_Outpatient\_Claims\_Sample\_1.zip  
DE1\_0\_2008\_to\_2010\_Prescription\_Drug\_Events\_Sample\_1.zip  
DE1\_0\_2009\_Beneficiary\_Summary\_File\_Sample\_1.zip  
DE1\_0\_2010\_Beneficiary\_Summary\_File\_Sample\_1.zip  



## 3. Download CDMv5 Vocabulary files
Download vocabulary files from <http://www.ohdsi.org/web/athena/>, ensuring that you select at minimum, the following vocabularies: 
SNOMED, ICD9CM, ICD9Proc, CPT4, HCPCS, LOINC, RxNorm, and NDC.

- Unzip the resulting .zip file to a directory. 
- Because CPT4 vocabulary is not part of CONCEPT.csv file, one must download it with the provided cpt4.jar program via:
``java -jar cpt4.jar``, which will create the file concept_cpt4.csv.

## 4. Setup the .env file to specify file locations
Edit the variables in the .env file which specify various directories
used during the ETL process. Example .env files are provided for
Windows (.env.example.windows) and unix (.env.example.unix) runs,
differing only in path name constructs.

- Set BASE\_SYNPUF\_INPUT\_DIRECTORY to where the downloaded CMS
directories are contained, that is, the DE\_1 through DE\_20 directories.
- Set BASE\_OMOP\_INPUT\_DIRECTORY to the CDM v5 vocabulary directory
- Create a directory and set its path to BASE\_OUTPUT\_DIRECTORY. This
directory will contain all of the output files after running the ETL.
- Create a directory and set its path to
BASE\_ETL\_CONTROL\_DIRECTORY. This contains files used for
autoincrementing record numbers so that the seperate DE\_1 through
DE\_20 directories can be processed and then combined later. These
files need to be deleted if you want to restart numbering.

## 5. Test ETL with DE\_0 CMS test data
We have provided the directory named DE\_0 inside the
python\_etl/test\_files directory. Copy this directory to your input
directory containing the DE\_X directories. This directory has sample
input corresponding to 2 persons which can be used to check the
desired output. It is based on the hand-coded samples.

Run the ETL process on the files in this directory with:  
``python CMS_SynPUF_ETL_CDM_v5.py 0``  
and check for the output generated in the BASE\_OUTPUT\_DIRECTORY
directory.  A .csv file should be generated for each of the CDM v5 tables,
suitable for comparing against the hand-coded outputs.  Note at this
time, many of the tables are not yet implemented.

## 6. Run ETL on CMS data 
To process any of the DE\_1 to DE\_20 folders, run:

- ``python CMS_SynPUF_ETL_CDM_v5.py <sample number>``
    - Where ``<sample number>`` is the number of one of the samples you downloaded from CMS
    - e.g. ``python CMS_SynPUF_ETL_CDM_v5.py 4`` will run the ETL on the SynPUF data in the DE_4 directory
    - The resulting output files should be suitable for bulk loading into a CDM v5 database, though the implementation of all tables is incomplete.
