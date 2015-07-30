# ETL-CMS
Workproducts to ETL CMS datasets into OMOP Common Data Model

## What's in Here?


### RabbitInAHat
The WhiteRabbit/RabbitInAHat files used to develop the ETL specification, along with the ETL specification in DOCX format.


### scripts
The scripts folder holds handy scripts for munging some of the raw data used in the ETLs we're developing.

Known scripts:

- get_synpuf_files.py
    - A python3 script that will download sets 4 and 15 from the CMS website and unzip them into a folder of your choosing.


### python_etl
A Python-based ETL of SynPUF into CDMv5-compatible CSV files.  **This implementation is under active development and is intended to be used by people aiming to contribute to its implementation.  It is not ready for general use.**


### hand_conversion
@claire-oi hand-converted a couple patient’s worth of SynPUF data into CDMv5.  Along the way she found several inconsistencies and ambiguities with the ETL specification which we aim to address soon.  Her internal notes, along with the sample patients and her hand-converted CDM outputs are available in this folder.

Any updates/additions you’d like to make to the test cases should be put in the Excel files in the `hand_conversion` directory.  Then run the `csvify.rb` script in the `hand_conversion` directory.  This script will re-generate the CSV files used for testing in the `python_etl/test_data` directory.


### Additional Resources
- [Partial ETL of SEER Medicare to OMOP CDMv4](https://github.com/outcomesinsights/seer_to_omop_cdmv4) - [Outcomes Insights](http://outins.com) has released their partial implementation of a SEER Medicare ETL, along with their specification document to serve as a reference for the ETLs created by this workgroup
