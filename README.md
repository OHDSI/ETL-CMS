# ETL-CMS version 1.0
Release date: 6/24/2016

This project contains the source code to convert the public
Centers for Medicare & Medicaid Services (CMS) Data Entrepreneurs'
[Synthetic Public Use File](<https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/DE_Syn_PUF.html>) (DE-SynPUF) to .csv files suitable for loading into an OMOP Common Data Model v5 database.

The DE-SynPUF dataset contains 2.33 million synthetic patients, and we
anticipate that this resource will be useful for researchers in
developing OHDSI tools, as well as serve as a testbed for the analysis
of observational health records.

The processed data will be made available shortly at a location yet to be determined.

This marks the first availability of a massive open CDM v5-adhering synthetic dataset. 


## What's in Here?


### python_etl
A complete Python-based ETL of the DE-SynPUF data into CDMv5-compatible CSV
files. See the [README.md](https://github.com/OHDSI/ETL-CMS/blob/master/python_etl/README.md) file therein for detailed instructions for
running the ETL, as well as creating and loading the data into a CDMv5 database.


### RabbitInAHat
The WhiteRabbit/RabbitInAHat files used to develop the ETL specification, along with an out-of-date ETL specification in DOCX format.


### scripts
The scripts folder holds handy scripts for downloading and munging some of the raw
data used in the ETL process. Instructions for their use can be found
in the [python_etl/README.md](https://github.com/OHDSI/ETL-CMS/blob/master/python_etl/README.md) file.


### hand_conversion
@claire-oi hand-converted a couple patients worth of SynPUF data into CDMv5.  Along the way she found several inconsistencies and ambiguities with the ETL specification which have hopefully been addressed.  Her internal notes, along with the sample patients and her hand-converted CDM outputs are available in the [hand_conversion](https://github.com/OHDSI/ETL-CMS/tree/master/hand_conversion) directory.

### Additional Resources
- [Partial ETL of SEER Medicare to OMOP CDMv4](https://github.com/outcomesinsights/seer_to_omop_cdmv4) - [Outcomes Insights](http://outins.com) has released their partial implementation of a SEER Medicare ETL, along with their specification document to serve as a reference for the ETLs created by this workgroup

#History of contributions

An early release of the Python-based ETL-CMS software was developed by
members of the CMS Working Group of the Observational Health Data
Sciences and Informatics (OHDSI) community to process the DE-SynPUF
files and to create OMOP CDM v5-compatible4 CSV files. Those contributors include:

- Ryan Duryea [@aguynamedryan](https://github.com/aguynamedryan), Outcomes Insights, Inc.
- Erica Voss [@ericaVoss](http://forums.ohdsi.org/users/ericaVoss), Janssen Research and Development.
- Jennifer Duryea [@jenniferduryea](https://github.com/jenniferduryea), Outcomes Insights, Inc.
- Don O'Hara [@donohara](https://github.com/donohara), Evidera.
- Claire Cangialose [@claire-oi](https://github.com/claire-oi), Outcomes Insights, Inc.
- Patrick Ryan [@Patrick_Ryan](http://forums.ohdsi.org/users/Patrick_Ryan), Janssen Research and Development.

Development was partial, stopping in August 2015. Researchers at the
University of New Mexico resumed development in December
2015 and implemented the complete ETL, releasing version 1.0 on June 24, 2016. 
Documentation was created for running the
ETL, creating an OMOP CDM v5 database, and loading the DE-SynPUF
data. Among many improvements, the ETL was overhauled to implement the
visit_occurrrence, location, care_site, payer_plan_period, drug_era, and condition_era tables, and
numerous deficiencies were rectified in concept mapping, in order to
be feature-complete with the CDM v5. All tables now conform to the
constraints defined in the schema. The contributors to this effort were:

- Christophe Lambert [@Christophe_Lambert](http://forums.ohdsi.org/users/Christophe_Lambert), University of New Mexico, Center for Global Health, Division of Translational Informatics, Department of Internal Medicine
- Praveen Kumar [@Praveen_Kumar](http://forums.ohdsi.org/users/Praveen_Kumar), University of New Mexico, Department of Computer Science
- Amritansh [@Amritansh](http://forums.ohdsi.org/users/Amritansh), University of New Mexico, Department of Computer Science
