/*********************************************************************************
# Copyright 2016 Observational Health Data Sciences and Informatics
#
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
********************************************************************************/
-- Change to the directory containing the data files
\cd :data_dir

-- Run the following command:
-- psql 'dbname={ohdsi} user={username} options=--search_path={schema_name}' -f load_CDMv5_synpuf.sql -v data_dir={dir_goes_here}

\COPY CARE_SITE FROM PROGRAM 'gzip -dc care_site.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY CONDITION_OCCURRENCE FROM PROGRAM 'gzip -dc condition_occurrence.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY DEATH FROM PROGRAM 'gzip -dc death.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY DEVICE_COST FROM PROGRAM 'gzip -dc device_cost.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY DRUG_COST FROM PROGRAM 'gzip -dc drug_cost.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY DRUG_EXPOSURE FROM PROGRAM 'gzip -dc drug_exposure.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY DEVICE_EXPOSURE FROM PROGRAM 'gzip -dc device_exposure.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY LOCATION FROM PROGRAM 'gzip -dc location.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY MEASUREMENT FROM PROGRAM 'gzip -dc measurement_occurrence.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY OBSERVATION FROM PROGRAM 'gzip -dc observation.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY PERSON FROM PROGRAM 'gzip -dc person.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY PROCEDURE_OCCURRENCE FROM PROGRAM 'gzip -dc procedure_occurrence.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY PROCEDURE_COST FROM PROGRAM 'gzip -dc procedure_cost.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY PROVIDER FROM PROGRAM 'gzip -dc provider.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY SPECIMEN FROM PROGRAM 'gzip -dc specimen.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY VISIT_COST FROM PROGRAM 'gzip -dc visit_cost.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY VISIT_OCCURRENCE FROM PROGRAM 'gzip -dc visit_occurrence.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY OBSERVATION_PERIOD FROM PROGRAM 'gzip -dc observation_period.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY PAYER_PLAN_PERIOD FROM PROGRAM 'gzip -dc payer_plan_period.csv.gz' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
