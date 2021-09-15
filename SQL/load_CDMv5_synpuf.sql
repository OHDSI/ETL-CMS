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


\COPY CARE_SITE FROM 'care_site.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY CONDITION_OCCURRENCE FROM 'condition_occurrence.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY DEATH FROM 'death.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY DEVICE_COST FROM 'device_cost.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY DRUG_COST FROM 'drug_cost.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY DRUG_EXPOSURE FROM 'drug_exposure.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY DEVICE_EXPOSURE FROM 'device_exposure.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY LOCATION FROM 'location.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY MEASUREMENT FROM 'measurement_occurrence.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY OBSERVATION FROM 'observation.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY PERSON FROM 'person.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY PROCEDURE_OCCURRENCE FROM 'procedure_occurrence.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY PROCEDURE_COST FROM 'procedure_cost.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY PROVIDER FROM 'provider.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY SPECIMEN FROM 'specimen.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY VISIT_COST FROM 'visit_cost.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY VISIT_OCCURRENCE FROM 'visit_occurrence.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY OBSERVATION_PERIOD FROM 'observation_period.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY PAYER_PLAN_PERIOD FROM 'payer_plan_period.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
