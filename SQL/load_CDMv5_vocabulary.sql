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
-- psql 'dbname={ohdsi} user={username} options=--search_path={schema_name}' -f load_CDMv5_vocabulary.sql -v data_dir={dir_goes_here}

-- Test that data load was successful:
-- psql 'dbname={ohdsi} user={username} options=--search_path={schema_name}'
-- SELECT * FROM concept_ancestor LIMIT 10;

\COPY concept FROM 'CONCEPT.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b';
\COPY concept_ancestor FROM 'CONCEPT_ANCESTOR.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b';
\COPY concept_class FROM 'CONCEPT_CLASS.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b';
\COPY concept_relationship FROM 'CONCEPT_RELATIONSHIP.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b';
DELETE FROM concept_relationship WHERE invalid_reason is not null;
\COPY concept_synonym FROM 'CONCEPT_SYNONYM.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b';
\COPY domain FROM 'DOMAIN.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b';
\COPY relationship FROM 'RELATIONSHIP.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b';
\COPY vocabulary FROM 'VOCABULARY.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b';
\COPY drug_strength FROM 'DRUG_STRENGTH.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b';
