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

\COPY synpuf5.CARE_SITE FROM '/home/lambert/CMS/cdm5/upload/care_site.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY synpuf5.CONDITION_OCCURRENCE FROM '/home/lambert/CMS/cdm5/upload/condition_occurrence.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY synpuf5.DEATH FROM '/home/lambert/CMS/cdm5/upload/death.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY synpuf5.DEVICE_COST FROM '/home/lambert/CMS/cdm5/upload/device_cost.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY synpuf5.DRUG_COST FROM '/home/lambert/CMS/cdm5/upload/drug_cost.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY synpuf5.DRUG_EXPOSURE FROM '/home/lambert/CMS/cdm5/upload/drug_exposure.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY synpuf5.DEVICE_EXPOSURE FROM '/home/lambert/CMS/cdm5/upload/device_exposure.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY synpuf5.LOCATION FROM '/home/lambert/CMS/cdm5/upload/location.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY synpuf5.MEASUREMENT FROM '/home/lambert/CMS/cdm5/upload/measurement_occurrence.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY synpuf5.OBSERVATION FROM '/home/lambert/CMS/cdm5/upload/observation.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY synpuf5.PERSON FROM '/home/lambert/CMS/cdm5/upload/person.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY synpuf5.PROCEDURE_OCCURRENCE FROM '/home/lambert/CMS/cdm5/upload/procedure_occurrence.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY synpuf5.PROCEDURE_COST FROM '/home/lambert/CMS/cdm5/upload/procedure_cost.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY synpuf5.PROVIDER FROM '/home/lambert/CMS/cdm5/upload/provider.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY synpuf5.SPECIMEN FROM '/home/lambert/CMS/cdm5/upload/specimen.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY synpuf5.VISIT_COST FROM '/home/lambert/CMS/cdm5/upload/visit_cost.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY synpuf5.VISIT_OCCURRENCE FROM '/home/lambert/CMS/cdm5/upload/visit_occurrence.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY synpuf5.OBSERVATION_PERIOD FROM '/home/lambert/CMS/cdm5/upload/observation_period.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
\COPY synpuf5.PAYER_PLAN_PERIOD FROM '/home/lambert/CMS/cdm5/upload/payer_plan_period.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
