/*********************************************************************************
# Copyright 2014 Observational Health Data Sciences and Informatics
#
# 
# Licensed under the Apache License, VersiON synpuf5.2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed ON synpuf5.an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
********************************************************************************/

/************************

 ####### #     # ####### ######      #####  ######  #     #           #######    ###                                           
 #     # ##   ## #     # #     #    #     # #     # ##   ##    #    # #           #  #    # #####  ###### #    # ######  ####  
 #     # # # # # #     # #     #    #       #     # # # # #    #    # #           #  ##   # #    # #       #  #  #      #      
 #     # #  #  # #     # ######     #       #     # #  #  #    #    # ######      #  # #  # #    # #####    ##   #####   ####  
 #     # #     # #     # #          #       #     # #     #    #    #       #     #  #  # # #    # #        ##   #           # 
 #     # #     # #     # #          #     # #     # #     #     #  #  #     #     #  #   ## #    # #       #  #  #      #    # 
 ####### #     # ####### #           #####  ######  #     #      ##    #####     ### #    # #####  ###### #    # ######  ####  
                                                                              

script to create the required indexes within OMOP commON synpuf5.data model, versiON synpuf5.5.0 for PostgreSQL database

last revised: 12 Oct 2014

author:  Patrick Ryan

description:  These indices are considered a minimal requirement to ensure adequate performance of analyses.

*************************/


/************************

Standardized vocabulary

************************/

CREATE UNIQUE INDEX  idx_concept_concept_id  ON synpuf5.concept  (concept_id ASC);
CLUSTER concept  USING  idx_concept_concept_id ;
CREATE INDEX idx_concept_code ON synpuf5.concept (concept_code ASC);
CREATE INDEX idx_concept_vocabluary_id ON synpuf5.concept (vocabulary_id ASC);
CREATE INDEX idx_concept_domain_id ON synpuf5.concept (domain_id ASC);
CREATE INDEX idx_concept_class_id ON synpuf5.concept (concept_class_id ASC);

CREATE UNIQUE INDEX  idx_vocabulary_vocabulary_id  ON synpuf5.vocabulary  (vocabulary_id ASC);
CLUSTER vocabulary  USING  idx_vocabulary_vocabulary_id ;

CREATE UNIQUE INDEX  idx_domain_domain_id  ON synpuf5.domain  (domain_id ASC);
CLUSTER domain  USING  idx_domain_domain_id ;

CREATE UNIQUE INDEX  idx_concept_class_class_id  ON synpuf5.concept_class  (concept_class_id ASC);
CLUSTER concept_class  USING  idx_concept_class_class_id ;

CREATE INDEX idx_concept_relationship_id_1 ON synpuf5.concept_relationship (concept_id_1 ASC); 
CREATE INDEX idx_concept_relationship_id_2 ON synpuf5.concept_relationship (concept_id_2 ASC); 
CREATE INDEX idx_concept_relationship_id_3 ON synpuf5.concept_relationship (relationship_id ASC); 

CREATE UNIQUE INDEX  idx_relationship_rel_id  ON synpuf5.relationship  (relationship_id ASC);
CLUSTER relationship  USING  idx_relationship_rel_id ;

CREATE INDEX  idx_concept_synonym_id  ON synpuf5.concept_synonym  (concept_id ASC);
CLUSTER concept_synonym  USING  idx_concept_synonym_id ;

CREATE INDEX  idx_concept_ancestor_id_1  ON synpuf5.concept_ancestor  (ancestor_concept_id ASC);
CLUSTER concept_ancestor  USING  idx_concept_ancestor_id_1 ;
CREATE INDEX idx_concept_ancestor_id_2 ON synpuf5.concept_ancestor (descendant_concept_id ASC);

CREATE INDEX  idx_source_to_concept_map_id_3  ON synpuf5.source_to_concept_map  (target_concept_id ASC);
CLUSTER source_to_concept_map  USING  idx_source_to_concept_map_id_3 ;
CREATE INDEX idx_source_to_concept_map_id_1 ON synpuf5.source_to_concept_map (source_vocabulary_id ASC);
CREATE INDEX idx_source_to_concept_map_id_2 ON synpuf5.source_to_concept_map (target_vocabulary_id ASC);
CREATE INDEX idx_source_to_concept_map_code ON synpuf5.source_to_concept_map (source_code ASC);

CREATE INDEX  idx_drug_strength_id_1  ON synpuf5.drug_strength  (drug_concept_id ASC);
CLUSTER drug_strength  USING  idx_drug_strength_id_1 ;
CREATE INDEX idx_drug_strength_id_2 ON synpuf5.drug_strength (ingredient_concept_id ASC);

CREATE INDEX  idx_cohort_definition_id  ON synpuf5.cohort_definitiON synpuf5.(cohort_definition_id ASC);
CLUSTER cohort_definitiON synpuf5.USING  idx_cohort_definition_id ;

CREATE INDEX  idx_attribute_definition_id  ON synpuf5.attribute_definitiON synpuf5.(attribute_definition_id ASC);
CLUSTER attribute_definitiON synpuf5.USING  idx_attribute_definition_id ;


/**************************

Standardized meta-data

***************************/





/************************

Standardized clinical data

************************/

CREATE UNIQUE INDEX  idx_person_id  ON synpuf5.persON synpuf5.(person_id ASC);
CLUSTER persON synpuf5.USING  idx_person_id ;

CREATE INDEX  idx_observation_period_id  ON synpuf5.observation_period  (person_id ASC);
CLUSTER observation_period  USING  idx_observation_period_id ;

CREATE INDEX  idx_specimen_person_id  ON synpuf5.specimen  (person_id ASC);
CLUSTER specimen  USING  idx_specimen_person_id ;
CREATE INDEX idx_specimen_concept_id ON synpuf5.specimen (specimen_concept_id ASC);

CREATE INDEX  idx_death_person_id  ON synpuf5.death  (person_id ASC);
CLUSTER death  USING  idx_death_person_id ;

CREATE INDEX  idx_visit_person_id  ON synpuf5.visit_occurrence  (person_id ASC);
CLUSTER visit_occurrence  USING  idx_visit_person_id ;
CREATE INDEX idx_visit_concept_id ON synpuf5.visit_occurrence (visit_concept_id ASC);

CREATE INDEX  idx_procedure_person_id  ON synpuf5.procedure_occurrence  (person_id ASC);
CLUSTER procedure_occurrence  USING  idx_procedure_person_id ;
CREATE INDEX idx_procedure_concept_id ON synpuf5.procedure_occurrence (procedure_concept_id ASC);
CREATE INDEX idx_procedure_visit_id ON synpuf5.procedure_occurrence (visit_occurrence_id ASC);

CREATE INDEX  idx_drug_person_id  ON synpuf5.drug_exposure  (person_id ASC);
CLUSTER drug_exposure  USING  idx_drug_person_id ;
CREATE INDEX idx_drug_concept_id ON synpuf5.drug_exposure (drug_concept_id ASC);
CREATE INDEX idx_drug_visit_id ON synpuf5.drug_exposure (visit_occurrence_id ASC);

CREATE INDEX  idx_device_person_id  ON synpuf5.device_exposure  (person_id ASC);
CLUSTER device_exposure  USING  idx_device_person_id ;
CREATE INDEX idx_device_concept_id ON synpuf5.device_exposure (device_concept_id ASC);
CREATE INDEX idx_device_visit_id ON synpuf5.device_exposure (visit_occurrence_id ASC);

CREATE INDEX  idx_condition_person_id  ON synpuf5.condition_occurrence  (person_id ASC);
CLUSTER condition_occurrence  USING  idx_condition_person_id ;
CREATE INDEX idx_condition_concept_id ON synpuf5.condition_occurrence (condition_concept_id ASC);
CREATE INDEX idx_condition_visit_id ON synpuf5.condition_occurrence (visit_occurrence_id ASC);

CREATE INDEX  idx_measurement_person_id  ON synpuf5.measurement  (person_id ASC);
CLUSTER measurement  USING  idx_measurement_person_id ;
CREATE INDEX idx_measurement_concept_id ON synpuf5.measurement (measurement_concept_id ASC);
CREATE INDEX idx_measurement_visit_id ON synpuf5.measurement (visit_occurrence_id ASC);

CREATE INDEX  idx_note_person_id  ON synpuf5.note  (person_id ASC);
CLUSTER note  USING  idx_note_person_id ;
CREATE INDEX idx_note_concept_id ON synpuf5.note (note_type_concept_id ASC);
CREATE INDEX idx_note_visit_id ON synpuf5.note (visit_occurrence_id ASC);

CREATE INDEX  idx_observation_person_id  ON synpuf5.observatiON synpuf5.(person_id ASC);
CLUSTER observatiON synpuf5.USING  idx_observation_person_id ;
CREATE INDEX idx_observation_concept_id ON synpuf5.observatiON synpuf5.(observation_concept_id ASC);
CREATE INDEX idx_observation_visit_id ON synpuf5.observatiON synpuf5.(visit_occurrence_id ASC);

CREATE INDEX idx_fact_relationship_id_1 ON synpuf5.fact_relationship (domain_concept_id_1 ASC);
CREATE INDEX idx_fact_relationship_id_2 ON synpuf5.fact_relationship (domain_concept_id_2 ASC);
CREATE INDEX idx_fact_relationship_id_3 ON synpuf5.fact_relationship (relationship_concept_id ASC);



/************************

Standardized health system data

************************/





/************************

Standardized health economics

************************/

CREATE INDEX  idx_period_person_id  ON synpuf5.payer_plan_period  (person_id ASC);
CLUSTER payer_plan_period  USING  idx_period_person_id ;





/************************

Standardized derived elements

************************/


CREATE INDEX idx_cohort_subject_id ON synpuf5.cohort (subject_id ASC);
CREATE INDEX idx_cohort_c_definition_id ON synpuf5.cohort (cohort_definition_id ASC);

CREATE INDEX idx_ca_subject_id ON synpuf5.cohort_attribute (subject_id ASC);
CREATE INDEX idx_ca_definition_id ON synpuf5.cohort_attribute (cohort_definition_id ASC);

CREATE INDEX  idx_drug_era_person_id  ON synpuf5.drug_era  (person_id ASC);
CLUSTER drug_era  USING  idx_drug_era_person_id ;
CREATE INDEX idx_drug_era_concept_id ON synpuf5.drug_era (drug_concept_id ASC);

CREATE INDEX  idx_dose_era_person_id  ON synpuf5.dose_era  (person_id ASC);
CLUSTER dose_era  USING  idx_dose_era_person_id ;
CREATE INDEX idx_dose_era_concept_id ON synpuf5.dose_era (drug_concept_id ASC);

CREATE INDEX  idx_condition_era_person_id  ON synpuf5.condition_era  (person_id ASC);
CLUSTER condition_era  USING  idx_condition_era_person_id ;
CREATE INDEX idx_condition_era_concept_id ON synpuf5.condition_era (condition_concept_id ASC);

