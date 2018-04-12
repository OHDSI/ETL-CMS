
-- insert_provider.sql

insert into cdm_synpuf_v667.dbo.provider (
provider_id,
provider_name,
NPI,
DEA,
specialty_concept_id,
care_site_id,
year_of_birth,
gender_concept_id,
provider_source_value,
specialty_source_value,
specialty_source_concept_id,
gender_source_value,
gender_source_concept_id
)
select provider_id,
provider_name,
NPI,
DEA,
coalesce(specialty_concept_id,0),
care_site_id,
year_of_birth,
coalesce(gender_concept_id,0),
provider_source_value,
specialty_source_value,
coalesce(specialty_source_concept_id,0),
gender_source_value,
coalesce(gender_source_concept_id,0) 
from cdm_synpuf_v665.dbo.provider;