
-- insert_care_site.sql

insert into cdm_synpuf_v667.dbo.care_site (
care_site_id,
care_site_name,
place_of_service_concept_id,
location_id,
care_site_source_value,
place_of_service_source_value
)
select care_site_id,
care_site_name,
coalesce(place_of_service_concept_id,0),
location_id,
care_site_source_value,
place_of_service_source_value 
from cdm_synpuf_v665.dbo.care_site;

