
-- insert_location.sql

insert into cdm_synpuf_v667.dbo.location (
location_id,
address_1,
address_2,
city,
state,
zip,
county,
location_source_value
)
select location_id,
address_1,
address_2,
city,
state,
zip,
county,
location_source_value 
from cdm_synpuf_v665.dbo.location;

