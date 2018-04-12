
-- insert_payer_plan_period.sql

insert into cdm_synpuf_v667.dbo.payer_plan_period (
payer_plan_period_id,
person_id,
payer_plan_period_start_date,
payer_plan_period_end_date,
payer_source_value,
plan_source_value,
family_source_value
)
select payer_plan_period_id,
person_id,
payer_plan_period_start_date,
payer_plan_period_end_date,
payer_source_value,
plan_source_value,
family_source_value 
from cdm_synpuf_v665.dbo.payer_plan_period;

