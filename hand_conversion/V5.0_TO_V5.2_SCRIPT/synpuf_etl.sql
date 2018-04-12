-- SYNPUF CDM 5.2 ETL 
--
-- SYNPUF V5.0 corresponds to database CDM_SYNPUF_V665
-- SYNPUF V5.2 corresponds to database CDM_SYNPUF_V667
--
-- This script will generate statements to populate tables in the CDM_SYNPUF_V667.dbo schema 
-- from the corresponding tables in the CDM_SYNPUF_V665.dbo schema.
--

-- Step 0: Generate truncate statements for v5.2 tables and create CTAS
-- statements for tables in 5.0 missing from 5.2.
-- NB: CTAS doesn't seem to be supported on whatever sql server is running on synpuf,
--     so we'll use SELECT INTO.
select 'truncate table cdm_synpuf_v667.dbo.' + table_name + ';'
  from cdm_synpuf_v667.information_schema.tables;

-- Step 1: Find nonempty v5.0 tables.
-- 
select t.name into #nonempty
  from cdm_synpuf_v665.sys.tables t,
       cdm_synpuf_v665.sys.partitions p
 where t.object_id = p.object_id 
 group by t.name
having max(p.rows) > 0;


-- Step 2: Determine how to load the v5.2 tables from the
--         nonempty v5.0 tables.  Tables that are structurally identical
--         (i.e., same number and names of columns) can use a simple insert-select.
--         Since a table can change the number of columns it has or change column names
--         (while potentially keeping the number of columns the same), we need to check for both.
--         For tables that have old_count = new_count = num_matches in the query below, we may populate
--         v5.2 with a simple insert-select.

select v50.table_name,
       count(distinct v50.column_name) old_count,
       count(distinct v52.column_name) new_count,
       sum(case when v50.column_name = v52.column_name then 1 else 0 end) num_matches            
  from cdm_synpuf_v665.information_schema.columns v50, 
       cdm_synpuf_v667.information_schema.columns v52
 where v50.table_name  = v52.table_name
   and v50.table_name in (select name from #nonempty)
 group by v50.table_name; 

 
-- Step 3: Using the query from step 2, generate necessary INSERT statements for tables that haven't changed.
--
--
 -- select concat(
       -- 'insert into cdm_synpuf_v667.dbo.',
        -- v50.table_name, 
       -- ' select * from cdm_synpuf_v665.dbo.',
        -- v50.table_name,';') sql_insert
  -- from cdm_synpuf_v665.information_schema.columns v50, 
       -- cdm_synpuf_v667.information_schema.columns v52
 -- where v50.table_name  = v52.table_name
   -- and v50.table_name in (select name from #nonempty)
 -- group by v50.table_name 
-- having count(distinct v50.column_name) = count(distinct v52.column_name)
   -- and count(distinct v52.column_name) = sum(case when v50.column_name = v52.column_name then 1 else 0 end);

 select v50.table_name into #unchanged_tables
  from cdm_synpuf_v665.information_schema.columns v50, 
       cdm_synpuf_v667.information_schema.columns v52
 where v50.table_name  = v52.table_name
   and v50.table_name in (select name from #nonempty)
 group by v50.table_name 
having count(distinct v50.column_name) = count(distinct v52.column_name)
   and count(distinct v52.column_name) = sum(case when v50.column_name = v52.column_name then 1 else 0 end);

-- Can't use the commented out simpler version above because null concept_id columns need to be converted to 0.
-- Generate insert-select statements which find nullable concept_id columns and use coalesce to replace NULL with 0.
with v50 as (
select table_name,
       column_name,
       case when lower(is_nullable) = 'yes' and lower(column_name) like '%concept_id%'
            then concat('coalesce(',column_name,',0)')
            else column_name
        end coalesced_column,
       ordinal_position,
       count(*)over(partition by table_name) cnt 
  from cdm_synpuf_v665.information_schema.columns 
 where table_name in (select table_name from #unchanged_tables)
), insert_stmt as (
select table_name, 1 flag,
       case when v50.ordinal_position = 1
            then concat('insert into cdm_synpuf_v667.dbo.',table_name,' (',column_name,',')
            when v50.ordinal_position = v50.cnt
            then concat(column_name,')')
            else concat(column_name,',')
            end stmt
  from v50
 ), select_stmt as (
select table_name, 2 flag,
       case when v50.ordinal_position = 1
            then concat('select ',coalesced_column,',')
            when v50.ordinal_position = v50.cnt
            then concat(coalesced_column,' from cdm_synpuf_v665.dbo.',table_name,';')
            else concat(coalesced_column,',')
            end stmt    
  from v50
) 
select stmt from (
select table_name, flag, stmt from insert_stmt 
union all
select table_name, flag, stmt from select_stmt 
) tmp order by table_name,flag;


-- Step 4: Using the query from step 2, find which tables have changed and cannot be populated via simple insert-select.
--

select v50.table_name into #custom_insert
  from cdm_synpuf_v665.information_schema.columns v50, 
       cdm_synpuf_v667.information_schema.columns v52
 where v50.table_name  = v52.table_name
   and v50.table_name in (select name from #nonempty)
 group by v50.table_name
having count(distinct v50.column_name) != count(distinct v52.column_name)
    or count(distinct v52.column_name) != sum(case when v50.column_name = v52.column_name then 1 else 0 end);

-- Step 5: While step 4 finds the tables for which we cannot do a simple insert-select because of new/changed columns,
--         at least some of the columns will be the same, so we can build partial insert-select statements for the columns
--         that the tables have in common between versions.  We'll build the insert-select statement so that the new/changed
--         columns are on the end.  
--         For example, the following query shows which columns are new/changed in DEATH:
--
--         (i)
--              select table_name,column_name
--                from cdm_synpuf_v667.information_schema.columns
--               where table_name = 'death'
--              except
--              select table_name,column_name
--                from cdm_synpuf_v665.information_schema.columns
--               where table_name = 'death'
--
--        The query below shows which columns have NOT changed in DEATH:
--
--        (ii)
--              select table_name,column_name
--                from cdm_synpuf_v667.information_schema.columns
--               where table_name = 'death'
--              intersect
--              select table_name,column_name
--                from cdm_synpuf_v665.information_schema.columns
--               where table_name = 'death'
--
--        So, we can build a simple query using (ii), then tack on the needed columns from (i) and document the rules to populate the new/changed columns.
--
--       For generating insert strings:
--         Use ? to indicate which columns we cannot use v5.0 to populate.
--         Cannot use ORDER BY in a view in PDW, so using row_number to impose order.
--         Use temp tables for readability.
--
--       First determine which columns exist in both v5.0 and v5.2 and which do not:  
select v52.table_name,
       v52.column_name,
       case when v50.column_name is null then 0 else 1 end is_match,
       row_number()over(partition by v52.table_name order by case when v50.column_name is null then 0 else 1 end desc) pos
       into #ordered_cols
  from      cdm_synpuf_v667.information_schema.columns v52
  left join cdm_synpuf_v665.information_schema.columns v50
         on v52.table_name  = v50.table_name 
        and v52.column_name = v50.column_name
 where v52.table_name in (select table_name from #custom_insert);
 
--     
--     Use the temp table created above to generate insert statements of the form:
--
--            insert into cdm_synpuf_v667.dbo.foo (col1,col2,...,colm,coln) 
--            select col1,col2,...,?,? from cdm_synpuf_v665.dbo.foo 
--
--     With the ? signifying that columns colm and coln do not exist in v5.0.
--
--     NB: These insert statements merely form the foundation for the actual insert statement needed.
--         The data may require joins to other tables

select table_name,insert_string 
  into #insert_statements
  from ( 
 select table_name,
        case when pos = 1 
            then 'insert into cdm_synpuf_v667.dbo.' + table_name + ' ( ' + column_name + ', ' 
            when pos = max(pos)over(partition by table_name) 
            then column_name + ')'
            else column_name + ', '
       end insert_string
   from #ordered_cols
union all
 select table_name,
        case when pos = 1 
             then 'select ' + column_name + ', ' 
             when is_match = 1 
             then column_name + ', '
             when pos = max(pos)over(partition by table_name)
             then '? from cdm_synpuf_v665.dbo.' + table_name
             when is_match = 0
             then '?'
       end 
   from #ordered_cols
        ) tmp 
  order by table_name;
  
-- Populate the COST table - new in 5.2
--   Check which 5.0 cost tables are not empty
-- 

select t.name, max(p.rows) cardinality
  from cdm_synpuf_v665.sys.tables t,
       cdm_synpuf_v665.sys.partitions p
 where t.object_id = p.object_id and t.name like '%cost%'
 group by t.name;

-- name             cardinality
------------------  ------------
-- cost	            0
-- device_cost      0
-- drug_cost	    111085969
-- procedure_cost   633812025
-- visit_cost	    0  

-- Create insert statements for drug_cost and procedure_cost using https://github.com/OHDSI/CommonDataModel/wiki/COST
-- See insert_cost.sql

-- Row count verification: find tables that do not have the same counts.  
-- NB: Verify predicates on index_id is correct!

select v50t.name, v50p.rows v50_cnt, v52p.rows v52_cnt
  from cdm_synpuf_v665.sys.tables     v50t,
       cdm_synpuf_v665.sys.partitions v50p,
       cdm_synpuf_v667.sys.tables     v52t,
       cdm_synpuf_v667.sys.partitions v52p
 where v50t.object_id = v50p.object_id 
   and v52t.object_id = v52p.object_id
   and v50t.name      = v52t.name
   and v50p.index_id  = 1
   and v52p.index_id  = 0
   and v50p.rows     != v52p.rows;


	