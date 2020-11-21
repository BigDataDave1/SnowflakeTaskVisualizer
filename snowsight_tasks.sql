--Test of Git Commit: 21-Nov-2020
--Context
use role accountadmin;
use database demo;
use schema public;
use warehouse demo_wh;

--Show all Tasks in Account
show tasks in account;

--Show all Tasks in Demo Database
show tasks;

--Create a Task Warehouse
create warehouse if not exists task_wh with warehouse_size = 'xsmall' auto_suspend = 60;

--Create Generic 5 Task Dependant Workflow to Work from Snowflake Sample Data
/*-------------------------------------------------------------------------------------------------------------------------
T01_CTAS
T02_INSERT_SAMPLE
T03_CTAS_AGG
T04_INSERT_AGG
T05_DELETE_SAMPLE
-------------------------------------------------------------------------------------------------------------------------*/
--Task 01 CTAS
create or replace task demo.public.t01_ctas
  warehouse = task_wh
  schedule = '15 minute'
as
  create table tbl_sample if not exists as select * from snowflake_sample_data.tpcds_sf10tcl.store_sales limit 1;
;

--Start the Task
alter task demo.public.t01_ctas resume;
--Stop the Task to Add Child Tasks
alter task demo.public.t01_ctas suspend;

--Task History
select *
from table(
  information_schema.task_history(
  scheduled_time_range_start=>dateadd('hour',-24,current_timestamp()),
  result_limit => 100,
  task_name=>'t01_ctas'));

--Task 02 INSERT SAMPLE
create or replace task demo.public.t02_insert_sample
  warehouse = task_wh
  after demo.public.t01_ctas
as
  insert into tbl_sample (select * from (select * from snowflake_sample_data.tpcds_sf10tcl.store_sales sample block (0.5)))
;

--Task 03 CTAS AGG
create or replace task demo.public.t03_ctas_agg
  warehouse = task_wh
  after demo.public.t02_insert_sample
as
  create table tbl_sample_agg if not exists as
      select d_date,
      sum(ss_quantity) as sum_ss_quantity,
      sum(ss_ext_sales_price) as sum_ss_ext_sales_price
      from tbl_sample ts
      join snowflake_sample_data.tpcds_sf10tcl.date_dim dd on ts.ss_sold_date_sk = dd.d_date_sk
      group by d_date
      order by d_date desc;
;

--Task 04 INSERT AGG
create or replace task demo.public.t04_insert_agg
  warehouse = task_wh
  after demo.public.t03_ctas_agg
as
  insert into tbl_sample_agg (
      select d_date,
      sum(ss_quantity) as sum_ss_quantity,
      sum(ss_ext_sales_price) as sum_ss_ext_sales_price
      from tbl_sample ts
      join snowflake_sample_data.tpcds_sf10tcl.date_dim dd on ts.ss_sold_date_sk = dd.d_date_sk
      group by d_date
      order by d_date desc)
;

--Task 05 DELETE SAMPLE
create or replace task demo.public.t05_delete_sample
  warehouse = task_wh
  after demo.public.t04_insert_agg
as
  delete from tbl_sample
;

--Start the Task Group
alter task demo.public.t05_delete_sample resume;
alter task demo.public.t04_insert_agg resume;
alter task demo.public.t03_ctas_agg resume;
alter task demo.public.t02_insert_sample resume;
alter task demo.public.t01_ctas resume;

--Depedencies
show tasks;
select "name", "id", "state", "predecessors" from table(result_scan(last_query_id()));

--Full Task History
(select *
from table(
  information_schema.task_history(
  scheduled_time_range_start=>dateadd('hour',-24,current_timestamp()),
  result_limit => 100,
  task_name=>'t01_ctas')))
union
(select *
from table(
  information_schema.task_history(
  scheduled_time_range_start=>dateadd('hour',-24,current_timestamp()),
  result_limit => 100,
  task_name=>'t02_insert_sample')))
union
(select *
from table(
  information_schema.task_history(
  scheduled_time_range_start=>dateadd('hour',-24,current_timestamp()),
  result_limit => 100,
  task_name=>'t03_ctas_agg')))
union
(select *
from table(
  information_schema.task_history(
  scheduled_time_range_start=>dateadd('hour',-24,current_timestamp()),
  result_limit => 100,
  task_name=>'t04_insert_agg')))
union
(select *
from table(
  information_schema.task_history(
  scheduled_time_range_start=>dateadd('hour',-24,current_timestamp()),
  result_limit => 100,
  task_name=>'t05_delete_sample')));

--Call Get Show 2 with Tasks
call store_get_show2('tasks');
select v::variant from tasks_table;

--Query to Visualize Dependencies Explicit
select *, sys_connect_by_path (path, '->') as dependency_tree , level
from (select v:name::string as name, v:schedule::string as schedule, v:predecessors::string as predecessor,
     v:database_name::string || '.' || v:schema_name::string || '.' || v:name::string as path
     from demo.public.tasks_table)
start with path = 'DEMO.PUBLIC.T01_CTAS'
connect by predecessor = prior path
order by len(dependency_tree) asc
;

--Query to Visualize All Dependencies for Tasks Dynamic with a Recursive CTE
with recursive tasks_layering (layer_id, top_level_task_name, task_name, predecessor_name, dependency_tree) as (
    select
    1 as layer_id,
    path as top_level_task_name,
    path as task_name,
    predecessor as predecessor_name,
    path as dependency_tree from
        (select v:name::string as name, v:schedule::string as schedule, v:predecessors::string as predecessor,
        v:database_name::string || '.' || v:schema_name::string || '.' || v:name::string as path
        from demo.public.tasks_table)
    where predecessor is null
    union all
    select
    layer_id + 1,
    tasks_layering.top_level_task_name,
    path, 
    predecessor,
    dependency_tree || '->' || path from
        (select v:name::string as name, v:schedule::string as schedule, v:predecessors::string as predecessor,
        v:database_name::string || '.' || v:schema_name::string || '.' || v:name::string as path
        from demo.public.tasks_table)
    join tasks_layering on task_name = predecessor)
 select layer_id::integer, top_level_task_name, task_name, predecessor_name, dependency_tree from tasks_layering;


--Queries to Visualize Run & Timing for Explicit Tasks Tree
--Parent Task Visual Heat Map
select
  query_id,
  database_name || '.' || schema_name || '.' || name as task_name,
  scheduled_time,
  datediff('seconds',query_start_time, completed_time) as run_time_in_seconds 
from table(
  information_schema.task_history(
  scheduled_time_range_start=>dateadd('hour',-24,current_timestamp()),
  result_limit => 100,
  task_name=>'t01_ctas'));


--Full Task Run History
select
  'task t01_ctas to t05_delete sample' as task_name,
  root_task_id,
  run_id,
  min(scheduled_time) as start_time,
  datediff('seconds',min(query_start_time), max(completed_time)) as run_time_in_seconds
from
((select *
from table(
  information_schema.task_history(
  scheduled_time_range_start=>dateadd('hour',-24,current_timestamp()),
  result_limit => 100,
  task_name=>'t01_ctas')))
union
(select *
from table(
  information_schema.task_history(
  scheduled_time_range_start=>dateadd('hour',-24,current_timestamp()),
  result_limit => 100,
  task_name=>'t02_insert_sample')))
union
(select *
from table(
  information_schema.task_history(
  scheduled_time_range_start=>dateadd('hour',-24,current_timestamp()),
  result_limit => 100,
  task_name=>'t03_ctas_agg')))
union
(select *
from table(
  information_schema.task_history(
  scheduled_time_range_start=>dateadd('hour',-24,current_timestamp()),
  result_limit => 100,
  task_name=>'t04_insert_agg')))
union
(select *
from table(
  information_schema.task_history(
  scheduled_time_range_start=>dateadd('hour',-24,current_timestamp()),
  result_limit => 100,
  task_name=>'t05_delete_sample'))))
where
  root_task_id = (select distinct root_task_id from
                  table(information_schema.task_history(
                  scheduled_time_range_start=>dateadd('hour',-24,current_timestamp()),
                  result_limit => 100,task_name=>'t01_ctas')))
group by
  task_name,
  root_task_id,
  run_id
order by
  start_time desc;

--Stop the Tasks if you want to Stop Hourly Billing
alter task demo.public.t01_ctas suspend;
alter task demo.public.t02_insert_sample suspend;
alter task demo.public.t03_ctas_agg suspend;
alter task demo.public.t04_insert_agg suspend;
alter task demo.public.t05_delete_sample suspend;