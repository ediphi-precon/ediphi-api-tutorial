with cte_tables as (
	select 
		c.oid table_oid
		,c.relname table_name
	from pg_catalog.pg_class c
	where pg_catalog.pg_table_is_visible(c.oid)
)
,cte_columns as (
	select 
		t.table_name
		,a.attname column_name
		,pg_catalog.format_type(a.atttypid, a.atttypmod) data_type
	from pg_catalog.pg_attribute a
	join cte_tables t
	on t.table_oid = a.attrelid
	where a.attnum > 0
	and not a.attisdropped
)
,cte_constraints as (
	select 
		c.conrelid::pg_catalog.regclass::text table_name
		,case when pg_get_constraintdef(oid)::text like 'PRIMARY KEY (%' then substring(pg_get_constraintdef(oid)::text from '\((.*?)\)') else null end pk
		,case when pg_get_constraintdef(oid)::text like 'FOREIGN KEY (%' then substring(pg_get_constraintdef(oid)::text from '\((.*?)\)') else null end fk
		,case when pg_get_constraintdef(oid)::text like 'FOREIGN KEY (%' then substring(pg_get_constraintdef(oid)::text from 'REFERENCES\s+([^()]+)\(') else null end references_table
		,case when pg_get_constraintdef(oid)::text like 'FOREIGN KEY (%' then substring(pg_get_constraintdef(oid)::text from 'REFERENCES\s+[^(]+\(([^)]+)\)') else null end references_column
	from pg_constraint c
	where contype in ('f', 'p ')
	and connamespace = 'public'::regnamespace
	and c.conrelid::pg_catalog.regclass::text in (select table_name from cte_tables)
	order by conrelid::regclass::text, contype desc
)
select
	c.table_name
	,c.column_name
	,c.data_type
	,case when cp.pk is not null then true else false end is_pk
	,case when cf.fk is not null then true else false end is_fk
	,cf.references_table
	,cf.references_column
from    (cte_columns c
left outer join cte_constraints cp
on ((cp.table_name = c.table_name) and (cp.pk = c.column_name)) )
left outer join cte_constraints cf
on ((cf.table_name = c.table_name) and (cf.fk = c.column_name))
