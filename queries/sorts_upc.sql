with cte_extras as (
	select 
		t.id
		,jsonb_object_keys(t.extras) sf_keys
		,extras ->> jsonb_object_keys(t.extras) sc_keys
	from products t
)	
select 
	e.id
	,f.name code_name
	,c.code
	,c.description
from    (cte_extras e
left outer join sort_fields f
on f.id::text = e.sf_keys )
left outer join sort_codes c
on c.id::text = e.sc_keys 