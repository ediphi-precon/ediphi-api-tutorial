select
    jsonb_array_elements(value) ->> 'code' mf1_code
    ,jsonb_array_elements(value) ->> 'description' mf1_desc
    ,jsonb_array_elements(jsonb_array_elements(value)->'children') ->> 'code' mf2_code
    ,jsonb_array_elements(jsonb_array_elements(value)->'children') ->> 'description' mf2_desc
    ,jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(value)->'children')->'children') ->> 'code' mf3_code
    ,jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(value)->'children')->'children') ->> 'description' mf3_desc
from setup s 
where key = 'sort_codes:mf'