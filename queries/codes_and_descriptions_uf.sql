select
    jsonb_array_elements(value) ->> 'code' uf1_code
    ,jsonb_array_elements(value) ->> 'description' uf1_desc
    ,jsonb_array_elements(jsonb_array_elements(value)->'children') ->> 'code' uf2_code
    ,jsonb_array_elements(jsonb_array_elements(value)->'children') ->> 'description' uf2_desc
    ,jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(value)->'children')->'children') ->> 'code' uf3_code
    ,jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(value)->'children')->'children') ->> 'description' uf3_desc
from setup s 
where key = 'sort_codes:uf'