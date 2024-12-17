select 
    l.id
    ,l.name
    ,l.uom
    ,(l.uf ->> 'uf1') uf1_code
    ,(l.uf ->> 'uf2') uf2_code
    ,(l.uf ->> 'uf3') uf3_code
    ,(l.mf ->> 'mf1') mf1_code
    ,(l.mf ->> 'mf2') mf2_code
    ,(l.mf ->> 'mf3') mf3_code
from line_items l
where estimate = '__ESTIMATE_ID__'