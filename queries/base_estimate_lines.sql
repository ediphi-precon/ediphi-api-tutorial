select 
    l.id
    ,l.name
    ,l.quantity
    ,l.uom
    ,total_uc
    ,(l.uf ->> 'uf1') uf1_code
    ,(l.uf ->> 'uf2') uf2_code
    ,(l.uf ->> 'uf3') uf3_code
    ,(l.mf ->> 'mf1') mf1_code
    ,(l.mf ->> 'mf2') mf2_code
    ,(l.mf ->> 'mf3') mf3_code
__ADD_COLS__
from line_items l
where estimate = '__ESTIMATE_ID__'