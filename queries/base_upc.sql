select 
	p.id
	,p.name
	,p.uom
	,(p.uf ->> 'uf1') uf1_code
	,(p.uf ->> 'uf2') uf2_code
	,(p.uf ->> 'uf3') uf3_code
	,(p.mf ->> 'mf1') mf1_code
	,(p.mf ->> 'mf2') mf2_code
	,(p.mf ->> 'mf3') mf3_code
from products p