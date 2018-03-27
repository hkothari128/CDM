
select a.concept_name as state,b.concept_name as district, count(*)  from concept as a,concept as b,measurements where measurement_concept_id in(select concept_id from concept where concept_name='district') 
and b.concept_id= measurement_value_concept_id and entity_id in (select entity_id from measurements where measurement_concept_id in(select concept_id from concept where concept_name='state') 
 and a.concept_id= measurement_value_concept_id) group by state,district
 