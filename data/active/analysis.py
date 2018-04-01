mortality=
"create view mortality as\
select a.concept_name as state, count(*) as pregnant_deaths  from concept as a,measurements \
where measurement_concept_id in(select concept_id from concept where concept_name='state') \
 and a.concept_id= measurement_value_concept_id\
 and measurements.entity_id in \
 	(select entity.id from entity,concept where concept.concept_name like '%mort%' and  entity.source_concept_id=concept.concept_id)\
 group by state order by state asc"

 care_sites=
 "create view care_sites as\
 select a.concept_name as state, count(*) as care_sites  from concept as a,measurements \
 where measurement_concept_id in(select concept_id from concept where concept_name='state') \
 and a.concept_id= measurement_value_concept_id\
 and measurements.entity_id in \
 	(select entity_id from measurements where measurement_concept_id in(select concept_id from concept where domain_id='center'))\
 group by state order by state asc"


still_births=
"create view still_births\
as\
select concept_name as state,sum(a.measurement_value_as_value::int) as still_births\
from concept,measurements as a,measurements\
where \
measurements.measurement_concept_id in \
	(select concept_id from concept where concept_name='state')\
and concept.concept_id=measurements.measurement_value_concept_id\
and a.measurement_concept_id in\
	(select concept_id from concept where concept_name like '%still%births%')\
and measurements.entity_id=a.entity_id\
group by state\
order by state"

total_outcomes=
"create view total_outcomes\
as\
select concept_name as state,sum(a.measurement_value_as_value::int) as total_outcome \
from concept,measurements as a,measurements\
where \
measurements.measurement_concept_id in \
	(select concept_id from concept where concept_name='state')\
and concept.concept_id=measurements.measurement_value_concept_id\
and a.measurement_concept_id in\
	(select concept_id from concept where concept_name like '%total%outcome%')\
and measurements.entity_id=a.entity_id\
group by state\
order by state"

join=
"select still_births.state,pregnant_deaths,care_sites,total_outcome,still_births\
from mortality join care_sites join total_outcomes join still_births on\
mortality.state=care_sites.state and\
care_sites.state=total_outcomes.state and\
total_outcomes.state=still_births.state\
order by still_births.state"