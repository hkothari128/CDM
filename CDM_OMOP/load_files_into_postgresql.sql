--------------------------------------------------------------------
-- This SQL script loads a 1000 person sample of simulated CMS SynPUF patient data from csv files into a subset of the OMOP Common Data Model Version 5 (CDMV5) tables.
--
-- Change the below file paths to reference the folder where you unzipped the csv data files
--
-- LTS Computing LLC
-- http://www.ltscomputingllc.com
--------------------------------------------------------------------
--
COPY CARE_SITE FROM '/home/huzaifa/Desktop/college/BE/BE_PROJECT/CDM_OMOP/CDM_CARE_SITE.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
COPY CONDITION_OCCURRENCE FROM '/home/huzaifa/Desktop/college/BE/BE_PROJECT/CDM_OMOP/CDM_CONDITION_OCCURRENCE.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
COPY DEATH FROM '/home/huzaifa/Desktop/college/BE/BE_PROJECT/CDM_OMOP/CDM_DEATH.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
COPY DRUG_EXPOSURE FROM '/home/huzaifa/Desktop/college/BE/BE_PROJECT/CDM_OMOP/CDM_DRUG_EXPOSURE.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
COPY DEVICE_EXPOSURE FROM '/home/huzaifa/Desktop/college/BE/BE_PROJECT/CDM_OMOP/CDM_DEVICE_EXPOSURE.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
COPY LOCATION FROM '/home/huzaifa/Desktop/college/BE/BE_PROJECT/CDM_OMOP/CDM_LOCATION.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
COPY MEASUREMENT FROM '/home/huzaifa/Desktop/college/BE/BE_PROJECT/CDM_OMOP/CDM_MEASUREMENT.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
COPY OBSERVATION FROM '/home/huzaifa/Desktop/college/BE/BE_PROJECT/CDM_OMOP/CDM_OBSERVATION.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
COPY OBSERVATION_PERIOD FROM '/home/huzaifa/Desktop/college/BE/BE_PROJECT/CDM_OMOP/CDM_OBSERVATION_PERIOD.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
COPY PERSON FROM '/home/huzaifa/Desktop/college/BE/BE_PROJECT/CDM_OMOP/CDM_PERSON.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
COPY PROCEDURE_OCCURRENCE FROM '/home/huzaifa/Desktop/college/BE/BE_PROJECT/CDM_OMOP/CDM_PROCEDURE_OCCURRENCE.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
COPY PROVIDER FROM '/home/huzaifa/Desktop/college/BE/BE_PROJECT/CDM_OMOP/CDM_PROVIDER.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
COPY VISIT_OCCURRENCE FROM '/home/huzaifa/Desktop/college/BE/BE_PROJECT/CDM_OMOP/CDM_VISIT_OCCURRENCE.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
COPY DRUG_ERA FROM '/home/huzaifa/Desktop/college/BE/BE_PROJECT/CDM_OMOP/CDM_DRUG_ERA.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
COPY CONDITION_ERA FROM '/home/huzaifa/Desktop/college/BE/BE_PROJECT/CDM_OMOP/CDM_CONDITION_ERA.csv' WITH DELIMITER E',' CSV HEADER QUOTE E'\b';
