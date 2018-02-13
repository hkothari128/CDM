This zip file contains a 1000 person sample of simulated CMS SynPUF patient data. These csv data files have been tranformed into the OMOP Common Data Model Version 5 (CDMV5) format.

This publically available data set is useful for testing & creating demos for the OHDSI open source applications.
Note. This is a first release of the data set and improvements will be made in future releases (in particular populating the CDMV5 cost tables).

Dependencies:

1) Create the CDMV5 database
2) Load the CDMV5 vocabulary tables from the OHDSI Athena web site
3) The SQL load script is specific to PostgreSQL.  However it should be possible to create similar similar load scripts for other DBMSs, including SQL Server & Oracle.

Created by:
Lee Evans 
LTS Computing LLC
http://www.ltscomputingllc.com