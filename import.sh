
#!/bin/bash

. download.sh

# setup our database
echo "Create database immigration-lca"
 dropdb --if-exists immigration-lca
 createdb immigration-lca
 psql immigration-lca -c "CREATE EXTENSION postgis;"
 psql immigration-lca -c "CREATE EXTENSION postgis_topology"
 psql immigration-lca -c "SELECT postgis_full_version()"


#get the DDL below by examining the data: 
# csvsql -i postgresql --table working_h1_b_2014 data/H-1B_FY2014.csv
echo "Create table for data/H-1B_FY2014.csv in the database"
psql immigration-lca -c 'CREATE TABLE "working_h1_b_2014" (
	"LCA_CASE_NUMBER" VARCHAR(18) NOT NULL, 
	"STATUS" VARCHAR(19) NOT NULL, 
	"LCA_CASE_SUBMIT" TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	"DECISION_DATE" TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	"VISA_CLASS" VARCHAR(15) NOT NULL, 
	"LCA_CASE_EMPLOYMENT_START_DATE" DATE, 
	"LCA_CASE_EMPLOYMENT_END_DATE" DATE, 
	"LCA_CASE_EMPLOYER_NAME" VARCHAR(50), 
	"LCA_CASE_EMPLOYER_ADDRESS" VARCHAR(50), 
	"LCA_CASE_EMPLOYER_CITY" VARCHAR(31), 
	"LCA_CASE_EMPLOYER_STATE" VARCHAR(4), 
	"LCA_CASE_EMPLOYER_POSTAL_CODE" VARCHAR(14), 
	"LCA_CASE_SOC_CODE" VARCHAR(10), 
	"LCA_CASE_SOC_NAME" VARCHAR(50), 
	"LCA_CASE_JOB_TITLE" VARCHAR(56), 
	"LCA_CASE_WAGE_RATE_FROM" FLOAT, 
	"LCA_CASE_WAGE_RATE_TO" FLOAT, 
	"LCA_CASE_WAGE_RATE_UNIT" VARCHAR(9), 
	"FULL_TIME_POS" BOOLEAN, 
	"TOTAL_WORKERS" INTEGER, 
	"LCA_CASE_WORKLOC1_CITY" VARCHAR(31), 
	"LCA_CASE_WORKLOC1_STATE" VARCHAR(4), 
	"PW_1" FLOAT, 
	"PW_UNIT_1" VARCHAR(9), 
	"PW_SOURCE_1" VARCHAR(30), 
	"OTHER_WAGE_SOURCE_1" VARCHAR(90), 
	"YR_SOURCE_PUB_1" INTEGER, 
	"LCA_CASE_WORKLOC2_CITY" VARCHAR(30), 
	"LCA_CASE_WORKLOC2_STATE" VARCHAR(4), 
	"PW_2" FLOAT, 
	"PW_UNIT_2" VARCHAR(9), 
	"PW_SOURCE_2" VARCHAR(30), 
	"OTHER_WAGE_SOURCE_2" VARCHAR(90), 
	"YR_SOURCE_PUB_2" VARCHAR(90), 
	"LCA_CASE_NAICS_CODE" INTEGER
);'

########## Need to investigate "YR_SOURCE_PUB_2" VARCHAR(90), 


echo "Import data/H-1B_FY2014.csv to the database"
psql immigration-lca -c "COPY working_h1_b_2014 FROM '`pwd`/data/H-1B_FY2014.csv' DELIMITER ',' CSV HEADER;"


#get the DDL below by examining the data: 
# csvsql -i postgresql --table working_h1_b_2013 data/H-1B_FY2013.csv
 echo "Create table for data/H-1B_FY2013.csv in the database"
 psql immigration-lca -c 'CREATE TABLE working_h1_b_2013 (
	"LCA_CASE_NUMBER" VARCHAR(18) NOT NULL, 
	"STATUS" VARCHAR(50) NOT NULL, 
	"LCA_CASE_SUBMIT" TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	"Decision_Date" TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
	"VISA_CLASS" VARCHAR(26) NOT NULL, 
	"LCA_CASE_EMPLOYMENT_START_DATE" DATE, 
	"LCA_CASE_EMPLOYMENT_END_DATE" DATE, 
	"LCA_CASE_EMPLOYER_NAME" VARCHAR(50), 
	"LCA_CASE_EMPLOYER_ADDRESS" VARCHAR(50), 
	"LCA_CASE_EMPLOYER_CITY" VARCHAR(37), 
	"LCA_CASE_EMPLOYER_STATE" VARCHAR(4), 
	"LCA_CASE_EMPLOYER_POSTAL_CODE" VARCHAR(14), 
	"LCA_CASE_SOC_CODE" VARCHAR(10), 
	"LCA_CASE_SOC_NAME" VARCHAR(50), 
	"LCA_CASE_JOB_TITLE" VARCHAR(50), 
	"LCA_CASE_WAGE_RATE_FROM" FLOAT, 
	"LCA_CASE_WAGE_RATE_TO" FLOAT, 
	"LCA_CASE_WAGE_RATE_UNIT" VARCHAR(16) NOT NULL, 
	"FULL_TIME_POS" BOOLEAN, 
	"TOTAL_WORKERS" INTEGER, 
	"LCA_CASE_WORKLOC1_CITY" VARCHAR(30), 
	"LCA_CASE_WORKLOC1_STATE" VARCHAR(4), 
	"PW_1" VARCHAR(50), 
	"PW_UNIT_1" VARCHAR(9), 
	"PW_SOURCE_1" VARCHAR(30), 
	"OTHER_WAGE_SOURCE_1" VARCHAR(100), 
	"YR_SOURCE_PUB_1" VARCHAR(10), 
	"LCA_CASE_WORKLOC2_CITY" VARCHAR(30), 
	"LCA_CASE_WORKLOC2_STATE" VARCHAR(4), 
	"PW_2" VARCHAR(50), 
	"PW_UNIT_2" VARCHAR(9), 
	"PW_SOURCE_2" VARCHAR(30), 
	"OTHER_WAGE_SOURCE_2" VARCHAR(90), 
	"YR_SOURCE_PUB_2" INTEGER, 
	"LCA_CASE_NAICS_CODE" INTEGER
);'

echo "Import datadata/H-1B_FY2013.csv to the database"
psql immigration-lca -c "COPY working_h1_b_2013 FROM '`pwd`/data/H-1B_FY2013.csv' DELIMITER ',' CSV HEADER;"

# TO CLEAN:
#ERROR:  invalid input syntax for type double precision: "."
#CONTEXT:  COPY working_h1_b_2013, line 29406, column PW_1: "."
psql immigration-lca -c "UPDATE working_h1_b_2013 SET \"PW_1\" = NULL WHERE \"PW_1\" = '.';"
psql immigration-lca -c "UPDATE working_h1_b_2013 SET \"YR_SOURCE_PUB_1\" = NULL WHERE \"YR_SOURCE_PUB_1\" = '.';"


#ERROR:  invalid input syntax for type double precision: "N/A"
#CONTEXT:  COPY working_h1_b_2013, line 287624, column PW_2: "N/A"
psql immigration-lca -c "UPDATE working_h1_b_2013 SET \"PW_2\" = NULL WHERE \"PW_2\" = 'N/A';"

psql immigration-lca -c "UPDATE working_h1_b_2013 SET \"YR_SOURCE_PUB_1\" = NULL WHERE \"YR_SOURCE_PUB_1\" = 'N/A';"

echo "Merge years together"
echo "Create table h1_b in the database"
psql immigration-lca -c 'CREATE TABLE h1_b (
	"LCA_SOURCE_YEAR" INTEGER,
 	"LCA_CASE_NUMBER" VARCHAR(18) NOT NULL, 
 	"STATUS" VARCHAR(50) NOT NULL, 
 	"LCA_CASE_SUBMIT" TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
 	"DECISION_DATE" TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
 	"VISA_CLASS" VARCHAR(26) NOT NULL, 
 	"LCA_CASE_EMPLOYMENT_START_DATE" DATE, 
 	"LCA_CASE_EMPLOYMENT_END_DATE" DATE, 
 	"LCA_CASE_EMPLOYER_NAME" VARCHAR(50), 
 	"LCA_CASE_EMPLOYER_ADDRESS" VARCHAR(50), 
 	"LCA_CASE_EMPLOYER_CITY" VARCHAR(37), 
 	"LCA_CASE_EMPLOYER_STATE" VARCHAR(4), 
 	"LCA_CASE_EMPLOYER_POSTAL_CODE" VARCHAR(14), 
 	"LCA_CASE_SOC_CODE" VARCHAR(10), 
 	"LCA_CASE_SOC_NAME" VARCHAR(50), 
 	"LCA_CASE_JOB_TITLE" VARCHAR(56), 
 	"LCA_CASE_WAGE_RATE_FROM" FLOAT, 
 	"LCA_CASE_WAGE_RATE_TO" FLOAT, 
 	"LCA_CASE_WAGE_RATE_UNIT" VARCHAR(16), 
 	"FULL_TIME_POS" BOOLEAN, 
 	"TOTAL_WORKERS" INTEGER, 
 	"LCA_CASE_WORKLOC1_CITY" VARCHAR(31), 
 	"LCA_CASE_WORKLOC1_STATE" VARCHAR(4), 
 	"PW_1" FLOAT, 
 	"PW_UNIT_1" VARCHAR(9), 
 	"PW_SOURCE_1" VARCHAR(30), 
 	"OTHER_WAGE_SOURCE_1" VARCHAR(100), 
 	"YR_SOURCE_PUB_1" INTEGER, 
 	"LCA_CASE_WORKLOC2_CITY" VARCHAR(30), 
 	"LCA_CASE_WORKLOC2_STATE" VARCHAR(4), 
 	"PW_2" FLOAT, 
 	"PW_UNIT_2" VARCHAR(9), 
	"PW_SOURCE_2" VARCHAR(30), 
	"OTHER_WAGE_SOURCE_2" VARCHAR(90), 
	"YR_SOURCE_PUB_2" VARCHAR(90), 
	"LCA_CASE_NAICS_CODE" INTEGER
);'

echo "Copy 2014 data into merged table"
psql immigration-lca -c 'INSERT INTO h1_b (
		"LCA_SOURCE_YEAR",
		"LCA_CASE_NUMBER", 
		"STATUS", 
		"LCA_CASE_SUBMIT", 
		"DECISION_DATE", 
       	"VISA_CLASS", 
       	"LCA_CASE_EMPLOYMENT_START_DATE", 
       	"LCA_CASE_EMPLOYMENT_END_DATE", 
       	"LCA_CASE_EMPLOYER_NAME", 
       	"LCA_CASE_EMPLOYER_ADDRESS", 
       	"LCA_CASE_EMPLOYER_CITY", 
       	"LCA_CASE_EMPLOYER_STATE", 
       	"LCA_CASE_EMPLOYER_POSTAL_CODE", 
       	"LCA_CASE_SOC_CODE", 
       	"LCA_CASE_SOC_NAME", 
       	"LCA_CASE_JOB_TITLE", 
       	"LCA_CASE_WAGE_RATE_FROM", 
       	"LCA_CASE_WAGE_RATE_TO", 
       	"LCA_CASE_WAGE_RATE_UNIT", 
       	"FULL_TIME_POS", 
       "TOTAL_WORKERS", 
       "LCA_CASE_WORKLOC1_CITY", 
       "LCA_CASE_WORKLOC1_STATE", 
       "PW_1", 
       "PW_UNIT_1", 
       "PW_SOURCE_1", 
       "OTHER_WAGE_SOURCE_1", 
       "YR_SOURCE_PUB_1", 
       "LCA_CASE_WORKLOC2_CITY", 
       "LCA_CASE_WORKLOC2_STATE", 
       "PW_2", 
       "PW_UNIT_2", 
       "PW_SOURCE_2", 
       "OTHER_WAGE_SOURCE_2", 
       "YR_SOURCE_PUB_2", 
       "LCA_CASE_NAICS_CODE")
	SELECT
		2014, 
		"LCA_CASE_NUMBER", 
		"STATUS", 
		"LCA_CASE_SUBMIT", 
		"DECISION_DATE", 
	   	"VISA_CLASS", 
	   	"LCA_CASE_EMPLOYMENT_START_DATE", 
	   	"LCA_CASE_EMPLOYMENT_END_DATE", 
	   	"LCA_CASE_EMPLOYER_NAME", 
	   	"LCA_CASE_EMPLOYER_ADDRESS", 
	   	"LCA_CASE_EMPLOYER_CITY", 
	   	"LCA_CASE_EMPLOYER_STATE", 
	   	"LCA_CASE_EMPLOYER_POSTAL_CODE", 
	   	"LCA_CASE_SOC_CODE", 
	   	"LCA_CASE_SOC_NAME", 
	   	"LCA_CASE_JOB_TITLE", 
	   	"LCA_CASE_WAGE_RATE_FROM", 
	   	"LCA_CASE_WAGE_RATE_TO", 
	   	"LCA_CASE_WAGE_RATE_UNIT", 
	   	"FULL_TIME_POS", 
       "TOTAL_WORKERS", 
       "LCA_CASE_WORKLOC1_CITY", 
       "LCA_CASE_WORKLOC1_STATE", 
       "PW_1", 
       "PW_UNIT_1", 
       "PW_SOURCE_1", 
       "OTHER_WAGE_SOURCE_1", 
       "YR_SOURCE_PUB_1", 
       "LCA_CASE_WORKLOC2_CITY", 
       "LCA_CASE_WORKLOC2_STATE", 
       "PW_2", 
       "PW_UNIT_2", 
       "PW_SOURCE_2", 
       "OTHER_WAGE_SOURCE_2", 
       "YR_SOURCE_PUB_2", 
       "LCA_CASE_NAICS_CODE"
  	FROM working_h1_b_2014;'

echo "Copy 2013 data into merged table"
psql immigration-lca -c 'INSERT INTO h1_b (
		"LCA_SOURCE_YEAR",
		"LCA_CASE_NUMBER", 
		"STATUS", 
		"LCA_CASE_SUBMIT", 
		"DECISION_DATE", 
       	"VISA_CLASS", 
       	"LCA_CASE_EMPLOYMENT_START_DATE", 
       	"LCA_CASE_EMPLOYMENT_END_DATE", 
       	"LCA_CASE_EMPLOYER_NAME", 
       	"LCA_CASE_EMPLOYER_ADDRESS", 
       	"LCA_CASE_EMPLOYER_CITY", 
       	"LCA_CASE_EMPLOYER_STATE", 
       	"LCA_CASE_EMPLOYER_POSTAL_CODE", 
       	"LCA_CASE_SOC_CODE", 
       	"LCA_CASE_SOC_NAME", 
       	"LCA_CASE_JOB_TITLE", 
       	"LCA_CASE_WAGE_RATE_FROM", 
       	"LCA_CASE_WAGE_RATE_TO", 
       	"LCA_CASE_WAGE_RATE_UNIT", 
       	"FULL_TIME_POS", 
       "TOTAL_WORKERS", 
       "LCA_CASE_WORKLOC1_CITY", 
       "LCA_CASE_WORKLOC1_STATE", 
       "PW_1", 
       "PW_UNIT_1", 
       "PW_SOURCE_1", 
       "OTHER_WAGE_SOURCE_1", 
       "YR_SOURCE_PUB_1", 
       "LCA_CASE_WORKLOC2_CITY", 
       "LCA_CASE_WORKLOC2_STATE", 
       "PW_2", 
       "PW_UNIT_2", 
       "PW_SOURCE_2", 
       "OTHER_WAGE_SOURCE_2", 
       "YR_SOURCE_PUB_2", 
       "LCA_CASE_NAICS_CODE")
	SELECT
		2013, 
		"LCA_CASE_NUMBER", 
		"STATUS", 
		"LCA_CASE_SUBMIT", 
		"Decision_Date", 
	   	"VISA_CLASS", 
	   	"LCA_CASE_EMPLOYMENT_START_DATE", 
	   	"LCA_CASE_EMPLOYMENT_END_DATE", 
	   	"LCA_CASE_EMPLOYER_NAME", 
	   	"LCA_CASE_EMPLOYER_ADDRESS", 
	   	"LCA_CASE_EMPLOYER_CITY", 
	   	"LCA_CASE_EMPLOYER_STATE", 
	   	"LCA_CASE_EMPLOYER_POSTAL_CODE", 
	   	"LCA_CASE_SOC_CODE", 
	   	"LCA_CASE_SOC_NAME", 
	   	"LCA_CASE_JOB_TITLE", 
	   	"LCA_CASE_WAGE_RATE_FROM", 
	   	"LCA_CASE_WAGE_RATE_TO", 
	   	"LCA_CASE_WAGE_RATE_UNIT", 
	   	"FULL_TIME_POS", 
       "TOTAL_WORKERS", 
       "LCA_CASE_WORKLOC1_CITY", 
       "LCA_CASE_WORKLOC1_STATE", 
       "PW_1"::FLOAT, 
       "PW_UNIT_1", 
       "PW_SOURCE_1", 
       "OTHER_WAGE_SOURCE_1", 
       "YR_SOURCE_PUB_1"::INTEGER, 
       "LCA_CASE_WORKLOC2_CITY", 
       "LCA_CASE_WORKLOC2_STATE", 
       "PW_2"::FLOAT, 
       "PW_UNIT_2", 
       "PW_SOURCE_2", 
       "OTHER_WAGE_SOURCE_2", 
       "YR_SOURCE_PUB_2", 
       "LCA_CASE_NAICS_CODE"
  	FROM working_h1_b_2013;'

#get the DDL below by examining the data: 
# csvsql -i postgresql --table working_naics_2012 data/NAICS_2-digit_2012_Codes.csv
 echo "Create table for data/NAICS_2-digit_2012_Codes.csv in the database"
 psql immigration-lca -c 'CREATE TABLE working_naics_2012 (
	"Seq. No." INTEGER NOT NULL, 
	"2012 NAICS US   Code" VARCHAR(8) NOT NULL, 
	"2012 NAICS US Title" VARCHAR(118) NOT NULL
);'

echo "Import data/NAICS_2-digit_2012_Codes.csv to the database table working_naics_2012"
psql immigration-lca -c "COPY working_naics_2012 FROM '`pwd`/data/NAICS_2-digit_2012_Codes.csv' DELIMITER ',' CSV HEADER;"

#Cleanse to make sure NAICS_US_Codes don't have trailing ".0"'s
psql immigration-lca -c "UPDATE working_naics_2012 SET \"2012 NAICS US   Code\" = left(\"2012 NAICS US   Code\", length(\"2012 NAICS US   Code\") -2) WHERE \"2012 NAICS US   Code\" NOT like '%-%';"

echo "Create table for enriched 2012 Codes"
psql immigration-lca -c 'CREATE TABLE naics_2012 (
	"sequence_number" INTEGER NOT NULL, 
	"NAICS_US_code" VARCHAR(6) NOT NULL, 
	"NAICS_US_title" VARCHAR(118) NOT NULL,
	"NAICS_US_code_lower_range" integer,
	"NAICS_US_code_upper_range" integer,
	"level" integer,
	"parent" VARCHAR(6),
	"l2_parent" VARCHAR(6),
	"l3_parent" VARCHAR(6),
	"l4_parent" VARCHAR(6),
	"l5_parent" VARCHAR(6)
);'

echo "Copy 2012 Codes data to enriched tables"
psql immigration-lca -c '
INSERT INTO naics_2012 (
	"sequence_number" , 
	"NAICS_US_code", 
	"NAICS_US_title")
	SELECT  
		"Seq. No.", 
		LEFT("2012 NAICS US   Code", 6), 
		"2012 NAICS US Title"
		FROM working_naics_2012;'

echo "Enrich NAICS 2012 codes so hierarchy data can be easily queried"
psql immigration-lca -c "
	UPDATE naics_2012 SET \"NAICS_US_code_lower_range\" = \"NAICS_US_code\"::INTEGER, \"NAICS_US_code_upper_range\" = \"NAICS_US_code\"::INTEGER WHERE \"NAICS_US_code\" NOT LIKE '%-%';
	UPDATE naics_2012 SET \"NAICS_US_code_lower_range\" = LEFT(\"NAICS_US_code\", POSITION('-' IN \"NAICS_US_code\") - 1)::INTEGER, \"NAICS_US_code_upper_range\" = RIGHT(\"NAICS_US_code\", LENGTH(\"NAICS_US_code\") - POSITION('-' IN \"NAICS_US_code\"))::INTEGER WHERE \"NAICS_US_code\" LIKE '%-%';

	UPDATE naics_2012 n1 SET level = LENGTH(\"NAICS_US_code_lower_range\"::VARCHAR), parent = (SELECT \"NAICS_US_code\" FROM naics_2012 n2 WHERE LEFT(n1.\"NAICS_US_code\", LENGTH(n1.\"NAICS_US_code\")-1)::INTEGER <= n2.\"NAICS_US_code_upper_range\" AND LEFT(n1.\"NAICS_US_code\", LENGTH(n1.\"NAICS_US_code\")-1)::INTEGER >= n2.\"NAICS_US_code_lower_range\") WHERE \"NAICS_US_code\" NOT LIKE '%-%';
	UPDATE naics_2012 n1 SET level = LENGTH(\"NAICS_US_code_lower_range\"::VARCHAR) WHERE \"NAICS_US_code\" LIKE '%-%';

	UPDATE naics_2012 SET l5_parent = (SELECT parent FROM naics_2012 parent_naics_2012 WHERE parent_naics_2012.\"NAICS_US_code\" = naics_2012.\"NAICS_US_code\") where level = 6;
	UPDATE naics_2012 SET l5_parent = \"NAICS_US_code\" where level = 5;

	UPDATE naics_2012 SET l4_parent = (SELECT parent FROM naics_2012 parent_naics_2012 WHERE parent_naics_2012.\"NAICS_US_code\" = naics_2012.\"l5_parent\") where level >= 5;
	UPDATE naics_2012 SET l4_parent = \"NAICS_US_code\" where level = 4;

	UPDATE naics_2012 SET l3_parent = (SELECT parent FROM naics_2012 parent_naics_2012 WHERE parent_naics_2012.\"NAICS_US_code\" = naics_2012.\"l4_parent\") where level >= 4;
	UPDATE naics_2012 SET l3_parent = \"NAICS_US_code\" where level = 3;

	UPDATE naics_2012 SET l2_parent = (SELECT parent FROM naics_2012 parent_naics_2012 WHERE parent_naics_2012.\"NAICS_US_code\" = naics_2012.\"l3_parent\") where level >= 3;
	UPDATE naics_2012 SET l2_parent = \"NAICS_US_code\" where level = 2;"



