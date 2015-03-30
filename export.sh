#!/bin/bash

echo "Generate CSVs for visualizations"
mkdir -p export/vis
psql immigration-lca -c "COPY (
	SELECT 	
		\"LCA_SOURCE_YEAR\", 
		\"LCA_CASE_NUMBER\", 
		\"VISA_CLASS\", 
		\"LCA_CASE_EMPLOYER_NAME\", 
		\"LCA_CASE_SOC_NAME\", 
		\"LCA_CASE_WAGE_RATE_FROM\", 
		\"LCA_CASE_WORKLOC1_STATE\", 
		\"PW_1\", 
	    NAICS_2012.\"NAICS_US_title\" AS \"LCA_CASE_NAICS_TITLE\",
	    \"TOTAL_WORKERS\"
	FROM public.h1_b
		LEFT JOIN NAICS_2012 ON h1_b.\"LCA_CASE_NAICS_CODE\"::VARCHAR = NAICS_2012.\"NAICS_US_code\"
	WHERE 
		UPPER(h1_b.\"STATUS\") = 'CERTIFIED'
		AND
		\"FULL_TIME_POS\" = TRUE
		AND
		\"LCA_CASE_WAGE_RATE_UNIT\" = 'Year'
		AND
		\"PW_UNIT_1\" = 'Year'
	) TO '`pwd`/export/vis/lca_h1b.csv' WITH CSV HEADER;"