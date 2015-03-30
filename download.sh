#!/bin/bash

# echo 'downloading H-1B_FY14_Q4.xlsx'
# wget -P data/ http://www.foreignlaborcert.doleta.gov/docs/py2014q4/H-1B_FY14_Q4.xlsx
# echo 'converting H-1B_FY14_Q4.xlsx to CSV'
# in2csv data/H-1B_FY14_Q4.xlsx > data/H-1B_FY2014.csv


# echo 'downloading LCA_FY2013.xlsx'
# wget -P data/ http://www.foreignlaborcert.doleta.gov/docs/lca/LCA_FY2013.xlsx
# echo 'converting LCA_FY2013.xlsx to CSV'
# in2csv data/LCA_FY2013.xlsx > data/H-1B_FY2013.csv

#see https://www.census.gov/cgi-bin/sssd/naics/naicsrch?chart=2012
echo 'downloading NAICS 2-digit_2012_Codes.xls'
wget -P data/ https://www.census.gov/eos/www/naics/2012NAICS/2-digit_2012_Codes.xls
echo 'converting 2-digit_2012_Codes.xls to CSV'
#use csvcut to get rid of blank rows
in2csv data/2-digit_2012_Codes.xls | csvcut -x > data/NAICS_2-digit_2012_Codes.csv