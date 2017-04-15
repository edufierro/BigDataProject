import numpy as np
from math import sqrt
from operator import add
from pyspark.mllib.clustering import KMeans, KMeansModel
from csv import reader

csvfile = sc.textFile('NYPD_crime.csv')
crimedata = csvfile.mapPartitions(lambda x: reader(x))
#crimedata = csvfile.map(lambda line: line.split(','))

# crimedata.count()

column_names = crimedata.take(1)[0]
crimedata = crimedata.filter(lambda line: line != column_names )
dataframe = spark.createDataFrame(crimedata, ('CMPLNT_NUM', 'CMPLNT_FR_DT', 'CMPLNT_FR_TM', 'CMPLNT_TO_DT', 'CMPLNT_TO_TM', 'RPT_DT', 'KY_CD', 'OFNS_DESC', 'PD_CD', 'PD_DESC', 'CRM_ATPT_CPTD_CD', 'LAW_CAT_CD', 'JURIS_DESC', 'BORO_NM', 'ADDR_PCT_CD', 'LOC_OF_OCCUR_DESC', 'PREM_TYP_DESC', 'PARKS_NM', 'HADEVELOPT', 'X_COORD_CD', 'Y_COORD_CD', 'Latitude', 'Longitude', 'Lat_Lon' ) )
dataframe.createOrReplaceTempView("df")

# 'CMPLNT_NUM', 'CMPLNT_FR_DT', 'CMPLNT_FR_TM', 'CMPLNT_TO_DT', 
# 'CMPLNT_TO_TM', 'RPT_DT', 'KY_CD', 'OFNS_DESC', 'PD_CD', 
# 'PD_DESC', 'CRM_ATPT_CPTD_CD', 'LAW_CAT_CD', 'JURIS_DESC', 
# 'BORO_NM', 'ADDR_PCT_CD', 'LOC_OF_OCCUR_DESC', 'PREM_TYP_DESC', 
# 'PARKS_NM', 'HADEVELOPT', 'X_COORD_CD', 'Y_COORD_CD', 'Latitude', 
# 'Longitude', 'Lat_Lon'

# Col 1 ------ CMPLNT_NUM 
# Randomly generated persistent ID for each complaint 

		# Check: all different
spark.sql("SELECT COUNT (DISTINCT CMPLNT_NUM) FROM df").show()
spark.sql("SELECT COUNT (CMPLNT_NUM) FROM df").show()

		#Check max and min values
spark.sql("SELECT MAX(int(CMPLNT_NUM)) FROM df").show()
spark.sql("SELECT MIN(int(CMPLNT_NUM)) FROM df").show()

spark.sql("SELECT CMPLNT_NUM ,'integer' AS base_type ,'commplaint number' AS semantic_type, CASE WHEN CMPLNT_NUM = '' THEN 'null' WHEN CMPLNT_NUM >= 100000000 AND CMPLNT_NUM <= 999999999 THEN 'valid' ELSE 'invalid' END AS is_valid FROM df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT CMPLNT_NUM ,'integer' AS base_type ,'commplaint number' AS semantic_type, CASE WHEN CMPLNT_NUM = '' THEN 'null' WHEN CMPLNT_NUM >= 100000000 AND CMPLNT_NUM <= 999999999 THEN 'valid' ELSE 'invalid' END AS is_valid FROM df) group by is_valid").show()

# Col 2 ----- CMPLNT_FR_DT
# Exact date of occurrence for the reported event

spark.sql("SELECT distinct( SUBSTRING(CMPLNT_FR_DT,7,4)) from df ").show()
spark.sql("SELECT min( int(SUBSTRING(CMPLNT_FR_DT,7,4))) from df limit 500").show()


spark.sql("SELECT CMPLNT_FR_DT ,'timestamp' AS base_type ,'date of occurrence' AS semantic_type, CASE WHEN CMPLNT_FR_DT = '' THEN 'null' WHEN SUBSTRING(CMPLNT_FR_DT,1,2) IN ('12','11','10','09','08','07','06','05','04','03','02','01') AND SUBSTRING(CMPLNT_FR_DT,4,2) IN ('01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31') AND int(SUBSTRING(CMPLNT_FR_DT,7,4)) > 1900 AND int(SUBSTRING(CMPLNT_FR_DT,7,4)) < 2018 THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT CMPLNT_FR_DT ,'timestamp' AS base_type ,'date of occurrence' AS semantic_type, CASE WHEN CMPLNT_FR_DT = '' THEN 'null' WHEN SUBSTRING(CMPLNT_FR_DT,1,2) IN ('12','11','10','09','08','07','06','05','04','03','02','01') AND SUBSTRING(CMPLNT_FR_DT,4,2) IN ('01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31') AND int(SUBSTRING(CMPLNT_FR_DT,7,4)) > 1900 AND int(SUBSTRING(CMPLNT_FR_DT,7,4)) < 2018 THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()

# COL 3 ---- time of occurrence: CMPLNT_FR_TM

spark.sql("SELECT CMPLNT_FR_TM from df ").show()

spark.sql("SELECT CMPLNT_FR_TM ,'timestamp' AS base_type ,'time of occurrence' AS semantic_type, CASE WHEN CMPLNT_FR_TM = '' THEN 'null' WHEN int(SUBSTRING(CMPLNT_FR_DT,1,2)) <= 23 AND  int(SUBSTRING(CMPLNT_FR_DT,1,2)) >=0 AND SUBSTRING(CMPLNT_FR_DT,4,2) < 60 AND SUBSTRING(CMPLNT_FR_DT,4,2) >= 0 AND SUBSTRING(CMPLNT_FR_DT,7,2) < 60 AND SUBSTRING(CMPLNT_FR_DT,7,2) >= 0 THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT CMPLNT_FR_TM ,'timestamp' AS base_type ,'time of occurrence' AS semantic_type, CASE WHEN CMPLNT_FR_TM = '' THEN 'null' WHEN int(SUBSTRING(CMPLNT_FR_DT,1,2)) <= 23 AND  int(SUBSTRING(CMPLNT_FR_DT,1,2)) >=0 AND SUBSTRING(CMPLNT_FR_DT,4,2) < 60 AND SUBSTRING(CMPLNT_FR_DT,4,2) >= 0 AND SUBSTRING(CMPLNT_FR_DT,7,2) < 60 AND SUBSTRING(CMPLNT_FR_DT,7,2) >= 0 THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()

# COL 4 ---- CMPLNT_TO_DT
# "Ending date of occurrence for the reported event, if exact time of occurrence is unknown"

spark.sql("SELECT CMPLNT_TO_DT ,'timestamp' AS base_type ,'ending date of occurrence' AS semantic_type, CASE WHEN CMPLNT_TO_DT = '' THEN 'null' WHEN SUBSTRING(CMPLNT_TO_DT,1,2) IN ('12','11','10','09','08','07','06','05','04','03','02','01') AND SUBSTRING(CMPLNT_TO_DT,4,2) IN ('01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31') AND int(SUBSTRING(CMPLNT_TO_DT,7,4)) > 1900 AND int(SUBSTRING(CMPLNT_TO_DT,7,4)) < 2018 THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT CMPLNT_TO_DT ,'timestamp' AS base_type ,'ending date of occurrence' AS semantic_type, CASE WHEN CMPLNT_TO_DT = '' THEN 'null' WHEN SUBSTRING(CMPLNT_TO_DT,1,2) IN ('12','11','10','09','08','07','06','05','04','03','02','01') AND SUBSTRING(CMPLNT_TO_DT,4,2) IN ('01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31') AND int(SUBSTRING(CMPLNT_TO_DT,7,4)) > 1900 AND int(SUBSTRING(CMPLNT_TO_DT,7,4)) < 2018 THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()


# COL 5 ---- CMPLNT_TO_TM
# "Ending time of occurrence for the reported event, if exact time of occurrence is unknown"

spark.sql("SELECT CMPLNT_TO_TM ,'timestamp' AS base_type ,'ending time of occurrence' AS semantic_type, CASE WHEN CMPLNT_TO_TM = '' THEN 'null' WHEN int(SUBSTRING(CMPLNT_TO_TM,1,2)) <= 23 AND  int(SUBSTRING(CMPLNT_TO_TM,1,2)) >=0 AND SUBSTRING(CMPLNT_TO_TM,4,2) < 60 AND SUBSTRING(CMPLNT_TO_TM,4,2) >= 0 AND SUBSTRING(CMPLNT_TO_TM,7,2) < 60 AND SUBSTRING(CMPLNT_TO_TM,7,2) >= 0 THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT CMPLNT_TO_TM ,'timestamp' AS base_type ,'ending time of occurrence' AS semantic_type, CASE WHEN CMPLNT_TO_TM = '' THEN 'null' WHEN int(SUBSTRING(CMPLNT_TO_TM,1,2)) <= 23 AND  int(SUBSTRING(CMPLNT_TO_TM,1,2)) >=0 AND SUBSTRING(CMPLNT_TO_TM,4,2) < 60 AND SUBSTRING(CMPLNT_TO_TM,4,2) >= 0 AND SUBSTRING(CMPLNT_TO_TM,7,2) < 60 AND SUBSTRING(CMPLNT_TO_TM,7,2) >= 0 THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()


# COL 6 ----- RPT_DT
# Date event was reported to police 

spark.sql("SELECT RPT_DT ,'timestamp' AS base_type ,'date of report ' AS semantic_type, CASE WHEN RPT_DT = '' THEN 'null' WHEN SUBSTRING(RPT_DT,1,2) IN ('12','11','10','09','08','07','06','05','04','03','02','01') AND SUBSTRING(RPT_DT,4,2) IN ('01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31') AND int(SUBSTRING(RPT_DT,7,4)) > 1900 AND int(SUBSTRING(RPT_DT,7,4)) < 2018 THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT RPT_DT ,'timestamp' AS base_type ,'date of report ' AS semantic_type, CASE WHEN RPT_DT = '' THEN 'null' WHEN SUBSTRING(RPT_DT,1,2) IN ('12','11','10','09','08','07','06','05','04','03','02','01') AND SUBSTRING(RPT_DT,4,2) IN ('01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31') AND int(SUBSTRING(RPT_DT,7,4)) > 1900 AND int(SUBSTRING(RPT_DT,7,4)) < 2018 THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()

# COL 7 --- KY_CD 
# Three digit offense classification code

spark.sql("SELECT min(KY_CD) FROM df limit 100").show()

spark.sql("SELECT KY_CD ,'integer' AS base_type ,'classification code' AS semantic_type, CASE WHEN KY_CD = '' THEN 'null' WHEN int(KY_CD) > 99 AND int(KY_CD) < 1000 THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT distinct is_valid from (SELECT KY_CD ,'integer' AS base_type ,'classification code' AS semantic_type, CASE WHEN KY_CD = '' THEN 'null' WHEN int(KY_CD) > 99 AND int(KY_CD) < 1000 THEN 'valid' ELSE 'invalid' END AS is_valid from df)").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT KY_CD ,'integer' AS base_type ,'classification code' AS semantic_type, CASE WHEN KY_CD = '' THEN 'null' WHEN int(KY_CD) > 99 AND int(KY_CD) < 1000 THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()

# Col 8 --- OFNS_DESC
# Description of offense corresponding with key code
spark.sql("SELECT distinct OFNS_DESC FROM df limit 100").show(2000)

spark.sql("SELECT OFNS_DESC ,'text' AS base_type ,'offense description' AS semantic_type, CASE WHEN OFNS_DESC = '' THEN 'null' WHEN OFNS_DESC NOT LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql(" SELECT is_valid, count(is_valid) from (SELECT OFNS_DESC ,'text' AS base_type ,'offesnse description' AS semantic_type, CASE WHEN OFNS_DESC = '' THEN 'null' WHEN OFNS_DESC NOT LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()

# Col 9 --- PD_CD
# Three digit internal classification code (more granular than Key Code)

spark.sql("SELECT PD_CD, count(PD_CD) FROM df group by PD_CD").show(2000)

spark.sql("SELECT PD_CD ,'integer' AS base_type ,'internal code' AS semantic_type, CASE WHEN PD_CD = '' THEN 'null' WHEN PD_CD not LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT PD_CD ,'integer' AS base_type ,'internal code' AS semantic_type, CASE WHEN PD_CD = '' THEN 'null' WHEN PD_CD NOT LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()


# Col 10 --- PD_DESC
# Description of internal classification corresponding with PD code (more granular than Offense Description)
spark.sql("SELECT count(distinct(PD_DESC)) FROM df").show(2000)

spark.sql("SELECT PD_DESC ,'text' AS base_type ,'internal description' AS semantic_type, CASE WHEN PD_DESC = '' THEN 'null' WHEN PD_DESC NOT LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT PD_DESC ,'text' AS base_type ,'internal description' AS semantic_type, CASE WHEN PD_DESC = '' THEN 'null' WHEN PD_DESC NOT LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()

# Col 11 --- CRM_ATPT_CPTD_CD
# "Indicator of whether crime was successfully completed or attempted, but failed or was interrupted prematurely"

spark.sql("SELECT CRM_ATPT_CPTD_CD, count(CRM_ATPT_CPTD_CD) FROM df group by CRM_ATPT_CPTD_CD").show(2000)

spark.sql("SELECT CRM_ATPT_CPTD_CD ,'text' AS base_type ,'crime status' AS semantic_type, CASE WHEN CRM_ATPT_CPTD_CD = '' THEN 'null' WHEN CRM_ATPT_CPTD_CD IN ('COMPLETED','ATTEMPTED') THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT CRM_ATPT_CPTD_CD ,'text' AS base_type ,'crime status' AS semantic_type, CASE WHEN CRM_ATPT_CPTD_CD = '' THEN 'null' WHEN CRM_ATPT_CPTD_CD IN ('COMPLETED','ATTEMPTED') THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()


# COL 12 --- LAW_CAT_CD
# "Level of offense: felony, misdemeanor, violation "
spark.sql("SELECT LAW_CAT_CD, count(LAW_CAT_CD) FROM df group by LAW_CAT_CD").show(2000)

spark.sql("SELECT LAW_CAT_CD ,'text' AS base_type ,'crime status' AS semantic_type, CASE WHEN LAW_CAT_CD = '' THEN 'null' WHEN LAW_CAT_CD IN ('FELONY','VIOLATION','MISDEMEANOR') THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT LAW_CAT_CD ,'text' AS base_type ,'crime status' AS semantic_type, CASE WHEN LAW_CAT_CD = '' THEN 'null' WHEN LAW_CAT_CD IN ('FELONY','VIOLATION','MISDEMEANOR') THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid ").show()


# Col 13 --- JURIS_DESC,
# "Jurisdiction responsible for incident. Either internal, like Police, Transit, and Housing; or external, like Correction, Port Authority, etc."
spark.sql("SELECT JURIS_DESC, count(JURIS_DESC) FROM df group by JURIS_DESC").show(2000)

spark.sql("SELECT JURIS_DESC ,'text' AS base_type ,'Jurisdiction responsible' AS semantic_type, CASE WHEN JURIS_DESC = '' THEN 'null' WHEN JURIS_DESC not like '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT JURIS_DESC ,'text' AS base_type ,'Jurisdiction responsible' AS semantic_type, CASE WHEN JURIS_DESC = '' THEN 'null' WHEN JURIS_DESC not like '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()

# COL 14 --- BORO_NM
# The name of the borough in which the incident occurred

spark.sql("SELECT BORO_NM, count(BORO_NM) FROM df group by BORO_NM").show(2000)

spark.sql("SELECT BORO_NM ,'text' AS base_type ,'borough' AS semantic_type, CASE WHEN BORO_NM = '' THEN 'null' WHEN BORO_NM IN ('QUEENS','BROOKLYN','BRONX','MANHATTAN','STATEN ISLAND') THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT BORO_NM ,'text' AS base_type ,'borough' AS semantic_type, CASE WHEN BORO_NM = '' THEN 'null' WHEN BORO_NM IN ('QUEENS','BROOKLYN','BRONX','MANHATTAN','STATEN ISLAND') THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()

# Col 15 --- ADDR_PCT_CD
# The precinct in which the incident occurred

spark.sql("SELECT ADDR_PCT_CD, count(ADDR_PCT_CD) FROM df group by ADDR_PCT_CD").show(2000)

spark.sql("SELECT ADDR_PCT_CD, 'integer' AS base_type ,'precinct' AS semantic_type, CASE WHEN ADDR_PCT_CD = '' THEN 'null' WHEN ADDR_PCT_CD not like '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT ADDR_PCT_CD, 'integer' AS base_type ,'precinct' AS semantic_type, CASE WHEN ADDR_PCT_CD = '' THEN 'null' WHEN ADDR_PCT_CD not like '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()

# Col 16 LOC_OF_OCCUR_DESC
# Specific location of occurrence in or around the premises; inside, opposite of, front of, rear of"
spark.sql("SELECT LOC_OF_OCCUR_DESC, count(LOC_OF_OCCUR_DESC) FROM df group by LOC_OF_OCCUR_DESC").show(2000)

spark.sql("SELECT LOC_OF_OCCUR_DESC,'text' AS base_type ,'specific location' AS semantic_type, CASE WHEN LOC_OF_OCCUR_DESC IN ('',' ') THEN 'null' WHEN LOC_OF_OCCUR_DESC IN ('OPPOSITE OF','REAR OF','INSIDE','OUTSIDE','FRONT OF') THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT LOC_OF_OCCUR_DESC,'text' AS base_type ,'specific location' AS semantic_type, CASE WHEN LOC_OF_OCCUR_DESC IN ('',' ') THEN 'null' WHEN LOC_OF_OCCUR_DESC IN ('OPPOSITE OF','REAR OF','INSIDE','OUTSIDE','FRONT OF') THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()

# Col 17 PREM_TYP_DESC
# Specific description of premises; grocery store, residence, street, etc."
spark.sql("SELECT PREM_TYP_DESC, count(PREM_TYP_DESC) FROM df group by PREM_TYP_DESC").show(2000)

spark.sql("SELECT PREM_TYP_DESC,'text' AS base_type ,'specific location' AS semantic_type, CASE WHEN PREM_TYP_DESC IN ('',' ') THEN 'null' WHEN PREM_TYP_DESC NOT LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT PREM_TYP_DESC,'text' AS base_type ,'specific location' AS semantic_type, CASE WHEN PREM_TYP_DESC IN ('',' ') THEN 'null' WHEN PREM_TYP_DESC NOT LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()

# Col 18 PARKS_NM
# Name of NYC park, playground or greenspace of occurrence, if applicable (state parks are not included)"
spark.sql("SELECT PARKS_NM, count(PARKS_NM) FROM df group by PARKS_NM").show(2000)

spark.sql("SELECT PARKS_NM,'text' AS base_type ,'park name' AS semantic_type, CASE WHEN PARKS_NM IN ('',' ') THEN 'null' WHEN PARKS_NM NOT LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT PARKS_NM,'text' AS base_type ,'park name' AS semantic_type, CASE WHEN PARKS_NM IN ('',' ') THEN 'null' WHEN PARKS_NM NOT LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()

# Col 19 --- HADEVELOPT
# Name of NYCHA housing development of occurrence, if applicable
spark.sql("SELECT HADEVELOPT, count(HADEVELOPT) FROM df group by HADEVELOPT").show(2000)

spark.sql("SELECT HADEVELOPT,'text' AS base_type ,'NYCHA housing' AS semantic_type, CASE WHEN HADEVELOPT IN ('',' ') THEN 'null' WHEN HADEVELOPT not LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT HADEVELOPT,'text' AS base_type ,'park name' AS semantic_type, CASE WHEN HADEVELOPT IN ('',' ') THEN 'null' WHEN HADEVELOPT not LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()

# Col 20 --- X_COORD_CD
# X-coordinate for New York State Plane Coordinate System, Long Island Zone, NAD 83, units feet (FIPS 3104)"
spark.sql("SELECT X_COORD_CD FROM df order by X_COORD_CD limit 50").show(2000)

spark.sql("SELECT X_COORD_CD,'integer' AS base_type ,'X-coordinate NAD 83' AS semantic_type, CASE WHEN X_COORD_CD IN ('',' ') THEN 'null' WHEN X_COORD_CD not LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT X_COORD_CD,'integer' AS base_type ,'X-coordinate NAD 83' AS semantic_type, CASE WHEN X_COORD_CD IN ('',' ') THEN 'null' WHEN X_COORD_CD not LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()

# Col 21 --- Y_COORD_CD
# Y-coordinate for New York State Plane Coordinate System, Long Island Zone, NAD 83, units feet (FIPS 3104)"

spark.sql("SELECT Y_COORD_CD FROM df order by Y_COORD_CD limit 50").show(2000)

spark.sql("SELECT Y_COORD_CD,'integer' AS base_type ,'Y-coordinate NAD 83' AS semantic_type, CASE WHEN Y_COORD_CD IN ('',' ') THEN 'null' WHEN Y_COORD_CD not LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT Y_COORD_CD,'integer' AS base_type ,'Y-coordinate NAD 83' AS semantic_type, CASE WHEN Y_COORD_CD IN ('',' ') THEN 'null' WHEN Y_COORD_CD not LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()

# Col 22 --- Latitude,
# Latitude coordinate for Global Coordinate System, WGS 1984, decimal degrees (EPSG 4326) "
spark.sql("SELECT Latitude FROM df limit 50").show(2000)

spark.sql("SELECT Latitude,'float' AS base_type ,'Latitude' AS semantic_type, CASE WHEN Latitude IN ('',' ') THEN 'null' WHEN Latitude < 40.917577 AND Latitude > 40.477399 THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT Latitude,'float' AS base_type ,'Latitude' AS semantic_type, CASE WHEN Latitude IN ('',' ') THEN 'null' WHEN Latitude < 40.917577 AND Latitude > 40.477399 THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()

# Col 23 --- Longitude
# Longitude coordinate for Global Coordinate System, WGS 1984, decimal degrees (EPSG 4326)"

spark.sql("SELECT Longitude,'float' AS base_type ,'Longitude' AS semantic_type, CASE WHEN Longitude IN ('',' ') THEN 'null' WHEN Longitude < -73.700009 AND Longitude > -74.25909  THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()
spark.sql("SELECT is_valid, count(is_valid) from (SELECT Longitude,'float' AS base_type ,'Longitude' AS semantic_type, CASE WHEN Longitude IN ('',' ') THEN 'null' WHEN Longitude < -73.700009 AND Longitude > -74.25909  THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()




