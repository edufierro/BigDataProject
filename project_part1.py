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

#Col 1 ------------- 

# Check: all different
spark.sql("SELECT COUNT (DISTINCT CMPLNT_NUM) FROM df").show()
spark.sql("SELECT COUNT (CMPLNT_NUM) FROM df").show()

#Check max and min values
spark.sql("SELECT MAX(int(CMPLNT_NUM)) FROM df").show()
spark.sql("SELECT MIN(int(CMPLNT_NUM)) FROM df").show()


SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'df' AND COLUMN_NAME = 'CMPLNT_NUM'

SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'df'


spark.sql("SELECT CMPLNT_NUM ,'integer' AS base_type ,'commplaint number' AS semantic_type, CASE WHEN CMPLNT_NUM >= 100000228 AND CMPLNT_NUM <= 999999904 THEN 'valid' ELSE 'invalid' END AS is_valid FROM df").show()

# Col 2 ------------


spark.sql("SELECT DATE(CMPLNT_FR_DT) from df").show()











