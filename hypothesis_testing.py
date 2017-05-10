import numpy as np
from math import sqrt
from operator import add
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.mllib.stat import Statistics
from csv import reader

# Data exploration using Pyspark
# PART I
# Validity per column & integrity. 

csv_load = sc.textFile('poverty_data_borough.csv')
demographic_csv = csv_load.mapPartitions(lambda x: reader(x))

demographic_csv.count()
# >>> 56

# The count should be 55 (11 years data for 5 boroughs). The extra count is for the column names


# get the column names
col_names = demographic_csv.take(1)[0]
col_names

#'Year', 'Borough', 'Total; Estimate; Population for whom poverty status is determined',
#'Below poverty level; Estimate; Population for whom poverty status is determined',
#'Percent below poverty level; Estimate; Population for whom poverty status is determined',
#'Below poverty level; Estimate; Population for whom poverty status is determined - AGE - Under 18 years',
#'Percent below poverty level; Estimate; Population for whom poverty status is determined - AGE - Under 18 years',
#'Below poverty level; Estimate; Population for whom poverty status is determined - AGE - 18 to 64 years',
#'Percent below poverty level; Estimate; Population for whom poverty status is determined - AGE - 18 to 64 years',
#'Below poverty level; Estimate; Population for whom poverty status is determined - SEX - Male',
#'Percent below poverty level; Estimate; Population for whom poverty status is determined - SEX - Male',
#'Below poverty level; Estimate; Population for whom poverty status is determined - SEX - Female',
#'Percent below poverty level; Estimate; Population for whom poverty status is determined - SEX - Female',
#'Below poverty level; Estimate; Population for whom poverty status is determined - RACE AND HISPANIC OR LATINO ORIGIN - One race - Black or African American',
#'Percent below poverty level; Estimate; Population for whom poverty status is determined - RACE AND HISPANIC OR LATINO ORIGIN - One race - Black or African American',
#'Total; Estimate; Population for whom poverty status is determined - EDUCATIONAL ATTAINMENT - Population 25 years and over - Less than high school graduate',
#'Total; Estimate; Population for whom poverty status is determined - EDUCATIONAL ATTAINMENT - Population 25 years and over - High school graduate (includes equivalency)',
#'Total; Estimate; Population for whom poverty status is determined - EDUCATIONAL ATTAINMENT - Population 25 years and over - Some college, associate\'s degree',
#'Crime count', 'Percent; Estimate; Population for whom poverty status is determined - EDUCATIONAL ATTAINMENT - Population 25 years and over - population percent with only high school'


#Modify the col names since it is too big
cols_to_use = ['Index', 'Year', 'Borough', 'Total_population', 'Below_poverty_level', 
				'Percent_Below_poverty_level', 'Below_poverty_level_under_age_18', 'Percent_Below_poverty_level_under_age_18', 
				'Below_poverty_level_age_18_to_64', 'Percent_Below_poverty_level_age_18_to_64', 'Below_poverty_level_male', 
				'Percent_Below_poverty_level_male', 'Below_poverty_level_female', 'Percent_Below_poverty_level_female', 'Below_poverty_level_Black', 
				'Percent_Below_poverty_level_Black', 'Education_attainment_less_than_high_school', 'Education_attainment_High_school_graduate', 
				'Education_attainment_Some_college_associate\'s_degree', 'crime count', 'percent with less education']

##########


# Create Spark Dataframe

demographic_csv = demographic_csv.filter(lambda x: x != col_names)

dataframe_obj = spark.createDataFrame(demographic_csv, tuple(cols_to_use) )


dataframe_obj.createOrReplaceTempView("df")


##### 1. DATA QUALITY CHECK

### COLUMN 1 - YEAR:

# All the distinct values should be between 2005 and 2015. Hence it should be 11

spark.sql("SELECT DISTINCT Year FROM df").show()

spark.sql("SELECT COUNT (DISTINCT Year) FROM df").show()




### COLUMN 2 - Borough

# there should be 5 distinct values

spark.sql("SELECT DISTINCT Borough FROM df").show()


spark.sql("SELECT COUNT (DISTINCT Borough) FROM df").show()




### COLUMN 3 - Total population

# check count - should be 55

spark.sql("SELECT COUNT (Total_population) FROM df").show()



# Check if it is integer or not

spark.sql("SELECT Total_population ,'integer' AS base_type ,'total population' AS semantic_type, CASE WHEN Total_population = '' THEN 'null' WHEN Total_population not LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()


spark.sql("SELECT is_valid, count(is_valid) from (SELECT Total_population ,'integer' AS base_type ,'total population' AS semantic_type, CASE WHEN Total_population = '' THEN 'null' WHEN Total_population not LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()





### COLUMN 4 - Below poverty level


# Check count

spark.sql("SELECT COUNT (Below_poverty_level) FROM df").show()


# Check it is integer or not

spark.sql("SELECT Below_poverty_level ,'integer' AS base_type ,'Below_poverty_level' AS semantic_type, CASE WHEN Below_poverty_level = '' THEN 'null' WHEN Below_poverty_level not LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()



# Check for invalid values

spark.sql("SELECT is_valid, count(is_valid) from (SELECT Below_poverty_level ,'integer' AS base_type ,'Below_poverty_level' AS semantic_type, CASE WHEN Below_poverty_level = '' THEN 'null' WHEN Below_poverty_level not LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()



### COLUMN 5 - Percent below poverty level

# Check count

spark.sql("SELECT COUNT (Percent_Below_poverty_level) FROM df").show()


# check the min and max. The min and max should be between 0 and 100 (because it is percentage calculation)


spark.sql("SELECT MIN(int(Percent_Below_poverty_level)) FROM df").show()



spark.sql("SELECT MAX(int(Percent_Below_poverty_level)) FROM df").show()



# Check it is integer or not

spark.sql("SELECT Percent_Below_poverty_level ,'integer' AS base_type ,'Percent Below poverty level' AS semantic_type, CASE WHEN Percent_Below_poverty_level = '' THEN 'null' WHEN Percent_Below_poverty_level not LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()




# Check for invalid values

spark.sql("SELECT is_valid, count(is_valid) from (SELECT Percent_Below_poverty_level ,'integer' AS base_type ,'Percent_Below_poverty_level' AS semantic_type, CASE WHEN Percent_Below_poverty_level = '' THEN 'null' WHEN Percent_Below_poverty_level not LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()





### COLUMN 5 - Percent Below poverty level Black

# Check count

spark.sql("SELECT COUNT (Percent_Below_poverty_level_Black) FROM df").show()




# check the min and max. The min and max should be between 0 and 100 (because it is percentage calculation)

spark.sql("SELECT MAX(int(Percent_Below_poverty_level_Black)) FROM df").show()



spark.sql("SELECT MIN(int(Percent_Below_poverty_level_Black)) FROM df").show()



# Check it is integer or not

spark.sql("SELECT Percent_Below_poverty_level_Black ,'integer' AS base_type ,'Percent_Below_poverty_level_Black' AS semantic_type, CASE WHEN Percent_Below_poverty_level_Black = '' THEN 'null' WHEN Percent_Below_poverty_level_Black not LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()



# Check for invalid values

spark.sql("SELECT is_valid, count(is_valid) from (SELECT Percent_Below_poverty_level_Black ,'integer' AS base_type ,'Percent_Below_poverty_level_Black' AS semantic_type, CASE WHEN Percent_Below_poverty_level_Black = '' THEN 'null' WHEN Percent_Below_poverty_level_Black not LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()



### COLUMN 6 - Percent with education less than high school level 


# Check count

spark.sql("SELECT COUNT (Percent_Below_poverty_level_Black) FROM df").show()



# check the min and max. The min and max should be between 0 and 100 (because it is percentage calculation)

spark.sql("SELECT MIN(int(percent_with_education_below_high_school)) FROM df").show()

spark.sql("SELECT MAX(int(percent_with_education_below_high_school) FROM df").show()

# Check it is integer or not

spark.sql("SELECT percent_with_education_below_high_school ,'integer' AS base_type ,'percent_with_education_below_high_school' AS semantic_type, CASE WHEN percent_with_education_below_high_school = '' THEN 'null' WHEN percent_with_education_below_high_school not LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df").show()

# Check for invalid values

spark.sql("SELECT is_valid, count(is_valid) from (SELECT percent_with_education_below_high_school ,'integer' AS base_type ,'percent_with_education_below_high_school' AS semantic_type, CASE WHEN percent_with_education_below_high_school = '' THEN 'null' WHEN percent_with_education_below_high_school not LIKE '%[^0-9]%' THEN 'valid' ELSE 'invalid' END AS is_valid from df) group by is_valid").show()




## PART II

# Finding correlation

v1 = demographic_csv.map(lambda x: x[3]) # vector for total population
v2 = demographic_csv.map(lambda x: x[4]) # vector for population under poverty level
v3 = demographic_csv.map(lambda x: x[14]) # vector for black population
v4 = demographic_csv.map(lambda x: x[20]) # vector for under-educated population
v5 = demographic_csv.map(lambda x: x[19]) # Total crime count


# Correlation between total population and crime count

print("Correlation is: " + str(Statistics.corr(v1, v5, method="pearson")))
# Correlation is: 0.6767118210127591


# Correlation between population under poverty level and crime count

print("Correlation is: " + str(Statistics.corr(v2, v5, method="pearson")))
# Correlation is: 0.7305919332530579



# Correlation between black population and crime count

print("Correlation is: " + str(Statistics.corr(v3, v5, method="pearson")))
# Correlation is: 0.6712880230552578



# Correlation between under-educated population and crime count

print("Correlation is: " + str(Statistics.corr(v4, v5, method="pearson")))
# Correlation is: 0.6321731661199721


