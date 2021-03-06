# BigDataProject

Tem Members: 
  - Akash Kadel (ak6201)
  - Raúl Delgado Sánchez (rds491)
  - Eduardo Fierro Farah (eff254)


All pyspark scripts were generated and tested using Spark version 2.0.0.cloudera1, 
Using Python version 3.4.4. The following dependencies are necessary for all the codes
to run correctly: 
  - numpy
  - operator
  - csv
  - pyspark.mllib
  - pyspark.ml (v2.0 or above)
  
All codes were run and tested on DUMBO, NYU's 48-node Hadoop cluster, running Cloudera CDH 5.9.0 (Hadoop 2.6.0 with Yarn).

# Preliminary settings:
  - Download or clone the repository
  - Copy all the python script and shell code into your dumbo account
  - We are using NYPD Crime data downloaded from https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i 
  - Move your NYPD_Crime data to HDFS . Note that the naming of the file in HDFS and the python code should be the same. For convenience, the name we are using is 'NYPD_crime.csv'
  - Move the BaseballData.csv to HDFS. Keep the name of the file intact. 
  - Execute the shell commands  
  
# Steps for running the dataForVisualization.py script:
  - After executing the shell commands, run the following command to execute this python script:
    ' spark-submit dataForVisualization.py '
  - Be sure no files names as the ones listed below are on HDFS.
  - This will execute the code and save 4 files in HDFS. 
    1. HourMinutesCounts.out
    2. OffensesCountsByYear.out
    3. YearCounts.out
    4. YearDifferences.out
    
  - It can be viewed by running the following command
    ' hfs -ls '
  - To download the files, you need to perform, " -get " command for every file (not the  getmerge command).
    
# Steps for running the Integrity.py script:
  - Fire up pyspark2 (as given in the shell command)
  - Run every command in the script
  
# Steps for running the BaseballDataDescriptivesGenerator.py script:
  - After executing the shell commands, run the following command to execute this python script:
    ' spark2-submit BaseballDataDescriptivesGenerator.py '
  - Be sure no files names as the ones listed below are on HDFS.
  - This will execute the code and save 4 files in HDFS. 
    1. subset250.out
    2. subset500.out
    3. subset1000.out
    4. subset2000.out
    
Note: This code may takes over 5 minutes to run. The output described above was added to the folder "Data".

# Steps for running the BaseballAnalysis.py script:
  - After executing the shell commands, run the following command to execute this python script:
    ' spark2-submit BaseballAnalysis.py >> BaseballAnalysis.txt'
    
  - This code prints it's result, and are transmitted via i/o to BaseballAnalysis.txt. 
  - When run on DUMBO, the file BaseballAnalysis.txt will be created on local account. 
  
Note: This code takes over 20 minutes to run. The output described above was added to the folder "Data".

#  R codes
Any of the R codes take as input all the outputs generated by the previous codes, or non input at all. 
All the codes were run and tested on R version 3.3.3 (2017-03-06) -- "Another Canoe". 
To run all the codes correctly, please install the following libraries: 
  - stringr
  - ggmap
  - ggplot2
  - rgdal
  - rgeos
  - maptools
  - plyr
  - XML
  - httr  
  - doBy
  
  
# Steps for running the hypothesis_testing.py script
  
Note: This module is to verify the hypothesis on demographics data. The hypothesis is well documented in the report. Also, the data file being used is poverty_data_borough.csv. This data was consioldated using the 20 data files collected from the American factfinder site. The steps required to execute the hypothesis_testing.py script

- Execute the shell commands from the preliminary settings. 
- Make sure that the file path is same as the data file.
- Fire up pyspark2.
- Run one by one the code in the hypothesis_testing.py script.
 
 
# Steps to produce the visualization using ipython notebook

- The code to produce the visualizations for both crime data and poverty data is there in the repo.
- Open the script (python notebook) and you can choose the option restart-run all to produce the graphs. Make sure that the file path is same where the 20 csv files are located.
- The ipython notebook is "census_analysis.ipynb"

