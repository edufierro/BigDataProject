# BigDataProject
Big data project - ak6201; eff254; rds491

Preliminary settings:
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

# Steps for running the BaseballDataDescriptivesGenerator.py script:
  - After executing the shell commands, run the following command to execute this python script:
    ' spark2-submit BaseballAnalysis.py >> BaseballAnalysis.txt'
    
  - This code prints it's result, and are transmitted via i/o to BaseballAnalysis.txt. 
  - The file BaseballAnalysis.txt will be created on local account when it's run on DUMBO. 