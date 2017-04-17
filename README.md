# BigDataProject
Big data project - ak6201; eff254; rds491

Preliminary settings:
  - Download or clone the repository
  - Copy all the python script and shell code into your dumbo account
  - Move the NYPD_Crime data to HDFS . Note that the naming of the file in HDFS and the python code should be the same. For convenience, the name we are using is 'NYPD_crime.csv'
  - Execute the shell commands
  
  
Steps for running the dataForVisualization.py script:
  - After executing the shell commands, run the following command to execute this python script:
    ' spark-submit dataForVisualization.py '
  - This will execute the code and save 4 files in HDFS. 
    1. HourMinutesCounts.out
    2. OffensesCountsByYear.out
    3. YearCounts.out
    4. YearDifferences.out
    
  - It can be viewed by running the following command
    ' hfs -ls '
    
Steps for running the Integrity.py script:
  - Fire up pyspark2 (as given in the shell command)
  - Run every command in the script
