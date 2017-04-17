# -*- coding: utf-8 -*-

import numpy as np
from operator import add
from csv import reader
from pyspark import SparkContext

# Data exploration using Pyspark
# PART II
# Generate data for further exploration and visualization. 

def data_import_reader(file_path, filename="NYPD_crime.csv", rm_header=True): 
    
    ''' Import original data  - With reader function from csv to iterate 
        Over lines'''
    ''' RETURNS DATA WITH NO HEADER BY DEFAULT'''
    
    sc = SparkContext()
    data_file = sc.textFile(file_path + filename)
    lines = data_file.mapPartitions(lambda x: reader(x))
    
    if rm_header==True: 
        header = header = lines.first() 
        lines = lines.filter(lambda x: x != header)
    
    return lines
    
def collapse_year(lines, do_return=False): 
    
    ''' Takes lines WITH NO HEADER from data_import_reader(), returns counts by year 
        When do_return = False, send file to hfs to plot outside 
            otherwise, it returns object '''
    ''' CAREFULL: WILL RETURN FileAlreadyExistsException IF OBJECT IS ALREADY DEFINED ''' 
     
    years = lines.map(lambda x: (str(x[1]).split("/")[2], 1) if len(str(x[1]).split("/")) > 1 else ('NA', 1))
    years = years.reduceByKey(add)
    
    if do_return==True: 
        return years
    else: 
        years.saveAsTextFile("YearCounts.out")
        
def collapse_hour_minute(lines, do_return=False):
    
    ''' Takes lines WITH NO HEADER from data_import_reader(), returns counts by hour'''
    ''' CAREFULL: WILL RETURN FileAlreadyExistsException IF OBJECT IS ALREADY DEFINED ''' 

    time = lines.map(lambda x: ((str(x[2]).split(":")[0], str(x[2]).split(":")[1]), 1) if len(str(x[2]).split(":")) > 2 else (('NA', 'NA'), 1))
    time = time.reduceByKey(add)
    
    if do_return==True: 
        return time
    else: 
        time.saveAsTextFile("HourMinutesCounts.out") 

def offense_counter_by_year(lines, do_return=False): 
    
    ''' Takes lines WITH NO HEADER from data_import_reader(), 
        returns offenses by year''' 
    ''' CAREFULL: WILL RETURN FileAlreadyExistsException IF OBJECT IS ALREADY DEFINED ''' 

    offenses = lines.map(lambda x: ((x[7], str(x[1]).split("/")[2]), 1) if len(str(x[1]).split("/")) > 1 else (('NA', 'NA'), 1))
    offenses = offenses.reduceByKey(add)
    
    if do_return==True: 
        return offenses
    else: 
        offenses.saveAsTextFile("OffensesCountsByYear.out")     

def year_differences(lines, do_return=False): 
    
    ''' Import original data  - With reader function from csv to iterate 
        returns offenses by year of occurence and registration'''
    ''' CAREFULL: WILL RETURN FileAlreadyExistsException IF OBJECT IS ALREADY DEFINED ''' 
    
    year_difference =  lines.map(lambda x: ((str(x[1]).split("/")[2], str(x[5]).split("/")[2]), 1) if len(str(x[1]).split("/")) > 1 else (('NA', 'NA'), 1))
    year_difference = year_difference.reduceByKey(add)
    
    if do_return==True: 
        return year_difference
    else: 
        year_difference.saveAsTextFile("YearDifferences.out")     
    
if __name__ == "__main__":
    ''' Assume data is in user's hfs '''        
    data_all = data_import_reader("") 
    
    ''' Assume no files already exists: '''
    collapse_year(data_all)
    collapse_hour_minute(data_all)
    offense_counter_by_year(data_all)
    year_differences(data_all)
         