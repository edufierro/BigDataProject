# -*- coding: utf-8 -*-

import numpy as np
from operator import add
from csv import reader
from pyspark import SparkContext

# Data to analyze Crime data near 
#   baseball stadiums during game-day. 

# Generates subset250.out; subset500.out
#           subset1000.out and subset2000.out

# Get them fro hfs with -getmerge command. 


def data_import_reader(file_path, filename="NYPD_crime.csv", rm_header=True): 
    
    ''' Import original data  - With reader function from csv to iterate 
        Over lines'''
    ''' RETURNS DATA WITH NO HEADER BY DEFAULT'''
    
    data_file = sc.textFile(file_path + filename)
    lines = data_file.mapPartitions(lambda x: reader(x))
    
    if rm_header==True: 
        header = lines.first() 
        lines = lines.filter(lambda x: x != header)
    
    return lines
    
def ballpark_subsetter(mydata, distance=500):  
    
    ''' Subsets a data frame to Bounding Box (by Lat - Lon) of:
             - Yankee Stadium (II and III)              
             - Citi Field (METS)            
        Depending on distance.              
        Takes: @ mydata: object with no header from data_import_reader()
                         CAREFULL! HAS LAT-LON on Third and Second to last columns!! 
               @ distance: Desired distance from BBox. Takes 
                           200, 500, 1000, and 2000
                           (Distance in meters). 
        Returns: @ mydata: Data with geographic subset 
    '''   
        
    # First, drop data with no Lat - Lon: 
    mydata = mydata.filter(lambda x: x[-3]!='')
    mydata = mydata.filter(lambda x: x[-2]!='') 
        
    # Define coords list of bbox (see R code where they are computed):     
    if distance == 250:
        coords = [40.82744, 40.83172, -73.92913, -73.92349, 40.75493, 40.75922, -73.84867, -73.84303]
    elif distance == 500:
        coords = [40.82530, 40.83387, -73.93195, -73.92067, 40.75279, 40.76136, -73.85149, -73.84022]
    elif distance == 1000: 
        coords = [40.82102, 40.83815, -73.93759, -73.91503, 40.74851, 40.76565, -73.85712, -73.83458]
    elif distance == 2000: 
        coords = [40.81245, 40.84671, -73.94887, -73.90375, 40.73993, 40.77422, -73.86839, -73.82331]
    else: 
        raise ValueError('Distance not valid')
        
    mydata = mydata.filter(lambda x: (float(x[-3]) > coords[0] and float(x[-3])< coords[1] and float(x[-2]) > coords[2] and float(x[-2])< coords[3]) or (float(x[-3]) > coords[4] and float(x[-3])< coords[5] and float(x[-2]) > coords[6] and float(x[-2])< coords[7]) )     
    
    return mydata
    
    
def game_day(mydata, baseball):  
    
    ''' leftOuterJoin by date between 'mydata' and 'baseball'.     
        Takes: @ mydata: object with no header from data_import_reader() or ballpark_subsetter()
                         CARFEULL: Asumes certain order to where date vars. are located. 
               @ baseball: "BaseballData.csv" imported with data_import_reader()        
        Returns: @ mydata2: (DATE, (CRIME, LAT, LON, TEMAM, IS_GAMEDAY))
    '''   
    
    # Filter crime data to valid date & year >= 2009
    mydata = mydata.filter(lambda x: len(str(x[1])) > 3)
    mydata = mydata.filter(lambda x: int(str(x[1]).split("/")[2]) >= 2009)
        
    # Filter to has crime spec. 
    mydata = mydata.filter(lambda x: len(x[7]) >= 1)    
        
    # Map using date as key for both data frames: 
    mydata2 = mydata.map(lambda x: (x[1], (x[7], x[-3], x[-2])))
    baseball_map = baseball.map(lambda x: (x[-1], x[-3]))
        
    mydata2 = mydata2.leftOuterJoin(baseball_map)  
    mydata2 = mydata2.map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][0][2], x[1][1], 'gameday')) if x[1][1] else ((x[0], (x[1][0][0], x[1][0][1], x[1][0][2], 'Yankees', 'not_gameday')) if float(x[1][0][1])>40.80  else (x[0], (x[1][0][0], x[1][0][1], x[1][0][2], 'Mets', 'not_gameday'))))   
        
    return mydata2
    
if __name__ == "__main__":  

    sc = SparkContext()

    # Import data frames #
    data_crime = data_import_reader("")
    data_baseball = data_import_reader("", filename="BaseballData.csv")
    
    # Export data for plots: 
    subset250 = ballpark_subsetter(data_crime, distance=250)
    subset500 = ballpark_subsetter(data_crime, distance=500)
    subset1000 = ballpark_subsetter(data_crime, distance=1000)
    subset2000 = ballpark_subsetter(data_crime, distance=2000)   
     
    subset250 = game_day(subset250, data_baseball)
    subset500 = game_day(subset500, data_baseball)
    subset1000 = game_day(subset1000, data_baseball)
    subset2000 = game_day(subset2000, data_baseball)
    
    subset250.saveAsTextFile("subset250.out")
    subset500.saveAsTextFile("subset500.out")
    subset1000.saveAsTextFile("subset1000.out")
    subset2000.saveAsTextFile("subset2000.out")

    sc.stop()
