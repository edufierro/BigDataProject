# -*- coding: utf-8 -*-

from __future__ import print_function

import numpy as np
from operator import add
from csv import reader
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint 
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SQLContext

# Data to analyze Crime data near 
#   baseball stadiums during game-day. 

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
    
def complete_dates(): 
    
    ''' Generates RDD of complete days between 2009 and 2016
        Returns: @datelist: RDD object with string list of all days between 
                 January 1 2009 and January 1 2016'''
        
    date_list = []
    for year in range(2009, 2017):
        for month in range(1, 13):
            if month == 2: 
                for day in range(1, 29): 
                    date_list.append(str(month).zfill(2) + "/" + str(day).zfill(2) + "/" + str(year))
            elif (month in (1, 3, 5, 7, 8, 10, 12)):
                for day in range(1, 32): 
                    date_list.append(str(month).zfill(2) + "/" + str(day).zfill(2) + "/" + str(year))
            else: 
                for day in range(1, 31):
                    date_list.append(str(month).zfill(2) + "/" + str(day).zfill(2) + "/" + str(year))
            
    # Append 29 of february: 
    date_list.append(str(2).zfill(2) + "/" + str(29).zfill(2) + "/" + str(2012))
    date_list.append(str(2).zfill(2) + "/" + str(29).zfill(2) + "/" + str(2016))
    
    return sc.parallelize(date_list)          
    
    
def prep_regression(mydata, crime='PETIT LARCENY', stadium='Yankees'):  
    
    ''' Takes output from game_day()
        Generates data by date for specific stadium and crime.            
        Depending on distance from game_day().              
        Takes: @ mydata: object generated with ballpark_subsetter()
               @ crime: Specific crime or 'all' 
               @ stadium: 'Yankees' or 'Mets'. 
        Returns: @ mydata2: Data to feed MLlib model type LabeledPoint(). 
                 Label: # crimes in a day 
                 1 feature: 1 = Game Day / 0 = Not Game Day. 
    '''       
    
    # Filter according to crime:     
    if (crime!='all'):
        mydata = mydata.filter(lambda x: x[1][0]==crime)
        
    mydata = mydata.filter(lambda x: x[1][3]==stadium)
    mydata = mydata.map(lambda x: ((x[0], x[1][4]), 1)).reduceByKey(add)
    mydata = mydata.map(lambda x: (x[0][0], (x[0][1], x[1])))
    
    # Have to complete data by day with zeros
    all_days = complete_dates().map(lambda x: (x, 0)) 
    all_days = all_days.leftOuterJoin(mydata)
    
    # Final prep for data: 
    all_days = all_days.map(lambda x: (x[1][1][1], (x[1][1][0])) if x[1][1] else (0, ('not_gameday')))   
    
    # Format for depreciated LinearRegressionWithSGD. Left for reference. 
    # all_days = all_days.map(lambda x: LabeledPoint(x[0], [1]) if x[1]=='gameday' else LabeledPoint(x[0], [0])) 
    
    # Format for pyspark.ml
    all_days = all_days.map(lambda x: (x[0], Vectors.dense(1)) if x[1]=='gameday' else (x[0], Vectors.dense(0))) 
    all_days2 = sqlContext.createDataFrame(all_days, ["label", "features"])
    
    return all_days2 
    
if __name__ == "__main__":  

    sc = SparkContext()
    sqlContext = SQLContext(sc)    

    # Import data frames #
    data_crime = data_import_reader("")
    data_baseball = data_import_reader("", filename="BaseballData.csv")
    
    # List to loop. 
    distances_list = [250, 500, 1000, 2000]
    crime_list = data_crime.map(lambda x: (x[7],1)).reduceByKey(add).map(lambda x: x[0]).filter(lambda x: x!='').collect()
    crime_list.append('all')
    stadiums_list = ['Yankees', 'Mets']
    
    for x in range(0,len(distances_list)):
        data_subset = ballpark_subsetter(data_crime, distance=distances_list[x])
        data_subset = game_day(data_subset, data_baseball)
        
        for y in range(0, len(crime_list)):
            for z in range(0, len(stadiums_list)):
            
                data_model = prep_regression(data_subset, crime=crime_list[y], stadium=stadiums_list[z])
                lr = LinearRegression(maxIter=1000, regParam=0, elasticNetParam=0, fitIntercept=True, standardization=False)
                
                try:
                    # This fails if the matrix (data_model) is not invertible (not full-rank). 
                    # I.E: The matrix has only ones or zeros of the IV, so it's correlated 
                    # with the intercept = True. 
                    # Then? There were no crimes of the specified type in that radius. 
                    
                    fitted = lr.fit(data_model)
                    summary = fitted.summary
                    coefficient = fitted.coefficients.array[0]
                    intercept = fitted.intercept    
                    variance = summary.explainedVariance 
                
                    try:    
                        # This is an experimental method. 
                        # Sometimes it fails, sometimes it doesn't
                        pvalue = summary.pValues[1] 
                    except: 
                        pvalue = -99999
                        
                except: 
                    coefficient = -99999
                    intercept = -99999
                    variance = -99999
                    pvalue = -99999
                                    
                print("%d, %s, %s, %f, %f, %f" % (distances_list[x], crime_list[y], stadiums_list[z], intercept, coefficient, pvalue))     
                      
    sc.stop()
