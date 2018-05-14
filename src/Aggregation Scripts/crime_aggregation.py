#!/usr/bin/env python
import sys
import csv
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import to_timestamp
import numpy as np
spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()


def generateNullValuesSummery(dataframe, tableName):
	filename = tableName + 'NullValuesSummery.txt'
	if os.path.exists(filename):
		append_write = 'a' # append if already exist
	else:
		append_write = 'w' 
	f = open(filename, append_write)
	for col in dataframe.columns:
	    query = "select count(*) - count("+str(col)+") as nullCnt, count("+str(col)+") as goodcnt from " + str(tableName)
	    nullCount = spark.sql(query).collect()
	    f.write("Column " + str(col) + "--> Number of records with Null values: " + str(nullCount[0][0]) + "\t|\t Number of records without Null values: " + str(nullCount[0][1]) + "\n")
	    f.flush()
	f.close()


def fixDateColumn(dataframe, columnName, tableName):
    dataframe = dataframe.withColumn(columnName, to_timestamp(dataframe[columnName], 'MM/dd/yyyy'))
    dataframe = dataframe.withColumn(columnName, dataframe[columnName].cast('date'))
    dataframe.createOrReplaceTempView(tableName)
    return dataframe

def loadData(filename, viewName):
    temp = spark.read.format('csv').options(header='true', inferschema='true').load(filename)
    temp.createOrReplaceTempView(viewName)
    return temp


crime_df = loadData('Project/NYPD_Complaint_Data_Historic.csv', 'crime_data')
crime_df = fixDateColumn(crime_df, 'CMPLNT_FR_DT', 'crime_data')
generateNullValuesSummery(crime_df,'crime_data')

#Queries

#For removing null and Only keep records from 2006
data = spark.sql("select * from crime_data where CMPLNT_FR_DT is not null and YEAR(CMPLNT_FR_DT) >= 2006 ")
data.createOrReplaceTempView("crime_data")

# 1 . daily counts of complaints
data = spark.sql("select CMPLNT_FR_DT, count(CMPLNT_NUM) as total from crime_data group by CMPLNT_FR_DT order by CMPLNT_FR_DT")
# data.select(format_string('%s\t%d',data.CMPLNT_FR_DT, data.total)).write.save( "Project/daily_complaint_count.txt", format="text")
data.write.csv('Project/daily_complaint_count.csv')

# 2. For geting values from 2011-2017
data = spark.sql("select * from crime_data where YEAR(CMPLNT_FR_DT) >= 2011 and  YEAR(CMPLNT_FR_DT) <= 2017")
data.createOrReplaceTempView("crime_subset_data")

data = spark.sql("select CMPLNT_FR_DT, count(CMPLNT_NUM) as total from crime_subset_data group by CMPLNT_FR_DT order by CMPLNT_FR_DT")
# data.select(format_string('%s\t%d',data.CMPLNT_FR_DT, data.total)).write.save( "Project/daily_complaint_count_2011-17.txt", format="text")
data.write.csv('Project/daily_complaint_count_2011-17.csv')

# 3. For MONTHLY Total
data = spark.sql("select MONTH(CMPLNT_FR_DT) as month, YEAR(CMPLNT_FR_DT) as year, count(CMPLNT_NUM) as total from crime_subset_data group by month,year order by year,month")
# data.select(format_string('%s\t%d',data.complaint_month, data.total)).write.save( "Project/monthly_complaint_count.txt", format="text")
data.write.csv('Project/monthly_complaint_count.csv')


#For getting the values of a year -2016
data = spark.sql("select CMPLNT_FR_DT, count(CMPLNT_NUM) as total from crime_subset_data where YEAR(CMPLNT_FR_DT)==2016 group by CMPLNT_FR_DT order by CMPLNT_FR_DT")
data.write.csv('Project/daily_complaint_count_2016.csv')

#For getting the values of a year monthly - 2016
data = spark.sql("select MONTH(CMPLNT_FR_DT) as complaint_month, count(CMPLNT_NUM) as total from crime_subset_data where YEAR(CMPLNT_FR_DT)==2016 group by complaint_month order by complaint_month")
data.write.csv('Project/monthly_complaint_count_2016.csv')


#TODO : Pick any category of crime and get monthly counts and get correlation with temp/weather data
# 348 - VEHICLE AND TRAFFIC LAWS
# 107 - BURGLARY 
# 344 - ASSAULT 3 & RELATED OFFENSES
# 110 - GRAND LARCENY OF MOTOR VEHICLE
# 101 - MURDER & NON-NEGL. MANSLAUGHTER

# FOR 2016
data = spark.sql("select MONTH(CMPLNT_FR_DT) as complaint_month, count(CMPLNT_NUM) as total from crime_subset_data where YEAR(CMPLNT_FR_DT)==2016  and KY_CD = 107 group by complaint_month order by complaint_month")
data.write.csv('Project/BURGLARY_2016_Monthly.csv')

data = spark.sql("select MONTH(CMPLNT_FR_DT) as complaint_month, count(CMPLNT_NUM) as total from crime_subset_data where YEAR(CMPLNT_FR_DT)==2016  and KY_CD = 344 group by complaint_month order by complaint_month")
data.write.csv('Project/ASSAULT_2016_Monthly.csv')

data = spark.sql("select MONTH(CMPLNT_FR_DT) as complaint_month, count(CMPLNT_NUM) as total from crime_subset_data where YEAR(CMPLNT_FR_DT)==2016  and KY_CD = 348 group by complaint_month order by complaint_month")
data.write.csv('Project/VEHICLE_AND_TRAFFIC_LAWS_2016_Monthly.csv')

data = spark.sql("select MONTH(CMPLNT_FR_DT) as complaint_month, count(CMPLNT_NUM) as total from crime_subset_data where YEAR(CMPLNT_FR_DT)==2016  and KY_CD = 110 group by complaint_month order by complaint_month")
data.write.csv('Project/GRAND_LARCENY_MOTOR_VEHICLE_2016_Monthly.csv')

data = spark.sql("select MONTH(CMPLNT_FR_DT) as complaint_month, count(CMPLNT_NUM) as total from crime_subset_data where YEAR(CMPLNT_FR_DT)==2016  and KY_CD = 101 group by complaint_month order by complaint_month")
data.write.csv('Project/MURDER_2016_Monthly.csv')

#FOR ALL YEARS

data = spark.sql("select MONTH(CMPLNT_FR_DT) as month, YEAR(CMPLNT_FR_DT) as year, count(CMPLNT_NUM) as total from crime_subset_data where KY_CD = 107 group by month,year order by year,month")
data.write.csv('Project/BURGLARY_Monthly.csv')

data = spark.sql("select MONTH(CMPLNT_FR_DT) as month, YEAR(CMPLNT_FR_DT) as year, count(CMPLNT_NUM) as total from crime_subset_data where KY_CD = 344 group by month,year order by year,month")
data.write.csv('Project/ASSAULT_Monthly.csv')


data = spark.sql("select MONTH(CMPLNT_FR_DT) as month, YEAR(CMPLNT_FR_DT) as year, count(CMPLNT_NUM) as total from crime_subset_data where KY_CD = 348 group by month,year order by year,month")
data.write.csv('Project/VEHICLE_AND_TRAFFIC_LAWS_Monthly.csv')


data = spark.sql("select MONTH(CMPLNT_FR_DT) as month, YEAR(CMPLNT_FR_DT) as year, count(CMPLNT_NUM) as total from crime_subset_data where KY_CD = 110 group by month,year order by year,month")
data.write.csv('Project/GRAND_LARCENY_MOTOR_VEHICLE_Monthly.csv')


data = spark.sql("select MONTH(CMPLNT_FR_DT) as month, YEAR(CMPLNT_FR_DT) as year, count(CMPLNT_NUM) as total from crime_subset_data where KY_CD = 101 group by month,year order by year,month")
data.write.csv('Project/MURDER_Monthly.csv')

#Ananlayse Jan 2016 GRAND_LARCENY_MOTOR_VEHICLE Crime
data = spark.sql("select CMPLNT_FR_DT , count(CMPLNT_NUM) as total from crime_subset_data where KY_CD = 110 and MONTH(CMPLNT_FR_DT) ==1 and YEAR(CMPLNT_FR_DT) == 2016 group by CMPLNT_FR_DT order by CMPLNT_FR_DT")
data.write.csv('Project/GRAND_LARCENY_MOTOR_VEHICLE_jan_2016.csv')


data = spark.sql("select CMPLNT_FR_DT , count(CMPLNT_NUM) as total from crime_subset_data where KY_CD = 110 and MONTH(CMPLNT_FR_DT) ==2 and YEAR(CMPLNT_FR_DT) == 2016 group by CMPLNT_FR_DT order by CMPLNT_FR_DT")

data.write.csv('Project/GRAND_LARCENY_MOTOR_VEHICLE_feb_2016.csv')
