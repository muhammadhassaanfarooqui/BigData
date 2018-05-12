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


weather_df = loadData('Project/weather-2011-2017.csv', 'weather_data')
weather_df = weather_df.toDF(*new_column_name_list)
weather_df = fixDateColumn(weather_df, 'CMPLNT_FR_DT', 'weather_data')
generateNullValuesSummery(weather_df,'weather_data')

#Queries
# July 2016 Avergare Temperature Daily
data = spark.sql("select g.Date, round(AVG(g.Temp),2) as average_temp from (select Temp,Date from weather_data where Temp<>'999.9' and YEAR(Date)==2016 and MONTH(Date)==7) g group by g.Date order by g.Date")
data.write.csv('Project/daily_average_temp_july_2016.csv')

# Jan 2015 Avergare Temperature Daily
data = spark.sql("select g.Date, round(AVG(g.Temp),2) as average_temp from (select Temp,Date from weather_data where Temp<>'999.9' and YEAR(Date)==2015 and MONTH(Date)==1) g group by g.Date order by g.Date")
data.write.csv('Project/daily_average_temp_july_2016.csv')




