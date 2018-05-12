
# coding: utf-8

# In[ ]:


import sys
import csv
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import to_timestamp
import numpy as np
from pyspark.sql.functions import col, create_map, lit
from itertools import chain
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


# In[ ]:


bor_dict = { '1': 'BRONX' ,'2':'BROOKLYN','3':'MANHATTAN', '4':'QUEENS' , '5':'STATEN ISLAND'}
mapping_expr = create_map([lit(x) for x in chain(*bor_dict.items())])


# In[ ]:


years = ["2011","2012","2013","2014","2015","2016"]

for year in years:
    filename = "Project/Poverty/"+year+".csv"
    viewName = "poverty_"+year
    df = loadData(filename, viewName)
    column_list = [ 'Povunit_Rel', 'Povunit_ID', 'HousingStatus', 'EducAttain', 'WorkExpIndiv', 'WorkersEquiv', 'Off_Pov_Stat', 'Boro', 'AgeCateg']
    data = df.select([c for c in df.columns if c in column_list])
    data = data.withColumn("Boro", mapping_expr.getItem(col("Boro")))
    data.repartition(1).write.format('com.databricks.spark.csv').save('Project/Poverty/poverty_'+year+'.csv',header = 'true')


# In[ ]:


for year in years:
    viewName = "poverty"
    df = loadData('Project/Poverty/poverty_'+year+'.csv', viewName)
    df.createOrReplaceTempView(viewName)
    #Queries-
    #1. Brough wise total officially poor count
    data1 = spark.sql("select Boro, count(*)from poverty where Off_Pov_Stat =1 group by Boro")
    #1. Brough wise total count
    data2 = spark.sql("select Boro, count(*)from poverty group by Boro")
    data1.write.csv("Project/Poverty/poverty_poor"+year+".csv")
    data2.write.csv("Project/Poverty/poverty_total"+year+".csv")

    


# In[ ]:


for year in years:
    viewName = "poverty"
    df = loadData('Project/Poverty/poverty_'+year+'.csv', viewName)
    df.createOrReplaceTempView(viewName)
    #Queries
    data = spark.sql("select Boro, count(*)from poverty where EducAttain in (1,2)group by Boro")
    data.write.csv("Project/Poverty/poverty_not_in_college_"+year+".csv")


# In[1]:


years = ["2011","2012","2013","2014","2015","2016"]

for year in years:
    query = "select BORO_NM, count(CMPLNT_NUM) as total from crime_subset_data where YEAR(CMPLNT_FR_DT)=={0} group by BORO_NM order by BORO_NM"
    query = query.format(year)
    data = spark.sql(query)
    data.repartition(1).write.format('com.databricks.spark.csv').save('Project/total_borough_crime_'+year+'.csv',header = 'true')


# In[4]:


#TODO : Pick any category of crime and get monthly counts and get correlation with temp/weather data
# 348 - VEHICLE AND TRAFFIC LAWS
# 107 - BURGLARY 
# 344 - ASSAULT 3 & RELATED OFFENSES
# 110 - GRAND LARCENY OF MOTOR VEHICLE
# 101 - MURDER & NON-NEGL. MANSLAUGHTER


# In[ ]:


# 348 - VEHICLE AND TRAFFIC LAWS
years = ["2011","2012","2013","2014","2015","2016"]
for year in years:
    query = "select BORO_NM, count(CMPLNT_NUM) as total from crime_subset_data where KY_CD = 348 and YEAR(CMPLNT_FR_DT)=={0} group by BORO_NM order by BORO_NM"
    query = query.format(year)
    data = spark.sql(query)
    data.repartition(1).write.format('com.databricks.spark.csv').save('Project/total_borough_vehicle_crime_'+year+'.csv',header = 'true')


# In[ ]:


# 107 - BURGLARY 
years = ["2011","2012","2013","2014","2015","2016"]
for year in years:
    query = "select BORO_NM, count(CMPLNT_NUM) as total from crime_subset_data where KY_CD = 107 and YEAR(CMPLNT_FR_DT)=={0} group by BORO_NM order by BORO_NM"
    query = query.format(year)
    data = spark.sql(query)
    data.repartition(1).write.format('com.databricks.spark.csv').save('Project/total_borough_burgulary_crime_'+year+'.csv',header = 'true')


# In[ ]:


# 344 - ASSAULT 3 & RELATED OFFENSES
years = ["2011","2012","2013","2014","2015","2016"]
for year in years:
    query = "select BORO_NM, count(CMPLNT_NUM) as total from crime_subset_data where KY_CD = 344 and YEAR(CMPLNT_FR_DT)=={0} group by BORO_NM order by BORO_NM"
    query = query.format(year)
    data = spark.sql(query)
    data.repartition(1).write.format('com.databricks.spark.csv').save('Project/total_borough_asssault_crime_'+year+'.csv',header = 'true')


# In[ ]:


# 101 - MURDER & NON-NEGL. MANSLAUGHTER
years = ["2011","2012","2013","2014","2015","2016"]
for year in years:
    query = "select BORO_NM, count(CMPLNT_NUM) as total from crime_subset_data where KY_CD = 101 and YEAR(CMPLNT_FR_DT)=={0} group by BORO_NM order by BORO_NM"
    query = query.format(year)
    data = spark.sql(query)
    data.repartition(1).write.format('com.databricks.spark.csv').save('Project/total_borough_murder_crime_'+year+'.csv',header = 'true')


# In[10]:


years = ["2011","2012","2013","2014","2015","2016"]
for year in years:
    print("hfs -getmerge Project/borough_crime/total_borough_crime_{0}.csv borough_crime/total_borough_crime_{1}.csv".format(year,year))



# In[15]:


years = ["2011","2012","2013","2014","2015","2016"]
for year in years:
    print("hfs -getmerge Project/Poverty/poverty_total{}.csv Poverty/poverty_total{}.csv".format(year,year))

