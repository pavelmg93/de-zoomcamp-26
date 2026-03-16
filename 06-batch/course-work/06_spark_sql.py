#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os

# 1. Define the path to our good Java
java21_path = "/usr/lib/jvm/java-21-openjdk-amd64"

# 2. Set JAVA_HOME
os.environ["JAVA_HOME"] = java21_path

# 3. The Magic: Shove Java 21 to the absolute front of the PATH
os.environ["PATH"] = f"{java21_path}/bin:" + os.environ.get("PATH", "")


# In[2]:


import pyspark
from pyspark.sql import SparkSession


# In[3]:


spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()


# In[4]:


df_green = spark.read \
    .option("recursiveFileLookup", "true") \
    .parquet('data/pq/green')


# In[5]:


df_green.show()


# In[6]:


df_green.count()


# In[7]:


df_yellow = spark.read \
    .option("recursiveFileLookup", "true") \
    .parquet('data/pq/yellow')


# In[8]:


df_yellow.show()


# In[9]:


df_yellow.count()


# In[10]:


df_green.columns


# In[11]:


df_yellow.columns


# In[12]:


set(df_green.columns) & set(df_yellow.columns)


# In[13]:


df_green = df_green \
   .withColumnRenamed('lpep_pickup_datetime', "pickup_datetime") \
   .withColumnRenamed('lpep_dropoff_datetime', "dropoff_datetime")


# In[14]:


df_yellow = df_yellow \
   .withColumnRenamed('tpep_pickup_datetime', "pickup_datetime") \
   .withColumnRenamed('tpep_dropoff_datetime', "dropoff_datetime")


# In[15]:


set(df_green.columns) & set(df_yellow.columns)


# In[16]:


common_columns = []

yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_columns.append(col)


# In[17]:


common_columns


# In[18]:


from pyspark.sql import functions as F


# In[19]:


df_green_sel = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))


# In[20]:


df_yellow_sel = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))


# In[21]:


# Combine (Union) Green and Yellow into Trips DataFrame

df_trips_data = df_green_sel.unionAll(df_yellow_sel)


# In[22]:


df_trips_data.groupBy('service_type').count().show()


# In[23]:


df_trips_data.createOrReplaceTempView('trips_data')


# In[24]:


spark.sql("""
SELECT
    service_type,
    COUNT(1)
FROM
    trips_data
GROUP BY
    service_type
""") \
    .show()


# In[25]:


df_result = spark.sql("""
SELECT 
    -- Revenue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")


# In[26]:


df_result.show()


# In[ ]:


# spark.conf.set("spark.sql.adaptive.enabled", "false")


# In[ ]:


df_result.write.parquet('data/report/revenue', mode = "overwrite")


# In[ ]:


# df_result.coalesce(1).write.parquet('data/report/revenue')


# In[ ]:




