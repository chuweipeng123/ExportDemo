# Databricks notebook source
import pandas as pd
import numpy as np
import pyspark

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .getOrCreate()
df = spark.read.parquet('dbfs:/mnt/mx_v12_mount/wayfinder-procdna-s3-wayfinder-data/Exelixis_Kythera_Analysis/tc_patients')

# COMMAND ----------

df = df.drop("diagnosis_array") 

# COMMAND ----------

df.toPandas()

# COMMAND ----------



# COMMAND ----------

df_pandas.show()

# COMMAND ----------

