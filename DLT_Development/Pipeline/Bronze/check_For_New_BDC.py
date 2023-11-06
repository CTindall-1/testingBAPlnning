# Databricks notebook source
import sys, os, json
from datetime import datetime
from pyspark.sql.types import *
import pyspark.sql.functions as func
from schema_config import *

# COMMAND ----------

RUN_FLAG = False
RUN_MISSING_FLAG = False
MISSING_RUN_DATE = ''

# COMMAND ----------

version_schema = StructType([StructField('version_name', StringType(), True),
                             StructField('version_date', DateType(), True)])

# COMMAND ----------

def get_dir_content(ls_path):
  dir_paths = dbutils.fs.ls(ls_path)
  subdir_paths = [get_dir_content(p.path) for p in dir_paths if p.isDir() and p.path != ls_path]
  #flat_subdir_paths = [p for subdir in subdir_paths for p in subdir]
  dict_path = {p.path:p.modificationTime for p in dir_paths}# + flat_subdir_paths
  return dict_path
    

paths = get_dir_content('dbfs:/mnt/data/fcc_nbm_api/data/')
#[print(p) for p in paths]

# COMMAND ----------

def grab_latest_content(file_dict):
    return max(file_dict)

latest_path = grab_latest_content(paths)

# COMMAND ----------

def get_datetime(path):
    return datetime.strptime(path[-7:-1], '%m%d%y')

max_date_obj = get_datetime(latest_path)

# COMMAND ----------

version_lookup = spark.sql("SELECT * FROM ba_planning_tool.version_lookup")

# COMMAND ----------

version_lookup = version_lookup.withColumn('temp_version_name', func.substring(func.col('version_name'), 3, 20))\
                               .withColumn('number', func.regexp_extract(func.col('temp_version_name'), r'([0-9]+)', 1).cast(IntegerType()))

# COMMAND ----------

def create_new_maxnum():
    number = version_lookup.withColumn('temp', func.lit('temp')).groupBy('temp').max().select('max(number)').collect()[0][0]
    number+=1
    return number

# COMMAND ----------

max_number = create_new_maxnum()

# COMMAND ----------

new_version = spark.createDataFrame([[f'v2{max_number}BETA', max_date_obj]], schema=version_schema)

# COMMAND ----------

version_lookup = version_lookup.select('version_name', 'version_date')

# COMMAND ----------

version_lookup = version_lookup.union(new_version)

# COMMAND ----------

version_lookup = version_lookup.withColumn('temp_version_name', func.substring(func.col('version_name'), 3, 20))\
                               .withColumn('number', func.regexp_extract(func.col('temp_version_name'), r'([0-9]+)', 1).cast(IntegerType()))\
                               .orderBy('number', ascending=True).coalesce(1)\
                               .dropDuplicates(subset=['version_date'])\
                               .select('version_name', 'version_date')

# COMMAND ----------

version_lookup.write.format('delta').mode('overwrite').saveAsTable("ba_planning_tool.version_lookup")

# COMMAND ----------

auth_bdc = spark.sql("SELECT DISTINCT effective_date FROM ba_planning_tool.authoritative_bdc")

# COMMAND ----------

auth_dates = auth_bdc.select('effective_date').collect()

# COMMAND ----------

auth_dates = [x[0] for x in auth_dates]

# COMMAND ----------

if max_date_obj.date() in auth_dates:
    RUN_FLAG = False
    raise TypeError(f"Pipeline Killed: Failed Since {max_date_obj.strftime('%m-%d-%y')} is Not a New BDC")
elif max_date_obj.date() > max(auth_dates):
    RUN_FLAG = True
