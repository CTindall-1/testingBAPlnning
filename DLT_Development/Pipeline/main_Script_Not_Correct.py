# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as func
import numpy as np
from schema_config import *
from pyspark.sql.window import Window

# COMMAND ----------

# spark.conf.set("spark.sql.state_name", "joe")
spark.conf.set("spark.sql.bdc_version", "v27BETA")

# COMMAND ----------

version = spark.conf.get("spark.sql.bdc_version")
version

# COMMAND ----------

bdc_schema = StructType([StructField('frn', StringType(), True),
                            StructField('provider_id', StringType(), True),
                            StructField('brand_name', StringType(), True),
                            StructField('location_id', StringType(), True),
                            StructField('technology', StringType(), True),
                            StructField('max_advertised_download_speed', DoubleType(), True),
                            StructField('max_advertised_upload_speed', DoubleType(), True),
                            StructField('low_latency', IntegerType(), True),
                            StructField('business_residential_code', StringType(), True),
                            StructField('state_usps', StringType(), True),
                            StructField('block_geoid', StringType(), True),
                            StructField('h3_res8_id', StringType(), True)])

# COMMAND ----------

#version = dbutils.widgets.get('version')
#version = 'v29BETA'

# COMMAND ----------

EOW_DATE = "12-31-9999"
DATE_FORMAT = "MM-dd-yyyy"

KEYS = ['location_id', 'frn', 'brand_name', 'technology',
              'max_advertised_download_speed', 'max_advertised_upload_speed', 
              'business_residential_code', 'speed_tier']

type2_cols = ['frn', 'brand_name', 'technology',
              'max_advertised_download_speed', 'max_advertised_upload_speed', 
              'business_residential_code', 'speed_tier']

scd2_cols = ["effective_bdc","expiration_bdc","current_flag"]

# COMMAND ----------

def column_renamer(df, suffix, append):
    """
    input:
        df: dataframe
        suffix: suffix to be appended to column name
        append: boolean value 
                if true append suffix else remove suffix
    
    output:
        df: df with renamed column
    """
    if append:

        new_column_names = list(map(lambda x: x if x in ['state_geoid', 'block_geoid'] else x+suffix, df.columns))
    else:
        new_column_names = list(map(lambda x: x.replace(suffix,""), df.columns))
    return df.toDF(*new_column_names)

def get_hash(df, keys_list):
    """
    input:
        df: dataframe
        key_list: list of columns to be hashed    
    output:
        df: df with hashed column
    """
    columns = [col(column) for column in keys_list]
    if columns:
        return df.withColumn("hash_md5", md5(concat_ws("", *columns)))
    else:
        return df.withColumn("hash_md5", lit('1'))

# COMMAND ----------

# MAGIC %md ###All Manip Functions

# COMMAND ----------

import dlt

# COMMAND ----------

@dlt.create_table(
    name=f'bdc_raw',
    table_properties = {"quality": "bronze"},
    temporary=True
)
@dlt.expect_or_fail("valid_location_id", "location_id IS NOT NULL")
def raw_bdc():
    version = spark.conf.get("spark.sql.bdc_version")
    new_bdc = spark.read.format("csv").option('header','true').schema(bdc_schema).load(f'dbfs:/mnt/data/BEADAnalytics/Bronze/bdc/{version}/*.csv')
    return new_bdc


# COMMAND ----------

@dlt.create_table(
    name=f'bdc_history',
    table_properties = {"quality": "silver"},
    temporary=True
)
@dlt.expect_all_or_fail({"valid_location_id":"location_id IS NOT NULL", 'valid_state_geoid':'state_geoid IS NOT NULL'})
def read_bdc_in():
    bdc_df = spark.sql('SELECT * FROM ba_planning_tool.authoritative_bdc')
    return bdc_df

# COMMAND ----------

@dlt.view(
    name='location_id_to_geoid',
    table_properties = {'quality':'silver'},
    temporary=True
)
def get_lookup_data():
    return spark.sql('SELECT DISTINCT location_id, state_geoid, block_Geoid FROM LIVE.bdc_history')

# COMMAND ----------

def apply_filters (bdc):    
    bdc = bdc.filter("technology IN ('10','40','50','71','72')")
    bdc = bdc.filter(func.col('low_latency')==1)

    bdc = bdc.select('frn', 'brand_name', 'location_id', 'technology', 'max_advertised_download_speed', 'max_advertised_upload_speed', 'business_residential_code', 'effective_date', 'expiration_date', 'current_flag')

    return bdc

def get_speed_tier_num(upload, download):
    
    if upload != upload or download != download:
        return 0
    elif upload < 3.0 or download < 25.0:
        return 0
    elif (upload >= 3.0 and upload < 20.0) or (download >= 25.0 and download < 100.0):
        return 1
    elif upload >= 20.0 or download >= 100.0:
        return 2

get_speed_tier_num_udf = func.udf(get_speed_tier_num, IntegerType())

# COMMAND ----------

rules = {}
rules["valid_state_geoid"] = "(state_geoid IS NOT NULL)"
quarantine_rules = "NOT({0})".format(" AND ".join(rules.values()))

# COMMAND ----------

quarantine_rules

# COMMAND ----------


@dlt.create_table(
    name='auth_bdc_quarantine',
    temporary=True,
    table_properties = {"quality": "silver"},
)
@dlt.expect_all(rules)
def process_bdc():

    bdc_df = dlt.read("bdc_history")
    current_df = dlt.read("bdc_raw")
    lookup = dlt.read("location_id_to_geoid")

    version = spark.conf.get("spark.sql.bdc_version")

    version_date = version_dates[version]
    current_df = current_df.withColumn('effective_date', to_date(lit(version_date), DATE_FORMAT))\
                    .withColumn('expiration_date', to_date(lit(EOW_DATE), DATE_FORMAT))\
                    .withColumn('current_flag', lit(True))

    current_df = apply_filters(current_df)
    current_df = lookup.join(current_df, lookup.location_id==current_df.location_id, how='left')

    current_df = current_df.withColumn('speed_tier', get_speed_tier_num_udf(func.col('max_advertised_upload_speed'), func.col('max_advertised_download_speed')))
    df_history_open = bdc_df.where(col('current_flag'))
    df_history_closed = bdc_df.where(col('current_flag')==lit(False))

    df_history_open_hash = column_renamer(get_hash(df_history_open, type2_cols), suffix="_history", append=True)
    df_current_hash = column_renamer(get_hash(current_df, type2_cols), suffix="_current", append=True)

    df_merged = df_history_open_hash\
                .join(df_current_hash, ((col("location_id_current")==col("location_id_history"))&
                                        (col('frn_current')==col('frn_history'))&
                                        (col('brand_name_current')==col('brand_name_history'))&
                                        (col('technology_current')==col('technology_history'))&
                                        (col('max_advertised_download_speed_current')==col('max_advertised_download_speed_history'))&
                                        (col('max_advertised_upload_speed_current')==col('max_advertised_upload_speed_history'))&
                                        (col('business_residential_code_current')==col('business_residential_code_history'))&
                                        (col('speed_tier')==col('speed_tier'))), how="full_outer")\
                .withColumn("action", when(col("hash_md5_current") == col("hash_md5_history")  , 'NOCHANGE')\
                .when(col("location_id_current").isNull(), 'DELETE')\
                .when(col("location_id_history").isNull(), 'INSERT')\
                .otherwise('UPDATE'))

    df_nochange = column_renamer(df_merged.filter(col("action") == 'NOCHANGE'), suffix="_history", append=False)\
                .select(df_history_open.columns)

    df_insert = column_renamer(df_merged.filter(col("action") == 'INSERT'), suffix="_current", append=False)\
                .select(current_df.columns+['state_geoid','block_geoid'])\
                .withColumn('effective_date', lit(version_date))\
                .withColumn('effective_date', to_date(lit(version_date), DATE_FORMAT))\
                .withColumn('expiration_date', to_date(lit(EOW_DATE), DATE_FORMAT))\
                .withColumn('current_flag', lit(True))

    df_deleted = column_renamer(df_merged.filter(col("action") == 'DELETE'), suffix="_history", append=False)\
                .select(df_history_open.columns)\
                .withColumn("expiration_date", to_date(lit(version_date),DATE_FORMAT))\
                .withColumn("current_flag", lit(False))

    df_update = column_renamer(df_merged.filter(col("action") == 'UPDATE'), suffix="_history", append=False)\
                .select(df_history_open.columns)\
                .withColumn("expiration_date", to_date(lit(version_date),DATE_FORMAT))\
                .withColumn("current_flag", lit(False))\
            .unionByName(
            column_renamer(df_merged.filter(col("action") == 'UPDATE'), suffix="_current", append=False)\
                .select(current_df.columns+['state_geoid','block_geoid'])\
                .withColumn("effective_date",to_date(lit(version_date),DATE_FORMAT))\
                .withColumn("expiration_date",to_date(lit(EOW_DATE),DATE_FORMAT))\
                .withColumn("current_flag", lit(True))\
                )
            
    df_final = df_history_closed\
            .unionByName(df_nochange)\
            .unionByName(df_insert)\
            .unionByName(df_deleted)\
            .unionByName(df_update)
    
    df_final = df_final.withColumn("is_quarantined", expr(quarantine_rules))

    return df_final

    

# COMMAND ----------

@dlt.table(
  name="authoritative_bdc_dlt",
)
def get_valid_bdc_data():
  return (
    dlt.read("auth_bdc_quarantine")
      .filter("is_quarantined=false")
  )

@dlt.table(
  name="invalid_bdc",
)
def get_invalid_bdc_data():
  return (
    dlt.read("auth_bdc_quarantine")
      .filter("is_quarantined=true")
  )

# COMMAND ----------

# check = spark.sql("select * from ba_planning_tool.new_auth_bdc")

# COMMAND ----------


