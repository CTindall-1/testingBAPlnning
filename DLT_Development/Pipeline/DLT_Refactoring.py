# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as func
import numpy as np
from schema_config import *
from pyspark.sql.window import Window
from datetime import datetime
import json

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Schema

# COMMAND ----------

max_date_schema = StructType([StructField('max_date', StringType(), True)])

# COMMAND ----------

version_schema = StructType([StructField('version_name', StringType(), True),
                             StructField('version_date', StringType(), True)])

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

# MAGIC %md ### Variables and Cols

# COMMAND ----------

EOW_DATE = "12-31-9999"
DATE_FORMAT = "M-d-yyyy"

KEYS = ['location_id', 'frn', 'brand_name', 'technology',
              'max_advertised_download_speed', 'max_advertised_upload_speed', 
              'business_residential_code', 'speed_tier']

type2_cols = ['frn', 'brand_name', 'technology',
              'max_advertised_download_speed', 'max_advertised_upload_speed', 
              'business_residential_code', 'speed_tier', 'bdc_time']

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

        new_column_names = list(map(lambda x: x+suffix, df.columns))
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

# MAGIC %md #### Max Date Data

# COMMAND ----------

@dlt.table(
    name=f'max_date_data',
    temporary=True
)
def get_max_date_df():
    max_date_df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").schema(max_date_schema).load('dbfs:/mnt/data/BEADAnalytics/Bronze/bdc_max_date/')

    max_date_df = max_date_df.withColumnRenamed('max_date', 'version_date')\
                             .withColumn('version_date', lit(to_date(col('version_date'), DATE_FORMAT)))\
                             .withColumn('max_date_time', func.current_timestamp())\
                             .withWatermark('max_date_time', '5 Seconds')

    return max_date_df


# COMMAND ----------

# MAGIC %md #### Version Lookup Table

# COMMAND ----------

@dlt.table(
    name="version_lookup", 
    table_properties={"quality": "gold"}
)
def get_version_lookup():
#     # version_data = spark.sql("SELECT * FROM ba_planning_tool.version_lookup")
#     # col_latest_date = func.max('version_date')
#     # max_data = version_data.select(col_latest_date)
#     # max_date = get_date_value(max_data.select('max(version_date)'))
#     # version_data = version_data.withColumn('max_date', func.lit(max_date))\
#     #                            .filter(col('version_date')==col('max_date'))

#     # version_data = spark.readStream.format("delta").load("dbfs:/user/hive/warehouse/ba_planning_tool.db/version_lookup")

#     # schema=spark.read.format("delta").load("dbfs:/user/hive/warehouse/ba_planning_tool.db/version_lookup").schema
#     # version_data = spark.readStream.format("delta").load("dbfs:/user/hive/warehouse/ba_planning_tool.db/version_lookup")
#     # sq = version_data.writeStream.trigger(processingTime='5 minutes')\
#     #                              .option("checkpointLocation","/staging/checkpoint/INV_UNIT/") \
#     #                              .start("/staging/data/INV_UNIT/",queryName='version_query', outputMode="append", format='delta')\
#     #                              .awaitTermination()

#     # sq = version_data.writeStream.queryName('version_query').start()
#     # version_data.writeStream.trigger(once=True) \
#     #                         .option("checkpointLocation","/staging/checkpoint/INV_UNIT/") \
#     #                         .format("delta") \
#     #                         .outputMode("append") \
#     #                         .start("/staging/data/INV_UNIT/") \
#     #                         .awaitTermination()

#     # )

    # version_data = spark.readStream.format("cloudFiles")\
    #                     .option("cloudFiles.format", "parquet")\
    #                     .load("dbfs:/mnt/data/BEADAnalytics/EE Toolkit/version_lookup_parquet")

    try: 
        spark.sql('SELECT * FROM ba_planning_tool.version_lookup').count()
    except:
        df.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('ba_planning_tool.version_lookup')
    version_data = spark.sql("SELECT * FROM ba_planning_tool.version_lookup")
    max_date_df = dlt.read('max_date_data')

    version_num = version_data.count() + 7

    append_df = max_date_df.withColumn('version_name', lit(f'v2{str(version_num)}BETA'))\
                           .select('version_name', 'version_date')

    version_data = version_data.union(append_df).dropDuplicates(subset=['version_date'])

    # if version_data.count()+1 == check_df.count():
    #     append_df.writeStream.format("delta")\
    #                         .outputMode("append")\
    #                         .option("checkpointLocation", "/tmp/delta/version_lookup/_checkpoints/")\
    #                         .start('/dbfs/user/hive/warehouse/ba_planning_tool.db/version_lookup')
        
    return version_data

# COMMAND ----------

@dlt.table(
    name=f'bdc_raw',
    table_properties = {"quality": "bronze"},
    temporary=True
)
@dlt.expect_or_fail("valid_location_id", "location_id IS NOT NULL")
def raw_bdc():
    new_bdc = spark.readStream.format("cloudFiles")\
                              .option("cloudFiles.format", "parquet")\
                              .option('header','true')\
                              .schema(bdc_schema)\
                              .load('dbfs:/mnt/data/BEADAnalytics/Bronze/bdc_dlt/')

    new_bdc = new_bdc.withColumn('bdc_raw_time', func.current_timestamp())\
                     .withWatermark('bdc_raw_time', "5 Minutes")

    return new_bdc


# COMMAND ----------

@dlt.table(
    name=f'bdc_history',
    temporary=True
)
@dlt.expect_all_or_fail({"valid_location_id":"location_id IS NOT NULL", 'valid_state_geoid':'state_geoid IS NOT NULL'})
def read_bdc_in():
    bdc_df = spark.sql("SELECT * FROM ba_planning_tool.authoritative_bdc")
    bdc_df = bdc_df.withColumn('bdc_time', func.current_timestamp())\
                   .withWatermark('bdc_time', "5 Minutes")
    return bdc_df

# COMMAND ----------

@dlt.table(
    name='location_id_to_geoid',
    temporary=True
)
def get_lookup_data():

    df = spark.sql('SELECT DISTINCT location_id, state_geoid, block_geoid FROM LIVE.bdc_history')
    df = df.withColumn('bdc_time', func.current_timestamp())\
           .withWatermark('bdc_time', "5 Minutes")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### BDC Apply Filters

# COMMAND ----------

def apply_filters(bdc):    
    bdc = bdc.filter("technology IN ('10','40','50','71','72')")
    bdc = bdc.filter(func.col('low_latency')==1)

    bdc = bdc.select('frn', 'brand_name', 'location_id', 'technology', 'max_advertised_download_speed', 'max_advertised_upload_speed', 'business_residential_code', 'effective_date', 'expiration_date', 'current_flag', 'bdc_raw_time')

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

# MAGIC %md
# MAGIC #### Rules

# COMMAND ----------

rules1 = {}
rules1['valid_effective_date'] = "(effective_date_current IS NOT NULL)"

# COMMAND ----------

with open('/dbfs/tmp/logs/log_check.txt', 'r+') as read_logs:
    lines = read_logs.readlines()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Authoritative BDC

# COMMAND ----------


@dlt.table(
    name='authoritative_cdc_merge',
    table_properties = {"quality": "silver"},
    temporary=True
)
# @dlt.expect_all_or_fail(rules1)
def process_bdc_cdc_merge():

    bdc_df = dlt.read_stream("bdc_history").withWatermark('bdc_time', "5 seconds")
    # current_df = dlt.read("bdc_raw").withWatermark('bdc_raw_time', "5 Minutes")
    current_df = dlt.read_stream("bdc_raw").withWatermark('bdc_raw_time', "5 Seconds")
    lookup = dlt.read_stream("location_id_to_geoid").withWatermark('bdc_time', "5 Seconds")
    version_data = dlt.read("version_lookup").withWatermark('version_time', "5 Minutes")
    max_data_df = dlt.read_stream("max_date_data")
    
    max_data_df = max_data_df.select('version_date', 'max_date_time').withColumn('temp', lit('temp'))\
                                                                     .withColumnRenamed('version_date', 'effective_date')\
                                                                     .withWatermark('max_date_time', '5 Seconds')

    
    current_df = current_df.withColumn('temp', lit('temp'))
    current_df = current_df.join(max_data_df, current_df.temp==max_data_df.temp, how='left').drop('temp')
    current_df = current_df.withColumn('effective_date', to_date(col('effective_date'), DATE_FORMAT))

    current_df = current_df.withColumn('expiration_date', to_date(lit(EOW_DATE), DATE_FORMAT))\
                           .withColumn('current_flag', lit(True))

    

    current_df = apply_filters(current_df)
    current_df = lookup.alias('lkup').join(current_df.alias('curr'), col('lkup.location_id')==col('curr.location_id'), how='left')
    current_df = dropDupeCols(current_df).drop('bdc_raw_time')

    current_df = current_df.fillna(0.00, subset=['max_advertised_download_speed', 'max_advertised_upload_speed'])
    current_df = current_df.withColumn("max_advertised_upload_speed", col('max_advertised_upload_speed').cast(DoubleType()))\
                             .withColumn('max_advertised_download_speed', col('max_advertised_download_speed').cast(DoubleType()))

    current_df = current_df.withColumn('speed_tier', get_speed_tier_num_udf(func.col('max_advertised_upload_speed'), func.col('max_advertised_download_speed')))

    df_history_open = bdc_df.where(col('current_flag'))
    
    # current_df = current_df.drop('__orig_data_[bdc]', '__orig_data_[bdc_history]', '__orig_data_[location_id_to_geoid]')
    # df_history_open = df_history_open.drop('__orig_data_[bdc]', '__orig_data_[bdc_history]', '__orig_data_[location_id_to_geoid]')
    # df_history_closed = df_history_closed.drop('__orig_data_[bdc]', '__orig_data_[bdc_history]', '__orig_data_[location_id_to_geoid]')
    # current_df = current_df.select('location_id', 'state_geoid', 'block_geoid', 'frn', 'brand_name', 'technology',
    #                                'max_advertised_download_speed', 'max_advertised_upload_speed', 'business_residential_code', 'effective_date',
    #                                'expiration_date', 'current_flag', 'speed_tier', 'bdc_time')
    
    # df_history_open = df_history_open.select('location_id', 'state_geoid', 'block_geoid', 'frn', 'brand_name', 'technology',
    #                                          'max_advertised_download_speed', 'max_advertised_upload_speed', 'business_residential_code',
    #                                          'effective_date', 'expiration_date', 'current_flag', 'speed_tier', 'bdc_time')
    
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
                                        (col('speed_tier_current')==col('speed_tier_history'))), how="full_outer")\
                .withColumn("action", when(col("hash_md5_current") == col("hash_md5_history")  , 'NOCHANGE')\
                .when(col("location_id_current").isNull(), 'DELETE')\
                .when(col("location_id_history").isNull(), 'INSERT')\
                .otherwise('UPDATE'))\

    df_merged = dropDupeCols(df_merged)
    
    return df_merged

# COMMAND ----------

rules2 = {}
rules2['valid_effective_date'] = "(effective_date IS NOT NULL)"
rules2['valid_state_geoid'] = "(state_geoid IS NOT NULL)"

# COMMAND ----------

# @dlt.table(
#     name='authoritative_bdc_dlt',
#     table_properties = {"quality": "silver", "pipelines.autoOptimize.managed": "true"}
# )
# # @dlt.expect_all_or_fail(rules2)
# def process_bdc_output():

#     df_merged = spark.sql("SELECT * FROM LIVE.authoritative_cdc_merge").withWatermark('bdc_time', '1 second')
#     df_history_closed = spark.sql("SELECT * FROM LIVE.bdc_history WHERE current_flag = FALSE")
#     df_merged = df_merged.drop('__orig_data_[authoritative_cdc_merge]')
#     df_history_closed = df_history_closed.drop('__orig_data_[bdc_history]', 'bdc_time')

#     hist_cols = ['location_id_history',
#                  'state_geoid_history',
#                  'block_geoid_history',
#                  'frn_history',
#                  'brand_name_history',
#                  'technology_history',
#                  'max_advertised_download_speed_history',
#                  'max_advertised_upload_speed_history',
#                  'business_residential_code_history',
#                  'effective_date_history',
#                  'expiration_date_history',
#                  'current_flag_history',
#                  'speed_tier_history',
#                  'bdc_time_history',
#                  'hash_md5_history']

#     curr_cols = ['location_id_current',
#                  'state_geoid_current',
#                  'block_geoid_current',
#                  'frn_current',
#                  'brand_name_current',
#                  'technology_current',
#                  'max_advertised_download_speed_current',
#                  'max_advertised_upload_speed_current',
#                  'business_residential_code_current',
#                  'effective_date_current',
#                  'expiration_date_current',
#                  'current_flag_current',
#                  'speed_tier_current',
#                  'bdc_time_current',
#                  'hash_md5_current']
    
    
#     df_merged_join = df_merged.select('location_id_current', 'bdc_time_history').distinct()

    df_nochange = column_renamer(df_merged.filter(col("action") == 'NOCHANGE'), suffix="_history", append=False)\
                .drop(*curr_cols,'action','hash_md5')\
                .withColumn('effective_date', to_date(col('effective_date').cast("string"), DATE_FORMAT))\
                .withColumn('expiration_date', to_date(col('expiration_date').cast("string"), DATE_FORMAT))\
                .drop('bdc_time')

    df_insert = column_renamer(df_merged.filter(col("action") == 'INSERT'), suffix="_current", append=False)\
                .drop(*hist_cols, 'action', 'hash_md5')\
                .withColumn('expiration_date', to_date(lit(EOW_DATE), DATE_FORMAT))\
                .withColumn('effective_date', to_date(col('effective_date').cast("string"), DATE_FORMAT))\
                .withColumn('current_flag', lit(True))\
                .drop('bdc_time')

    df_deleted = column_renamer(df_merged.filter(col("action") == 'DELETE'), suffix="_history", append=False)\
                .drop(*curr_cols, 'action','hash_md5')\
                .withColumn("expiration_date", col('effective_date'))\
                .withColumn("current_flag", lit(False))\
                .withColumn('effective_date', to_date(col('effective_date').cast("string"), DATE_FORMAT))\
                .withColumn('expiration_date', to_date(col('expiration_date').cast("string"), DATE_FORMAT))\
                .drop('bdc_time')

    curr_test = column_renamer(df_merged.filter(col("action") == 'UPDATE'), suffix="_current", append=False)\
                .drop(*hist_cols, 'action','hash_md5',)\
                .withColumn("expiration_date", col('effective_date'))\
                .withColumn("current_flag", lit(False))\
                .withColumn('effective_date', to_date(col('effective_date').cast("string"), DATE_FORMAT))\
                .withColumn('expiration_date', to_date(col('expiration_date').cast("string"), DATE_FORMAT))\
                .drop('bdc_time')
                            
    hist_test = column_renamer(df_merged.filter(col("action") == 'UPDATE'), suffix="_history", append=False)\
                .drop(*curr_cols, 'action','hash_md5')\
                .withColumn("expiration_date", col('effective_date'))\
                .withColumn("current_flag", lit(False))\
                .withColumn('effective_date', to_date(col('effective_date').cast("string"), DATE_FORMAT))\
                .withColumn('expiration_date', to_date(col('expiration_date').cast("string"), DATE_FORMAT))\
                .drop('bdc_time')

    with open('/dbfs/tmp/logs/log_check.txt', 'w') as file_writer:
        file_writer.write("not_change\n")
        file_writer.write('; '.join([str(i) +" -- "+str(x) for i, x in enumerate(df_nochange.dtypes)])+'\n')
        file_writer.write("insert\n")
        file_writer.write('; '.join([str(i) +" -- "+str(x) for i, x in enumerate(df_insert.dtypes)])+'\n')
        file_writer.write("deleted\n")
        file_writer.write('; '.join([str(i) +" -- "+str(x) for i, x in enumerate(df_deleted.dtypes)])+'\n')
        file_writer.write("current\n")
        file_writer.write('; '.join([str(i) +" -- "+str(x) for i, x in enumerate(curr_test.dtypes)])+'\n')
        file_writer.write("history\n")
        file_writer.write('; '.join([str(i) +" -- "+str(x) for i, x in enumerate(hist_test.dtypes)])+'\n')
        file_writer.close()

    df_update = column_renamer(df_merged.filter(col("action") == 'UPDATE'), suffix="_history", append=False)\
                .drop(*curr_cols, 'action','hash_md5')\
                .withColumn("expiration_date", col('effective_date'))\
                .withColumn("current_flag", lit(False))\
                .withColumn('effective_date', to_date(col('effective_date').cast("string"), DATE_FORMAT))\
                .withColumn('expiration_date', to_date(col('expiration_date').cast("string"), DATE_FORMAT))\
                .drop('bdc_time')\
            .unionByName(
            column_renamer(df_merged.filter(col("action") == 'UPDATE'), suffix="_current", append=False)\
                .drop(*hist_cols, 'action', 'hash_md5')\
                .withColumn("expiration_date",to_date(lit(EOW_DATE),DATE_FORMAT))\
                .withColumn('effective_date', to_date(col('effective_date').cast("string"), DATE_FORMAT))\
                .withColumn("current_flag", lit(True))\
                .drop('bdc_time')
                )

    df_final =  df_history_closed\
                .unionByName(df_nochange)\
                .unionByName(df_insert)\
                .unionByName(df_deleted)\
                .unionByName(df_update)

    return df_final

