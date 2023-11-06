# Databricks notebook source
# df = spark.sql("SELECT * FROM ba_planning_tool.authoritative_bdc_dlt")

# COMMAND ----------

# df = df.filter(col('effective_date').isNotNull())

# COMMAND ----------

# df.write.format('delta').mode('overwrite').saveAsTable('ba_planning_tool.authoritative_bdc_dlt')

# COMMAND ----------

# test = spark.sql('SELECT * FROM ba_planning_tool.authoritative_bdc_dlt WHERE location_id="1372517040"')

# COMMAND ----------

# test.display()

# COMMAND ----------

# spark.conf.set("spark.sql.state_name", "joe")
# spark.conf.set("spark.sql.bdc_version", "v28BETA")

# COMMAND ----------

# version = spark.conf.get("spark.sql.bdc_version")
# version

# COMMAND ----------

# check = spark.read.format('json').schema(max_date_schema).load('dbfs:/mnt/data/BEADAnalytics/Bronze/bdc_max_date.json')

# COMMAND ----------

# check.display()

# COMMAND ----------

# ppath = "/user/hive/warehouse/ba_planning_tool.db/version_lookup"

# COMMAND ----------

# df = spark.sql("SELECT * FROM ba_planning_tool.version_lookup")

# COMMAND ----------

# # Import functions
# from pyspark.sql.functions import col, current_timestamp

# # Define variables used in code below
# file_path = "/dbfs/user/hive/warehouse/ba_planning_tool.db/version_lookup"
# username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
# table_name = f"{username}_etl_quickstart"
# checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"

# # Clear out data from previous demo execution
# spark.sql(f"DROP TABLE IF EXISTS {table_name}")
# dbutils.fs.rm(checkpoint_path, True)

# # Configure Auto Loader to ingest JSON data to a Delta table
# (spark.readStream
#   .format("delta")
#   .schema(version_schema)
#   .load(file_path)
#   .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
#   .writeStream
#   .option("checkpointLocation", checkpoint_path)
#   .trigger(availableNow=True)
#   .toTable(table_name))

# COMMAND ----------

#version = dbutils.widgets.get('version')
#version = 'v29BETA'

# COMMAND ----------

# df = spark.createDataFrame([('v28BETA', '06-15-2023')], schema=version_schema)
# df = df.withColumn('version_date', lit(to_date(col('version_date'), DATE_FORMAT)))
# df.display()

# COMMAND ----------

# with open("/Workspace/Repos/jklein.ctr@ntia.gov/ba-planning-tool/bdc_max_date.json", "r") as read_file:
#     max_dict = json.load(read_file)

# COMMAND ----------

# def get_datetime(path):
#     return datetime.strptime(path, '%m-%d-%Y')

# max_date = get_datetime(max_dict['max_date'])

# COMMAND ----------

# version_data = spark.sql("SELECT * FROM ba_planning_tool.version_lookup")
# col_latest_date = func.max('version_date')
# max_data = version_data.select(col_latest_date)
# version_data = version_data.withColumn('max_date', lit(max_data.take(1)[0][0]))
# # max_date = get_date_value(max_data.select('max(version_date)')('max(version_date)'))
# version_data = version_data.filter(col('version_date')==col('max_date'))

# COMMAND ----------

# version_data.display()

# COMMAND ----------

# df = spark.table("ba_planning_tool.version_lookup").drop('version_time', '_rescue_data')

# COMMAND ----------

# df.write.mode('overwrite').saveAsTable("ba_planning_tool.version_lookup")

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

# bdc_df = column_renamer(get_hash(bdc_df.withColumn('bdc_time', func.current_timestamp()), type2_cols), suffix="_current", append=True)

# COMMAND ----------

# bdc_df.columns

# COMMAND ----------

# df = spark.sql('SELECT * FROM ba_planning_tool.authoritative_bdc').withColumn('bdc_time', func.current_timestamp())
# df_history = column_renamer(get_hash(df, type2_cols), suffix="_history", append=True)
# df_current = column_renamer(get_hash(df, type2_cols), suffix="_current", append=True)

# COMMAND ----------

@dlt.table(
    name='authoritative_bdc_dlt',
    table_properties = {"quality": "silver", "pipelines.autoOptimize.managed": "true"}
)
# @dlt.expect_all_or_fail(rules2)
def process_bdc_output():

    df_merged = spark.sql("SELECT * FROM LIVE.authoritative_cdc_merge").withWatermark('bdc_time', '1 second')
    df_history_closed = spark.sql("SELECT * FROM LIVE.bdc_history WHERE current_flag = FALSE")
    df_merged = df_merged.drop('__orig_data_[authoritative_cdc_merge]')
    df_history_closed = df_history_closed.drop('__orig_data_[bdc_history]', 'bdc_time')

    hist_cols = ['location_id_history',
                 'state_geoid_history',
                 'block_geoid_history',
                 'frn_history',
                 'brand_name_history',
                 'technology_history',
                 'max_advertised_download_speed_history',
                 'max_advertised_upload_speed_history',
                 'business_residential_code_history',
                 'effective_date_history',
                 'expiration_date_history',
                 'current_flag_history',
                 'speed_tier_history',
                 'bdc_time_history',
                 'hash_md5_history']

    curr_cols = ['location_id_current',
                 'state_geoid_current',
                 'block_geoid_current',
                 'frn_current',
                 'brand_name_current',
                 'technology_current',
                 'max_advertised_download_speed_current',
                 'max_advertised_upload_speed_current',
                 'business_residential_code_current',
                 'effective_date_current',
                 'expiration_date_current',
                 'current_flag_current',
                 'speed_tier_current',
                 'bdc_time_current',
                 'hash_md5_current']
    
    
    df_merged_join = df_merged.select('location_id_current', 'bdc_time_history').distinct()

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

# .withColumn('effective_date', lit(version_date))\
# .withColumn('effective_date', to_date(lit(version_date), DATE_FORMAT))\
# .withColumn("effective_date",to_date(lit(version_date),DATE_FORMAT))\

# COMMAND ----------

# check = spark.sql("select * from ba_planning_tool.new_auth_bdc")

# COMMAND ----------

# version_data = spark.read.format('parquet').load("dbfs:/mnt/data/BEADAnalytics/EE Toolkit/version_lookup_parquet")

# COMMAND ----------

# version_data.display()

# COMMAND ----------


# col_latest_date = func.max('version_date')
# max_data = version_data.select(col_latest_date)
# version_data = version_data.withColumn('max_date', lit(max_data.take(1)[0][0]))
# version_data = version_data.filter(col('version_date')==col('max_date'))
# version = collect_data(version_data, 'version_name')

# COMMAND ----------

# version_data = version_data.withColumn('version_time', func.current_timestamp())\
#                                .withWatermark('version_time', "5 Minutes")
# version_data.display()

# COMMAND ----------

# new_bdc = spark.read.format("csv").option('header','true').schema(bdc_schema).load(f'dbfs:/mnt/data/BEADAnalytics/Bronze/bdc/{version}/*.csv')

# current_df = new_bdc.withColumn('bdc_raw_time', func.current_timestamp())\
#                      .withWatermark('bdc_raw_time', "5 Minutes")

# COMMAND ----------

# current_df.display()

# COMMAND ----------

# bdc_df = spark.sql("SELECT * FROM ba_planning_tool.authoritative_bdc")
# bdc_df = bdc_df.withColumn('bdc_time', func.current_timestamp())\
#                 .withWatermark('bdc_time', "5 Minutes")

# COMMAND ----------

# bdc_df.select('effective_date').distinct().display()
# version_data = version_data.withColumn('temp', lit('temp')).select('temp', 'version_date', 'version_time')

# current_df = current_df.withColumn('temp', lit('temp'))
# current_df = current_df.join(version_data, current_df.temp==version_data.temp, how='left')
# current_df = current_df.withColumnRenamed('version_date', 'effective_date').drop('temp', 'version_time')

# current_df = current_df.withColumn('expiration_date', to_date(lit(EOW_DATE), DATE_FORMAT))\
#                         .withColumn('current_flag', lit(True))

# COMMAND ----------

lines
lines[1].split(';')
lines[3].split(';')
lines[5].split(';')
lines[7].split(';')
lines[9].split(';')
