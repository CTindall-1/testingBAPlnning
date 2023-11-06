# Databricks notebook source
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

# COMMAND ----------

rules1 = {}
rules1['valid_effective_date'] = "(effective_date_current IS NOT NULL)"

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

# COMMAND ----------

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


# COMMAND ----------

version_schema = StructType([StructField('version_name', StringType(), True),
                             StructField('version_date', StringType(), True)])

# COMMAND ----------

max_date_schema = StructType([StructField('max_date', StringType(), True)])
