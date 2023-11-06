# Databricks notebook source
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


# COMMAND ----------

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

def apply_filters(bdc):    
    bdc = bdc.filter("technology IN ('10','40','50','71','72')")
    bdc = bdc.filter(func.col('low_latency')==1)

    bdc = bdc.select('frn', 'brand_name', 'location_id', 'technology', 'max_advertised_download_speed', 'max_advertised_upload_speed', 'business_residential_code', 'effective_date', 'expiration_date', 'current_flag', 'bdc_raw_time')

    return bdc

# COMMAND ----------

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
