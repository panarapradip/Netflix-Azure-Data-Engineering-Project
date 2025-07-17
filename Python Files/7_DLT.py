# Databricks notebook source
gold_rules = {
    "rule1": "show_id is NOT NULL"
}

# COMMAND ----------

@dlt.table

@dlt.expect_all(gold_rules)
def gold_netflix_cast():
    df = spark.readStream.format("delta").load('abfss://silver@netflixdatalakepradip.dfs.core.windows.net/netflix_cast')
    return df

# COMMAND ----------

@dlt.table

@dlt.expect_all(gold_rules)
def gold_netflix_category():
    df = spark.readStream.format("delta").load('abfss://silver@netflixdatalakepradip.dfs.core.windows.net/netflix_category')
    return df

# COMMAND ----------

@dlt.table

@dlt.expect_all(gold_rules)
def gold_netflix_countries():
    df = spark.readStream.format("delta").load('abfss://silver@netflixdatalakepradip.dfs.core.windows.net/netflix_countries')
    return df

# COMMAND ----------

@dlt.table

@dlt.expect_all(gold_rules)
def gold_netflix_directors():
    df = spark.readStream.format("delta").load('abfss://silver@netflixdatalakepradip.dfs.core.windows.net/netflix_directors')
    return df

# COMMAND ----------

@dlt.table

def gold_stg_netflixtitles():
    df = spark.readStream.format("delta").load('abfss://silver@netflixdatalakepradip.dfs.core.windows.net/netflix_titles')
    return df

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

@dlt.view

def gold_trns_netflixtitles():
    df = spark.readStream.table("LIVE.gold_stg_netflixtitles")
    df = df.withColumn("newflag",lit("Y"))
    return df

# COMMAND ----------

master_rules = {
    "rule1": "show_id is NOT NULL",
    "rule2": "newflag is NOT NULL"
}

# COMMAND ----------

@dlt.table

@dlt.expect_all_or_drop(master_rules)
def gold_netflixtitles():
    df = spark.readStream.table("LIVE.gold_trns_netflixtitles")
    return df