# Databricks notebook source
# MAGIC %md
# MAGIC ## Reading netflix title data from bronze

# COMMAND ----------

df = spark.read.format("delta")\
            .option("header",True)\
            .option("inferSchema", True)\
            .load("abfss://bronze@netflixdatalakepradip.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.fillna({'duration_minutes': '0','duration_seasons': '1'})
df.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types  import *
from pyspark.sql.window import Window

# COMMAND ----------

df = df.withColumn('duration_minutes', col('duration_minutes').cast('int'))\
        .withColumn('duration_seasons', col('duration_seasons').cast('int'))
df.display()

# COMMAND ----------

df = df.withColumn('short_title',split(col('title'),':')[0])\
        .withColumn('rating',split(col('rating'),'-')[0])
df.display()

# COMMAND ----------

df = df.withColumn('flag_type',when(col('type')=='Movie',1)\
                                .when(col('type')=='TV Show',2)\
                                .otherwise(0))
df.display()

# COMMAND ----------

df = df.withColumn('duration_ranking',dense_rank().over(Window.orderBy(col('duration_minutes').desc())))
df.display()

# COMMAND ----------

df.groupBy('type').agg(count('*').alias('total_count')).display()

# COMMAND ----------

df.write.format('delta')\
        .mode('overwrite')\
        .option('path','abfss://silver@netflixdatalakepradip.dfs.core.windows.net/netflix_titles')\
        .save()

# COMMAND ----------

