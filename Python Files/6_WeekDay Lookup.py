# Databricks notebook source
var = dbutils.jobs.taskValues.get(taskKey = "WeekdayLookup", key = "weekoutput")
print(var)

# COMMAND ----------

