# Databricks notebook source
import dlt

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(spark.conf.get('bundle.sourcePath')))

import main

# COMMAND ----------

Pipeline = main.IngestionDLT(
    spark = spark
    ,volume = spark.conf.get("workflow_inputs.volume_path")
)

# COMMAND ----------


