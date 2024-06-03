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

Pipeline.stage_silver(
  bronze_table = "allergies_csv_bronze"
  ,table_name = "allergies"
  ,ddl = 'struct<start:date,stop:date,patient_id:string,encounter_id:string,code:string,system:string,description:string,type:string,category:string,reaction1:string,description1:string,severity1:string,reaction2:string,description2:string,severity2:string>'
)
