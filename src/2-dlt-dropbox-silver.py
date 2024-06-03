# Databricks notebook source
import dlt

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(spark.conf.get('bundle.sourcePath')))

import main

# COMMAND ----------

key_path = spark.conf.get('bundle.fixturePath') + "/keys"
sys.path.append(os.path.abspath(key_path))

import keys

# COMMAND ----------

Pipeline = main.IngestionDLT(
    spark = spark
    ,volume = spark.conf.get("workflow_inputs.volume_path")
)

# COMMAND ----------

catalog = spark.conf.get("workflow_inputs.catalog")
schema = spark.conf.get("workflow_inputs.schema")

# COMMAND ----------

ddl_ref = spark.sql(f"select table_name, ddl from {catalog}.{schema}.synthea_silver_schemas").collect()
ddl_ref = [row.asDict() for row in ddl_ref]
ddl_ref

# COMMAND ----------

for i in ddl_ref:
  table_name = i["table_name"]
  ddl = i["ddl"]
  Pipeline.stage_silver(
    bronze_table = f"{catalog}.{schema}.{table_name}_csv_bronze"
    ,table_name = table_name
    ,ddl = ddl
  )

# COMMAND ----------

Pipeline.stream_silver(
  bronze_table = f"{catalog}.{schema}.allergies_csv_bronze"
  ,table_name = "allergies"
  ,sequence_by = "sequence_by"
  ,keys = keys.allergies
  ,schema = None
)

# COMMAND ----------


