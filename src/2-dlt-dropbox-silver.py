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

# Pipeline.stage_silver(
#   bronze_table = f"{catalog}.{schema}.allergies_csv_bronze"
#   ,table_name = "allergies"
#   ,ddl = 'struct<start:date,stop:date,patient_id:string,encounter_id:string,code:string,system:string,description:string,type:string,category:string,reaction1:string,description1:string,severity1:string,reaction2:string,description2:string,severity2:string>'
# )
