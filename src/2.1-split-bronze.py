# Databricks notebook source
import dlt

# COMMAND ----------

from classDefinitions import *

# COMMAND ----------

# # used for active development, but not run during DLT execution, use DLT configurations instead
# dbutils.widgets.dropdown(name = "env_mode", defaultValue = "prd", choices = ["dev", "tst", "uat", "prd"], label = "Environment Mode")
# dbutils.widgets.text(name = "catalog_name", defaultValue="", label="Catalog Name")
# dbutils.widgets.text(name = "schema_name", defaultValue="synthea", label="Schema Name")
# dbutils.widgets.text(name = "volume_name", defaultValue="synthetic_files_raw", label="Volume Name")

# spark.conf.set("workflow_inputs.env_mode", dbutils.widgets.get(name = "env_mode"))
# spark.conf.set("workflow_inputs.catalog_name", dbutils.widgets.get(name = "catalog_name"))
# spark.conf.set("workflow_inputs.schema_name", dbutils.widgets.get(name = "schema_name"))
# spark.conf.set("workflow_inputs.volume_name", dbutils.widgets.get(name = "volume_name"))

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

env_mode = spark.conf.get("workflow_inputs.env_mode")
catalog_name = spark.conf.get("workflow_inputs.catalog_name")
schema_name = spark.conf.get("workflow_inputs.schema_name")
volume_name = spark.conf.get("workflow_inputs.volume_name")
volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/"
print(f"""
    env_mode = {env_mode}
    catalog_name = {catalog_name}
    schema_name = {schema_name}
    volume_name = {volume_name}
    volume_path = {volume_path}
""")

# COMMAND ----------

Pipeline = IngestionDLT(
    spark = spark
    ,env_mode = env_mode
    ,catalog = catalog_name
    ,schema = schema_name
    ,volume = volume_name
)

# COMMAND ----------

Pipeline

# COMMAND ----------

Pipeline.split_bronze_table_synchronous(
  bronze_table = "synthea_csv_bronze"
  ,live = False
)
