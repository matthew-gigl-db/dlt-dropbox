# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Bronze Autoloader Ingestion - Dropbox
# MAGIC ***
# MAGIC
# MAGIC This notebook automatically streams in any files that have been landed in the configured volume as a full text key value pair brone quality table with the following schema:  
# MAGIC
# MAGIC * **fullFilePath** 
# MAGIC * **datasource**
# MAGIC * **inputFileName**
# MAGIC * **ingestTime**
# MAGIC * **ingestDate**
# MAGIC * **value**
# MAGIC * **fileMetadata**

# COMMAND ----------

# MAGIC %md
# MAGIC *** 
# MAGIC
# MAGIC Import dlt and the operations and classes defined for the pipeline.  

# COMMAND ----------

import dlt

# COMMAND ----------

from classDefinitions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC For development purposes only, uncommment the below code to manually set the Spark Conf Variables using Databricks Widgets.  

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

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC Retreive inputs for the DLT run from the Spark Conf. 

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

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC Initialize the pipeline as an IngestionDLT class object.  

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

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC Ingest the raw files into a key-value pair bronze table.  

# COMMAND ----------

Pipeline.ingest_raw_to_bronze(
    table_name="synthea_csv_bronze"
    ,table_comment="A full text record of every file that has landed in our raw synthea landing folder."
    ,table_properties={"quality":"bronze", "phi":"True", "pii":"True", "pci":"False"}
    ,source_folder_path_from_volume="output/csv"
)

# COMMAND ----------

Pipeline.list_dropbox_files(
  bronze_table = "synthea_csv_bronze"
)

# COMMAND ----------

# filenames = spark.sql(f"select distinct * from {catalog_name}.{schema_name}.temp_landed_bronze_files").collect()
# filenames_list = [row.inputFileName for row in filenames]
# filenames_list

# COMMAND ----------

filenames_list = ("encounters.csv", "allergies.csv", "imaging_studies.csv", "providers.csv", "medications.csv", "patients.csv", "immunizations.csv", "payer_transitions.csv", "conditions.csv", "observations.csv", "claims_transactions.csv", "careplans.csv", "supplies.csv", "procedures.csv", "devices.csv", "payers.csv", "claims.csv", "organizations.csv")

# COMMAND ----------

for filename in filenames_list:
  name = filename.replace(".", "_")
  Pipeline.split_bronze_table(bronze_table = "synthea_csv_bronze", filename = filename, table_name = name, live = True)
