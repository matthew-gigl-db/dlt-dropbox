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

import sys, os
sys.path.append(os.path.abspath(spark.conf.get('bundle.sourcePath')))

import main

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC For development purposes only, uncommment the below code to manually set the Spark Conf Variables using Databricks Widgets.  

# COMMAND ----------

# # used for active development, but not run during DLT execution, use DLT configurations instead
# dbutils.widgets.text(name = "volume_path", defaultValue="/Volumes/mgiglia/synthea/synthetic_files_raw", label="Volume Path")

# spark.conf.set("workflow_inputs.volume_path", dbutils.widgets.get(name = "volume_path"))

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC Retreive inputs for the DLT run from the Spark Conf. 

# COMMAND ----------

volume_path = spark.conf.get("workflow_inputs.volume_path")
print(f"""
    volume_path = {volume_path}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC Initialize the pipeline as an IngestionDLT class object.  

# COMMAND ----------

Pipeline = main.IngestionDLT(
    spark = spark
    ,volume = spark.conf.get("workflow_inputs.volume_path")
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
    ,options = {"skipRows" : 1}
)

# COMMAND ----------

Pipeline.list_dropbox_files(
  bronze_table = "synthea_csv_bronze"
)

# COMMAND ----------

full_paths = main.recursive_ls(f"{volume_path}/output/csv")
shortened_names = []
for full_path in full_paths:
  filename = full_path.split("/")[-1]
  shortened_names.append(filename)
filenames_list = list(set(shortened_names))
filenames_list

# COMMAND ----------

# filenames_list = ("encounters.csv", "allergies.csv", "imaging_studies.csv", "providers.csv", "medications.csv", "patients.csv", "immunizations.csv", "payer_transitions.csv", "conditions.csv", "observations.csv", "claims_transactions.csv", "careplans.csv", "supplies.csv", "procedures.csv", "devices.csv", "payers.csv", "claims.csv", "organizations.csv")

# COMMAND ----------

for filename in filenames_list:
  name = filename.replace(".", "_")
  Pipeline.split_bronze_table(bronze_table = "synthea_csv_bronze", filename = filename, table_name = name)
