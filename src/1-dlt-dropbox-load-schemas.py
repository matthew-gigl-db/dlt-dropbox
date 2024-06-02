# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip show databricks-sdk | grep -oP '(?<=Version: )\S+'

# COMMAND ----------

import dlt

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(spark.conf.get('bundle.sourcePath')))

import main

# COMMAND ----------

ddl_path = spark.conf.get('bundle.fixturePath') + "/ddl"
ddl_path

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient(
  host = spark.conf.get('workflow_inputs.host')
  ,token = dbutils.secrets.get(spark.conf.get('workflow_inputs.secret_scope'), spark.conf.get('workflow_inputs.databricks_pat_key'))
)

# COMMAND ----------

ddl_files = main.retrieve_ddl_files(
  ddl_path = ddl_path
  ,workspace_client = w
) 

# COMMAND ----------

Pipeline = main.IngestionDLT(
    spark = spark
    ,volume = spark.conf.get("workflow_inputs.volume_path")
)

# COMMAND ----------

Pipeline.load_ddl_files(
  ddl_df = ddl_files
)
