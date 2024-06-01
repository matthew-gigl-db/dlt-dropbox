# Databricks notebook source
# dbutils.library.restartPython()

# COMMAND ----------

import dlt

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(spark.conf.get('bundle.sourcePath')))

import main

# COMMAND ----------

# import os

# def get_absolute_path(*relative_parts):
#     if 'dbutils' in globals():
#         base_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()) # type: ignore
#         path = os.path.normpath(os.path.join(base_dir, *relative_parts))
#         return path if path.startswith("/Workspace") else "/Workspace" + path
#     else:
#         return os.path.join(*relative_parts)

# COMMAND ----------

ddl_path = os.path.normpath(os.path.join(spark.conf.get('bundle.sourcePath'), "..", "fixtures", "ddl"))
ddl_path

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient(
  host = spark.conf.get('workflow_inputs.host')
  ,token = dbutils.secrets.get(spark.conf.get('workflow_inputs.secret_scope'), spark.conf.get('workflow_inputs.databricks_pat_name'))
)

# COMMAND ----------

# import pandas as pd

# def retrieve_ddl_files(ddl_path: str = ddl_path, workspace_client: WorkspaceClient =  w) -> pd.DataFrame:
#   """Retrieve all ddl files from the given path and load into a pandas dataframe."""
#   ddl_files = [file.as_dict() for file in w.workspace.list(path = ddl_path)]
#   for ddl_file in ddl_files:
#     path = ddl_file.get("path")
#     ddl_file["table_name"] = path.split("/")[-1].replace(".ddl", "")
#     with open(path, 'r') as file:
#       ddl_file["ddl"] = file.read()
#   return pd.DataFrame(ddl_files)


# COMMAND ----------

ddl_files = main.retrieve_ddl_files(
  ddl_path = ddl_path
  ,workspace_client = w
) 

# COMMAND ----------

# def load_ddl_files(ddl_df: pd.DataFrame = ddl_df):
#   @dlt.table(
#     name = "synthea_silver_schemas"
#     ,comment = "Reference table containing the schema definitions for the Synthea csv file datasets."
#     ,temporary = False
#     ,table_properties = {
#       "pipelines.autoOptimize.managed" : "true"
#       ,"pipelines.autoOptimize.zOrderCols" : None
#       ,"pipelines.reset.allowed" : "true"}
#   )
#   def synthea_schemas():
#     return spark.createDataFrame(ddl_files)

# COMMAND ----------

Pipeline = main.IngestionDLT(
    spark = spark
    # ,env_mode = env_mode
    # ,catalog = catalog_name
    # ,schema = schema_name
    ,volume = spark.conf.get("workflow_inputs.volume_path")
)

# COMMAND ----------

Pipeline.load_ddl_files(
  ddl_df = ddl_files
)
