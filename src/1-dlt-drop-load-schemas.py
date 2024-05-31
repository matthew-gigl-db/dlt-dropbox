# Databricks notebook source
import pandas as pd
import os

def get_absolute_path(*relative_parts):
    if 'dbutils' in globals():
        base_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()) # type: ignore
        path = os.path.normpath(os.path.join(base_dir, *relative_parts))
        return path if path.startswith("/Workspace") else "/Workspace" + path
    else:
        return os.path.join(*relative_parts)

ddl_path = get_absolute_path("../..", "fixtures/ddl", "allergies.ddl")

# COMMAND ----------

ddl_content = {}
with open(ddl_path, 'r') as file:
    ddl_content['allergies_ddl'] = file.read()

# COMMAND ----------

print(ddl_content['allergies_ddl'])
