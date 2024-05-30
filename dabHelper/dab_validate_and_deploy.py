# Databricks notebook source
user_name = spark.sql("select current_user()").collect()[0][0]
secret_scope = user_name.split(sep="@")[0].replace(".", "-")
secret_scope

# COMMAND ----------

dbutils.widgets.text(name = "repo_url", defaultValue="")
dbutils.widgets.text(name = "project", defaultValue="")
dbutils.widgets.text(name = "workspace_url", defaultValue="")

# Add a widget for the Databricks Secret representing the Databricks Personal Access Token  
dbutils.widgets.text("pat_secret", "databricks_pat", "DB Secret for PAT")

# COMMAND ----------

repo_url = dbutils.widgets.get(name="repo_url")
project = dbutils.widgets.get(name="project")
workspace_url = dbutils.widgets.get(name="workspace_url")
print(
f"""
  repo_url = {repo_url}
  project = {project}
  workspace_url = {workspace_url}
"""
)

# COMMAND ----------


import dabHelper
import subprocess
from tempfile import TemporaryDirectory

# COMMAND ----------

Dir = TemporaryDirectory()
temp_directory = Dir.name

# COMMAND ----------

temp_directory

# COMMAND ----------

command = f"cd {temp_directory}; pwd; git clone {repo_url}; cd {project}; ls -alt;"

!{command}

# COMMAND ----------

  Dir = TemporaryDirectory()
  temp_directory = Dir.name  

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

print(remove_cloned_bundle(
  directory=temp_directory
  ,project = project
))

# COMMAND ----------

install_cmd_resp = !{"""curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"""}
install_cmd_resp

# COMMAND ----------

if install_cmd_resp[0][0:4] == "Inst":
  cli_path = install_cmd_resp[0].split("at ")[1].replace(".", "")
else:
  cli_path = install_cmd_resp[0].split(" ")[2]
cli_path

# COMMAND ----------

version_cmd = f"{cli_path} -v"

!{version_cmd}

# COMMAND ----------

db_pat = dbutils.secrets.get(
  scope = secret_scope
  ,key = dbutils.widgets.get("pat_secret")
)

db_pat

# COMMAND ----------

configure_command = f"""echo '{db_pat}' | {cli_path} configure --host 'https://{workspace_url}'"""

!{configure_command}

# COMMAND ----------

check_cli_cmd = f"{cli_path} current-user me"
!{check_cli_cmd}

# COMMAND ----------

def validate_bundle(bundle_path: str, cli_path: str):
  cmd = f"""cd {bundle_path}; pwd; git pull; {cli_path} bundle validate"""
  result = subprocess.run(cmd, shell=True, capture_output=True)
  return result.stdout.decode("utf-8")

# COMMAND ----------

print(validate_bundle(
  bundle_path=f"{temp_directory}/{project}"
  ,cli_path=cli_path
))

# COMMAND ----------

print(
  deploy_bundle(
    bundle_path=f"{temp_directory}/{project}"
    ,cli_path=cli_path
    ,target="dev"
  )
)
