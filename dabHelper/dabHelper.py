# functions to assist in using Databricks Asset Bundles directly on Databricks

import subprocess
from tempfile import TemporaryDirectory

def clone_bundle(directory: str, repo_url: str, project: str):
  cmd = f"cd {temp_directory}; pwd; git clone {repo_url}; cd {project}; ls -alt;"
  result = subprocess.run(cmd, shell=True, capture_output=True)
  return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")

def remove_cloned_bundle(directory: str, project: str):
  cmd = f"rm -rf {directory}/project/.git; rm -rf {directory}"
  result = subprocess.run(cmd, shell=True, capture_output=True)
  return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")  

def validate_bundle(bundle_path: str, cli_path: str):
  cmd = f"""cd {bundle_path}; pwd; git pull; {cli_path} bundle validate"""
  result = subprocess.run(cmd, shell=True, capture_output=True)
  return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")

def deploy_bundle(bundle_path: str, cli_path: str, target: str = "dev"):
  cmd = f"cd {bundle_path}; pwd; {cli_path} bundle deploy -t {target}"
  result = subprocess.run(cmd, shell=True, capture_output=True)
  return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")

  def destroy_bundle(bundle_path: str, cli_path: str, target: str = "dev"):
    cmd = f"cd {bundle_path}; pwd; {cli_path} bundle destroy -t {target}"    
    result = subprocess.run(cmd, shell=True, capture_output=True)
    return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")
  
  

