# functions to assist in using Databricks Asset Bundles directly on Databricks

import subprocess
from tempfile import TemporaryDirectory

class databricksCli:

    def __init__(
      self
      ,workspace_url: str
      ,db_pat: str
    ):
      self.workspace_url = workspace_url
      self.db_pat = db_pat

    def __repr__(self):
      return f"""databricksCli(workspace_url='{self.workspace_url}', db_pat='{self.db_pat}')"""
    
    def install(self) -> str:
      cmd = f"curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"
      result = subprocess.run(cmd, shell=True, capture_output=True)
      response = result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")
      if response[0:4] == "Inst":
        cli_path = response.split("at ")[1].replace(".", "").strip("\n")
      else:
        cli_path = response.split(" ")[2].strip("\n")
      version = subprocess.run(f"{cli_path} --version", shell=True, capture_output=True)
      print(f"databricks-cli installed at {cli_path} version {version.stdout.decode('utf-8').strip()}")
      self.cli_path = cli_path
      return cli_path
    
    def configure(self):
      cmd= f"""echo '{self.db_pat}' | {self.cli_path} configure --host '{self.workspace_url}'"""
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result
    
    def validate(self):
      cmd = f"{self.cli_path} current-user me"
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result

      
  



class assetBundle:

    def __init__(
      self
      ,directory: str
      ,repo_url: str
      ,project: str
      ,cli_path: str
      ,target: str = "dev"
    ):
      self.directory = directory
      self.repo_url = repo_url
      self.project = project
      self.target = target
      self.cli_path = cli_path

    def __repr__(self):
        return f"""assetBundle(directory='{self.directory}', repo_url='{self.repo_url}', project='{self.project}', target='{self.target}', cli_path='{self.cli_path}')"""

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
  
  

