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
      
#######################  
#######################
#######################

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
      self.bundle_path = f"{self.directory}/{self.project}"
      self.cli_path = cli_path

    def __repr__(self):
        return f"""assetBundle(directory='{self.directory}', repo_url='{self.repo_url}', project='{self.project}', target='{self.target}', bundle_path='{self.bundle_path}', cli_path='{self.cli_path}')"""

    def clone(self):
      cmd = f"cd {self.directory}; pwd; git clone {self.repo_url}; cd {self.project}; ls -alt;"
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")

    def remove_clone(self):
      cmd = f"rm -rf {self.bundle_path}/.git; rm -rf {self.directory}"
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")  
    
    def checkout(self, branch: str):
      cmd = f"cd {self.bundle_path}; git checkout {branch}"
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")

    def validate(self):
      cmd = f"""cd {self.bundle_path}; pwd; git pull; {self.cli_path} bundle validate"""
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")

    def deploy(self):
      cmd = f"cd {self.bundle_path}; pwd; {self.cli_path} bundle deploy -t {self.target}"
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")

    def destroy(self):
      cmd = f"cd {self.bundle_path}; pwd; {self.cli_path} bundle destroy -t {self.target} --auto-approve"    
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")
    
    def run(self, key: str, pipeline_flag: str = "--validate-only"):
      cmd = f"cd {self.bundle_path}; pwd; {self.cli_path} bundle run -t {self.target} {pipeline_flag} {key}"    
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")
  
  

