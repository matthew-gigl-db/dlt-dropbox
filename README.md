# dlt-dropbox

## Motivation 

In order to truly democratize Data + AI there needs to be an easy way to ingest files from any source into the bronze and silver layers of the Lakehouse.  Analytics teams outside of typical IT organizations, or downstream of Data Management teams often struggle to get IT resources to set up all of the appropriate cloud data storage containers and buckets, file watchers and file movement processes, let alone additional Unity Catalog external locations and Volumes, needed for each additional file source that needs to be ingested.  Typically, a team might be given a single "landing container" on their cloud data storage and this landing container is then used to move all files from on prem (or outside sources) to an area where the team can use it for analysis.  

![images/DLT-Dropbox File Movement Process.png](https://github.com/matthew-gigl-db/dlt-dropbox/blob/main/images/DLT-Dropbox%20File%20Movement%20Process.png)

With this in mind, we've designed DLT-Dropbox.  A meta-data driven example of how files can be landing into a single container for ingestion and then autoloaded using Delta Live Tables into bronze and silver layer Delta Lake tables.  

## Conceptual Design 

![images/DLT-Dropbox Conceptual Design.png](https://github.com/matthew-gigl-db/dlt-dropbox/blob/main/images/DLT-Dropbox%20Conceptual%20Design.png)

* Once files have arrived into into the designated landing container, Databricks Autoloader streams the files into a key-value pair style Bronze table using a Delta Live Tables (DLT) pipeline.  This first Bronze table is a record of every file that has arrived in the landing folder and Autoloader keeps track of what files have arrived since the last time the process was run.  Databricks Autoloader makes it easy to stream any file into the Lakehouse.  
* The DLT pipeline captures the distinct file names and paths ingested in the Bronze table, and based on matching patterns for the filenames (or paths) a distinct list of file source types is generated.  DLT functions then loop over this list to create separate streaming Bronze tables for each file source (i.e. files with the same schema to be processed similiarly together later in Silver).
* For known file sources, the DDL and natural keys of the are documented in a version controlled repo (e.g. the "fixtures" folder of this project) as authored by a Data Architect, Solution Architect or Data Engineer.  At runtime, the DLT pipeline ingests the DDL and creates a reference table of streaming target table names, schemas, and keys for applying CDC changes to those streaming target table names.  
* DLT pipeline methods are then applied over the reference table and automatically generates the DAG (directed acyclic graph) for each streaming table Silver table to be created.  A temporary "staging table" is created that prepares the CDC records to stream into the target table, and then using DLT's apply_changes method, inserts, updates, and deletes are applied to the target Silver table base on the known natural keys.  
* For new file sources that aren't defined yet by the Data Architect, a Databricks SQL Alert is generated to let the Data Architect know to evaluate the new source data in its already available Bronze table.  Since the entire system is 100% streaming, no other intervention, restarts or refreshes are required other than adding a new DDL record and key to the version controlled data model.  Delta Live Tables takes care of the rest! 

***

The 'dlt-dropbox' project was generated by using the Databricks Asset Bundles default-python template.

## Getting started with Databricks Asset Bundles

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

3. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.
    For example, the default template would deploy a job called
    `[dev yourname] dlt_dropbox_job` to your workspace.
    You can find that job by opening your workpace and clicking on **Workflows**.

4. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```

   Note that the default job from the template has a schedule that runs every day
   (defined in resources/dlt_dropbox_job.yml). The schedule
   is paused when deploying in development mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

5. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```

6. Optionally, install developer tools such as the Databricks extension for Visual Studio Code from
   https://docs.databricks.com/dev-tools/vscode-ext.html.

7. For documentation on the Databricks asset bundles format used
   for this project, and for CI/CD configuration, see
   https://docs.databricks.com/dev-tools/bundles/index.html.
