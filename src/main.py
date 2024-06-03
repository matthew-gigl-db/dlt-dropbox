import dlt
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from pyspark.sql.streaming import DataStreamReader, DataStreamWriter
from typing import Callable
import os
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import *

##########################
####### operations #######
##########################

# set the catalog and schema 
def use_catalog_schema(catalog: str, schema: str, env_mode: str = "dev", verbose: bool = False):
    if env_mode == "prd":
        catalog_stmnt = f"""use catalog {catalog};"""
    else:
        catalog_stmnt = f"""use catalog {catalog}_{env_mode};"""
    
    spark.sql(catalog_stmnt)
    spark.sql(f"""use schema {schema};""")
    if verbose:
        return spark.sql("""select current_catalog(), current_schema();""")

# read streaming data as whole text using autoloader    
def read_stream_raw(spark: SparkSession, path: str, maxFiles: int, maxBytes: str, wholeText: bool = False, skipRows: int = 0, options: dict = None) -> DataFrame:
    stream_schema = "value STRING"
    read_stream = (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholetext", wholeText)
        .option("cloudFiles.maxBytesPerTrigger", maxBytes)
        .option("cloudFiles.maxFilesPerTrigger", maxFiles)
        .option("skipRows", skipRows)
    )

    if options is not None:
        read_stream = read_stream.options(**options)

    read_stream = (
        read_stream
        .schema(stream_schema)
        .load(path)
    )

    return read_stream

# read streaming data as whole text using autoloader    
def read_stream_csv(spark: SparkSession, path: str, maxFiles: int, maxBytes: str, schema: str = None, skipRows: int = 0, header: bool = True, delimiter: str = ",", options: dict = None) -> DataFrame:
    
    if schema is not None:
        stream_schema = schema
    else:
        stream_schema = "value STRING"
        delimiter = "~"
    
    read_stream = (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("schema", schema)
        .option("cloudFiles.maxBytesPerTrigger", maxBytes)
        .option("cloudFiles.maxFilesPerTrigger", maxFiles)
        .option("skipRows", skipRows)
        .option("header", header)
        .option("delimiter", delimiter)
    )

    if options is not None:
        read_stream = read_stream.options(**options)

    read_stream = (
        read_stream
        .schema(stream_schema)
        .load(path)
    )

    return read_stream

def get_absolute_path(*relative_parts):
    if 'dbutils' in globals():
        base_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()) # type: ignore
        path = os.path.normpath(os.path.join(base_dir, *relative_parts))
        return path if path.startswith("/Workspace") else "/Workspace" + path
    else:
        return os.path.join(*relative_parts)
    
def retrieve_ddl_files(ddl_path: str, workspace_client: WorkspaceClient) -> pd.DataFrame:
  """Retrieve all ddl files from the given path and load into a pandas dataframe."""
  ddl_files = [file.as_dict() for file in workspace_client.workspace.list(path = ddl_path)]
  for ddl_file in ddl_files:
    path = ddl_file.get("path")
    ddl_file["table_name"] = path.split("/")[-1].replace(".ddl", "")
    with open(path, 'r') as file:
      ddl_file["ddl"] = file.read()
  return pd.DataFrame(ddl_files)

def recursive_ls(path):
    files = []
    for item in dbutils.fs.ls(path):
        if item.isDir():
            files.extend(recursive_ls(item.path))
        else:
            files.append(item.path)
    return files

# flatten struct columns in a spark data frame
def explode_and_split(df):
  newDF = df
  for colName in df.columns:
    colType = df.schema[colName].dataType
    if isinstance(colType, ArrayType):
      newDF = newDF.withColumn(colName, explode_outer(col(colName)))
      # newDF = explodeAndSplit(newDF)  # Recurse if column is an array
    elif isinstance(colType, StructType):
      for field in colType.fields:
          fieldName = field.name
          newDF = newDF.withColumn(f"{fieldName}", col(f"{colName}.{fieldName}"))
      newDF = newDF.drop(colName)
      # newDF = explodeAndSplit(newDF)  # Recurse if column is a struct
  return newDF




###########################
### classes and methods ###
###########################

class IngestionDLT:

    def __init__(
        self
        ,spark: SparkSession # = spark
        ,volume: str
    ):
        self.spark = spark
        self.volume = volume

    def __repr__(self):
        return f"""IngestionDLT(volume='{self.volume}')"""

    def ingest_raw_to_bronze(self, table_name: str, table_comment: str, table_properties: dict, source_folder_path_from_volume: str = "", maxFiles: int = 1000, maxBytes: str = "10g", wholeText: bool = False, csv_schema: str = None, skipRows: int = 0, header: bool = True, delimiter: str = ",", file_type: str = "csv", options: dict = None):
        """
        Ingests all files in a volume's path to a key value pair bronze table.
        """
        @dlt.table(
            name = table_name
            ,comment = table_comment
            ,temporary = False
            ,table_properties = table_properties

        )
        def bronze_ingestion(spark = self.spark, source_folder_path_from_volume = source_folder_path_from_volume, maxFiles = maxFiles, maxBytes = maxBytes, wholeText = wholeText, skipRows = skipRows, options = options, volume = self.volume):

            if source_folder_path_from_volume == "":
                file_path = f"{volume}/"
            else:
                file_path = f"{volume}/{source_folder_path_from_volume}/"

            if file_type == "csv":
                raw_df = read_stream_csv(spark = spark, path = file_path, maxFiles = maxFiles, maxBytes = maxBytes, schema = csv_schema, skipRows = skipRows, header = header, delimiter = delimiter, options = options)
            else: 
                raw_df = read_stream_raw(spark = spark, path = file_path, maxFiles = maxFiles, maxBytes = maxBytes, wholeText = wholeText, options = options)

            bronze_df = (raw_df
                .withColumn("inputFilename", col("_metadata.file_name"))
                .withColumn("fullFilePath", col("_metadata.file_path"))
                .withColumn("fileMetadata", col("_metadata"))
                .select(
                    "fullFilePath"
                    ,lit(file_path).alias("datasource")
                    ,"inputFileName"
                    ,current_timestamp().alias("ingestTime")
                    ,current_timestamp().cast("date").alias("ingestDate")
                    ,"value"
                    ,"fileMetadata"
                )
            )

            return bronze_df
    
    ### Ingest from multiple subfolders of the same Volume into one bronze table.
    def ingest_raw_to_bronze_synchronous(self, table_names: list, table_comments: list, table_properties: dict, source_folder_path_from_volumes: str, maxFiles: int = 1000, maxBytes: str = "10g", wholeText: bool = True, skipRows: int = 0, options: dict = None):
        """
            Synchronously ingest from multiple subfolders of the same Volume into more than one bronze table.  Each bronze table created is managed as a streaming Delta Live Table in the same <catalog.schema> as the source volume.  
        """
        for i in range(0,len(table_names)):
            ingest_raw_to_bronze(self = self, table_name = table_names[i], table_comment = table_comments[i], source_folder_path_from_volume = source_folder_path_from_volumes[i], table_properties = table_properties, maxFiles = maxFiles, maxBytes = maxBytes, wholeText = wholeText, skipRows = skipRows, options = options)

    ### List the file names loaded into bronze.  We'll use this to determine if new files have arrived.  
    def list_dropbox_files(self, bronze_table: str): 
        @dlt.table(
            name = "temp_landed_bronze_files"
            ,comment = "Temporary table containing the distinct filename types loaded from the dropbox."
            ,temporary = True
            ,table_properties = None
        )
        def temp_bronze_files(spark = self.spark, bronze_table = bronze_table):
            return (spark.sql(f"""select distinct inputFileName from LIVE.{bronze_table}"""))

    ### Split Bronze Table Into Multiple Bronze Tables   
    def split_bronze_table(self, bronze_table: str, filename: str, table_name: str):
        """
            This method assumes that files dropped in the same landing volume with similiar filenames are of the same schema and should be moved to their own bronze table for further processing.  Note that use of this method is optional for the pipeline and works best when the files have the same name but are organzied by folders to differentiate dates arrived (typically paritions).  Do not use if every file name is unique.    
        """
        dlt.create_streaming_table(
            name = f"{table_name}_bronze"
            ,comment = f"Bronze table of every {filename} landed from associated {bronze_table}."
            # ,spark_conf={"<key>" : "<value", "<key" : "<value>"}
            # ,table_properties={"<key>" : "<value>", "<key>" : "<value>"}
            ,table_properties = None 
            # ,partition_cols=["<partition-column>", "<partition-column>"]
            ,partition_cols = None
            # ,path="<storage-location-path>"
            # ,schema="schema-definition"
            # ,expect_all = {"<key>" : "<value", "<key" : "<value>"}
            # ,expect_all_or_drop = {"<key>" : "<value", "<key" : "<value>"}
            # ,expect_all_or_fail = {"<key>" : "<value", "<key" : "<value>"}
        )

        @dlt.append_flow(
            target = f"{table_name}_bronze"
            ,name = f"{table_name}_bronze_append_flow"
            # ,spark_conf = {"<key>" : "<value", "<key" : "<value>"}
            # ,comment = "<comment>"
        ) 
        def split_bronze(spark = self.spark, bronze_table = bronze_table, filename = filename): 
            return spark.readStream.table(f"LIVE.{bronze_table}").where(col("inputFileName") == lit(filename))
        
    ### Load DDL Files From Workspace 
    def load_ddl_files(self, ddl_df: pd.DataFrame):
        @dlt.table(
            name = "synthea_silver_schemas"
            ,comment = "Reference table containing the schema definitions for the Synthea csv file datasets."
            ,temporary = False
            ,table_properties = {
            "pipelines.autoOptimize.managed" : "true"
            ,"pipelines.reset.allowed" : "true"}
        )
        def synthea_schemas():
            return self.spark.createDataFrame(ddl_df)
        
    def stage_silver(self, bronze_table: str, table_name: str, ddl: str):
        @dlt.table(
            name = f"{table_name}_stage"
            ,comment = "Staging Table for data to stage into silver. Normally temporary."
            ,temporary = False
            ,table_properties = {
            "pipelines.autoOptimize.managed" : "true"
            ,"pipelines.reset.allowed" : "true"}
        )
        def stage_silver_synthea():
            sdf = (
                spark.readStream.table(f"{bronze_table}")
                .withColumn("sequence_by", col("fileMetadata.file_modification_time"))
                .withColumn("data", from_csv(col("value"), schema=ddl)).alias("data")
            )
            return explode_and_split(sdf)

  
    ## stream changes into target silver table
    def stream_silver(self, bronze_table: str, table_name: str, sequence_by: str, keys: list, schema: str = None):
        # create the target table
        dlt.create_streaming_table(
            name = table_name
            ,comment = f"Silver database table created from ingested source data from associated {bronze_table} table."
            # ,spark_conf={"<key>" : "<value", "<key" : "<value>"}
            # ,table_properties={"<key>" : "<value>", "<key>" : "<value>"}
            ,table_properties = None 
            # ,partition_cols=["<partition-column>", "<partition-column>"]
            ,partition_cols = None
            # ,path="<storage-location-path>"
            # ,schema = ddl
            # ,expect_all = {"<key>" : "<value", "<key" : "<value>"}
            # ,expect_all_or_drop = {"<key>" : "<value", "<key" : "<value>"}
            # ,expect_all_or_fail = {"<key>" : "<value", "<key" : "<value>"}
        )

        # now apply changes 
        dlt.apply_changes(
            target = table_name
            ,source =  f"{table_name}_stage"
            ,keys = keys
            ,sequence_by = sequence_by
            ,ignore_null_updates = True
            ,apply_as_deletes = None
            ,apply_as_truncates = None
            ,column_list = None
            ,except_column_list = ["fullFilePath", "datasource", "inputFileName", "ingestTime", "ingestDate", "value", "sequence_by", "file_path", "file_name", "file_size", "file_block_start", "file_block_length", "file_modification_time"]
            ,stored_as_scd_type = "1"
            ,track_history_column_list = None
            ,track_history_except_column_list = None
        )

#################
        
        

        
            












