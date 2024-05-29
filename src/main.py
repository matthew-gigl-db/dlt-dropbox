import dlt
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.streaming import DataStreamReader, DataStreamWriter
from typing import Callable

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
def read_stream_raw(spark: SparkSession, path: str, maxFiles: int, maxBytes: str, wholeText: bool = True, options: dict = None) -> DataFrame:
    stream_schema = "value STRING"
    read_stream = (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholetext", wholeText)
        .option("cloudFiles.maxBytesPerTrigger", maxBytes)
        .option("cloudFiles.maxFilesPerTrigger", maxFiles)
    )

    if options is not None:
        read_stream = read_stream.options(**options)

    read_stream = (
        read_stream
        .schema(stream_schema)
        .load(path)
    )

    return read_stream

###########################
### classes and methods ###
###########################

class IngestionDLT:

    def __init__(
        self
        ,spark: SparkSession # = spark
        ,env_mode: str = "dev"
        ,catalog: str = "lakehouse"
        ,schema: str = "landing"
        ,volume: str = "dropbox"
    ):
        self.spark = spark
        self.env_mode = env_mode
        self.catalog = catalog
        self.schema = schema
        self.volume = volume

        # use_catalog_schema(catalog = self.catalog, schema = self.schema, env_mode = self.env_mode, verbose = False)
        if self.env_mode == "prd":
            self.catalog_set = self.catalog
        else:
            self.catalog_set = f"{self.catalog}_{self.env_mode}"

    def __repr__(self):
        return f"""IngestionDLT(env_mode='{self.env_mode}', catalog='{self.catalog_set}', schema='{self.schema}', volume='{self.volume}')"""

    def ingest_raw_to_bronze(self, table_name: str, table_comment: str, table_properties: dict, source_folder_path_from_volume: str = "", maxFiles: int = 1000, maxBytes: str = "10g", wholeText: bool = True, options: dict = None):
        """
        Ingests all files in a volume's path to a key value pair bronze table.
        """
        @dlt.table(
            name = table_name
            ,comment = table_comment
            ,temporary = False
            ,table_properties = table_properties

        )
        def bronze_ingestion(spark = self.spark, source_folder_path_from_volume = source_folder_path_from_volume, maxFiles = maxFiles, maxBytes = maxBytes, wholeText = wholeText, options = options, catalog = self.catalog_set, schema = self.schema, volume = self.volume):

            if source_folder_path_from_volume == "":
                file_path = f"/Volumes/{catalog}/{schema}/{volume}/"
            else:
                file_path = f"/Volumes/{catalog}/{schema}/{volume}/{source_folder_path_from_volume}/"

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
        
    def ingest_raw_to_bronze_synchronous(self, table_names: list, table_comments: list, table_properties: dict, source_folder_path_from_volumes: str, maxFiles: int = 1000, maxBytes: str = "10g", wholeText: bool = True, options: dict = None):
        """
            Synchronously ingest from multiple subfolders of the same Volume into more than one bronze table.  Each bronze table created is managed as a streaming Delta Live Table in the same <catalog.schema> as the source volume.  
        """
        for i in range(0,len(table_names)):
            ingest_raw_to_bronze(self = self, table_name = table_names[i], table_comment = table_comments[i], source_folder_path_from_volume = source_folder_path_from_volumes[i], table_properties = table_properties, maxFiles = maxFiles, maxBytes = maxBytes, wholeText = wholeText, options = options)

    def list_dropbox_files(self, bronze_table: str): 
        """
            This method assumes that files dropped in the same landing volume with similiar filenames are of the same schema and should be moved to their own bronze table for further processing.  Note that use of this method is optional for the pipeline and works best when the files have the same name but are organzied by folders to differentiate dates arrived (typically paritions).  Do not use if every file name is unique.    
        """
        @dlt.table(
            name = "temp_landed_bronze_files"
            ,comment = "Temporary table containing the distinct filename types loaded from the dropbox."
            ,temporary = True
            ,table_properties = None
        )
        def temp_bronze_files(spark = self.spark, bronze_table = bronze_table):
            return (spark.sql(f"""select distinct inputFileName from LIVE.{bronze_table}"""))
        
    def split_bronze_table(self, bronze_table: str, filename: str, table_name: str, live: bool = True):
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
            ,name = f"{table_name}_bronze_append_flow" # optional, defaults to function name
            # ,spark_conf = {"<key>" : "<value", "<key" : "<value>"} # optional
            # ,comment = "<comment>" # optional
        ) 
        def split_bronze(spark = self.spark, bronze_table = bronze_table, filename = filename): 
            # return dlt.streaming_read(bronze_table).where(col("inputFileName") == lit(filename))
            # return (dlt.read(bronze_table).where(col("inputFileName") == lit(filename)))
            return spark.readStream.table(f"LIVE.{bronze_table}").where(col("inputFileName") == lit(filename))

        # @dlt.table(
        #     name = f"{table_name}_bronze"
        #     ,comment = f"Bronze table of every {filename} landed from associated {bronze_table}."
        #     ,temporary = False
        #     ,table_properties = None # Note-- this should be inherited from the source bronze table.
        # )
        # def split_bronze(spark = self.spark, bronze_table = bronze_table, filename = filename): 
        #     if live == True:  
        #         return (dlt.read(bronze_table).where(col("inputFileName") == lit(filename)))
        #     # (spark.sql(f"""select * from LIVE.{bronze_table} where inputFileName = '{filename}'"""))
        #     else:
        #         return (spark.sql(f"""select * from {self.catalog_set}.{self.schema}.{bronze_table} where inputFileName = '{filename}'"""))
        
    def split_bronze_table_synchronous(self, bronze_table: str, live: bool = True):
        
        if live == True:  
            filenames = self.spark.sql(f"select distinct * from LIVE.temp_landed_bronze_files").collect()
        else:  
            filenames = self.spark.sql(f"select distinct * from {self.catalog_set}.{self.schema}.temp_landed_bronze_files").collect()
        
        filenames_list = [row.inputFileName for row in filenames]
            
        for filename in filenames_list:
            name = filename.replace(".", "_")
            self.split_bronze_table(bronze_table = bronze_table, filename = filename, table_name = name, live = live)
        
        

        
            












