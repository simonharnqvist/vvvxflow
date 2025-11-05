from pathlib import Path
import tomllib
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField
import glob
import shutil
import tqdm
import logging
from pyspark.sql import functions as F

from vvvxflow.array_columns import make_array_cols
#from vvvxflow.validate import validate
from vvvxflow.log import get_logger
from vvvxflow.schema_joined_source_detection import schema_joined_source_detection

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pathlib import Path
import logging

logger = logging.getLogger(__name__)
spark = SparkSession.builder.getOrCreate()

def create_or_use_tables(table_name: str, db_name: str, warehouse_path: str, schema: StructField, logger: logging.Logger):
    """
    Ensure that a database and both target and metadata tables exist.
    If parquet backing doesn't exist, create it.
    """

    # Create DB if needed
    db_path = Path(warehouse_path).joinpath(f"{db_name}.db")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{db_path}'")
    logger.info(f"Ensured database {db_name} exists at {db_path}")

    metadata_table_name = f"{table_name}_metadata"

    # Table locations
    table_path = db_path.joinpath(table_name)
    metadata_path = db_path.joinpath(metadata_table_name)

    # Use Hadoop FS to check for parquet existence (works on S3, ADLS, etc.)
    hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    def exists(path: Path):
        return hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(str(path)))

    # Check table existence
    table_exists = spark.catalog.tableExists(f"{db_name}.{table_name}")
    metadata_exists = spark.catalog.tableExists(f"{db_name}.{metadata_table_name}")

    # ---- Target Table ----
    if not table_exists and exists(table_path):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {db_name}.{table_name} USING PARQUET LOCATION '{table_path}'")
        logger.info(f"Registered existing parquet-backed table {db_name}.{metadata_table_name} at {table_path}")
    elif not table_exists and not exists(table_path):
        logger.info(f"Creating new parquet-backed table {db_name}.{table_name} at {table_path}")
        empty_df = spark.createDataFrame([], schema=schema)
        empty_df.write.mode("overwrite").format("parquet").saveAsTable(f"{db_name}.{table_name}")
    else:
        logger.info(f"Table {db_name}.{metadata_table_name} already exists — skipping creation")

    # ---- Metadata Table ----
    if not metadata_exists and exists(metadata_path):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {db_name}.{metadata_table_name} USING PARQUET LOCATION '{metadata_path}'")
        logger.info(f"Registered existing metadata table {db_name}.{metadata_table_name} at {metadata_path}")
    elif not metadata_exists and not exists(metadata_path):
        logger.info(f"Creating metadata table {db_name}.{metadata_table_name} at {metadata_path}")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {db_name}.{metadata_table_name} (
                mod STRING,
                timestamp TIMESTAMP
            )
            USING PARQUET
            LOCATION '{metadata_path}'
        """)
    else:
        logger.info(f"Metadata table {db_name}.{metadata_table_name} already exists — skipping creation")

    

def transform_single_mod(mod: str, table_name: str, cols_to_transform: list[str], order_by:str, source_path: str, detection_path: str, warehouse_path: str, db_name: str, spark: SparkSession):
    """Transform single modulus partition and load into target table"""
    
    detection = spark.read.parquet(str(Path(detection_path).joinpath(mod)))
    source = spark.read.parquet(str(Path(source_path).joinpath(mod)))
    
    detection_array_valued = make_array_cols(detection, key="sourceID", filter_col="filterID", 
                                             order_by=order_by, cols_to_transform=cols_to_transform)
    
    joined = source.join(detection_array_valued, on="sourceID")
    
    joined.createOrReplaceTempView("temp_view")
    
    spark.sql(f"""
            INSERT INTO {db_name}.{table_name}
            SELECT *
            FROM temp_view
        """)



def transform_mods(mods: list[str], table_name: str, schema: StructField, cols_to_transform: list[str], order_by: str, source_path: str, detection_path: str, warehouse_path: str, db_name: str, spark: SparkSession, logger: logging.Logger):
    """Iterate over mod partitions and transform each"""

    create_or_use_tables(table_name = table_name, db_name = db_name, warehouse_path = warehouse_path, schema = schema, logger = logger)

    # Skip mods already processed
    metadata_table = table_name + "_metadata"
    processed = spark.sql(f"SELECT mod FROM {db_name}.{metadata_table}")
    processed_mods = [row.mod for row in processed.collect()]
    mods_to_process = [m for m in mods if m not in processed_mods]

    logger.info(f"Skipping {len(mods) - len(mods_to_process)} mods already processed.")
    logger.info(f"Processing {len(mods_to_process)} new mods...")

    # Process new mods
    for mod in tqdm.tqdm(mods_to_process):
        logger.info(f"Processing {mod}")
        transform_single_mod(mod=mod, table_name=table_name, 
                             cols_to_transform=cols_to_transform, 
                             order_by = order_by,
                             source_path=source_path, detection_path=detection_path,
                             warehouse_path=warehouse_path, db_name=db_name, 
                             spark=spark)

        # Record this mod as processed in the tracking table
        spark.sql(f"""
            INSERT INTO {db_name}.{metadata_table} VALUES ('{mod}', current_timestamp())
        """)

    logger.info("Processing complete.")


def get_mods(detection_path: str, source_path: str) -> list[str]:
    """Get list of mods that are available from both source and detection"""
    detection_mods = [modpath.split("/")[-1] for modpath in glob.glob(str(Path(detection_path).joinpath("*")))]
    source_mods = [modpath.split("/")[-1] for modpath in glob.glob(str(Path(source_path).joinpath("*")))]
    mods = list(set(detection_mods).intersection(set(source_mods)))
    return mods

def pipeline(config_file: str = "config.toml", spark: SparkSession = None) -> None:
    with open(config_file, "rb") as f:
        config = tomllib.load(f)


    logger = get_logger(log_file = config["run_configs"]["logfile"])

    if not spark:
        spark = (
            SparkSession.builder
            .appName(f"ScienceArchives_{db_name}")
            .config("spark.sql.warehouse.dir", warehouse_path)
            .enableHiveSupport()
            .getOrCreate()
        )
        return spark

    if "mods" not in config["run_configs"]:
        mods = get_mods(detection_path = config["parquet_paths"]["detection"],
                    source_path = config["parquet_paths"]["source"])
    else:
        mods = config["run_configs"]["mods"]
    
    schema = schema_joined_source_detection
    
    transform_mods(
        mods = mods,
        table_name = config["run_configs"]["output_table_name"],
        schema = schema,
        cols_to_transform = config["transform"]["columns_to_array_value"],
        order_by = config["transform"]["order_by"],
        source_path = config["parquet_paths"]["source"],
        detection_path = config["parquet_paths"]["detection"],
        warehouse_path = config["spark_warehouse"]["path"],
        db_name = config["spark_warehouse"]["db_name"],
        spark = spark,
        logger = logger)

    #validate(logger = logger, config = config)
        


    

