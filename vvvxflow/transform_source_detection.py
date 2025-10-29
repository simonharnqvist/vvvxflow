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

def transform_single_mod(mod: str, table_name: str, cols_to_transform: list[str], order_by:str, source_path: str, detection_path: str, warehouse_path: str, db_name: str, spark: SparkSession):
    """Transform single modulus partition and load into target table"""
    
    detection = spark.read.parquet(str(Path(detection_path).joinpath(mod)))
    source = spark.read.parquet(str(Path(source_path).joinpath(mod)))
    
    detection_array_valued = make_array_cols(detection, key="sourceID", filter_col="filterID", 
                                             order_by=order_by, cols_to_transform=cols_to_transform)
    
    joined = source.join(detection_array_valued, on="sourceID")
    
    joined.createOrReplaceTempView("temp_view")
    
    spark.sql(f"USE {db_name}")
    spark.sql(f"""
            INSERT INTO {table_name}
            SELECT *
            FROM temp_view
        """)



def transform_mods(mods: list[str], table_name: str, schema: StructField, cols_to_transform: list[str], order_by: str, source_path: str, detection_path: str, warehouse_path: str, db_name: str, spark: SparkSession, logger: logging.Logger):
    """Iterate over mod partitions and transform each"""

    # Create database if not exists
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {db_name}
        LOCATION '{Path(warehouse_path).joinpath(db_name + ".db")}'
    """)

    logger.info(f"Created db {db_name} at {Path(warehouse_path).joinpath(db_name + '.db')}")

    # Use the database
    spark.sql(f"USE {db_name}")
    
    if not spark.catalog.tableExists(table_name):
        shutil.rmtree(Path(warehouse_path).joinpath(db_name + ".db").joinpath(table_name), ignore_errors=True)
        empty_df = spark.createDataFrame([], schema=schema)
        empty_df.write.format("parquet").mode("overwrite").saveAsTable(table_name)

    # Create processed mods tracking table if it doesn't exist
    processed_table = table_name + "_mods_processed"
    if not spark.catalog.tableExists(processed_table):
        shutil.rmtree(Path(warehouse_path).joinpath(db_name + ".db").joinpath(processed_table), ignore_errors=True)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {processed_table} (
                mod STRING,
                processed_at TIMESTAMP
            )
            USING PARQUET
        """)

    # Get list of already processed mods from metadata table
    processed_mods_df = spark.table(processed_table).select("mod")
    processed_mods = [row.mod for row in processed_mods_df.collect()]

    # Filter mods to process only those not processed yet
    mods_to_process = [m for m in mods if m not in processed_mods]

    logger.info(f"Skipping {len(mods) - len(mods_to_process)} mods already processed.")
    logger.info(f"Processing {len(mods_to_process)} new mods...")

    # Process new mods
    for mod in tqdm.tqdm(mods_to_process):
        logger.info(f"Processing {mod}...")
        transform_single_mod(mod=mod, table_name=table_name, 
                             cols_to_transform=cols_to_transform, 
                             order_by = order_by,
                             source_path=source_path, detection_path=detection_path,
                             warehouse_path=warehouse_path, db_name=db_name, 
                             spark=spark)

        # Record this mod as processed in the tracking table
        spark.sql(f"""
            INSERT INTO {processed_table} VALUES ('{mod}', current_timestamp())
        """)

        logger.info(f"done")

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
        


    

