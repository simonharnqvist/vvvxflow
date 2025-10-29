from pathlib import Path
from logging import get_logger
import tomllib

from vvvxflow.array_columns import make_array_cols
from vvvxflow.validate import validate


def transform_single_mod(mod: str, table_name: str, cols_to_transform: list[str], source_path: str, detection_path: str, warehouse_path: str, db_name: str, spark: SparkSession):
    """Transform single modulus partition and load into target table"""
    
    detection = spark.read.parquet(str(Path(detection_path).joinpath(mod)))
    source = spark.read.parquet(str(Path(source_path).joinpath(mod)))
    
    detection_array_valued = make_array_cols(detection, key="sourceID", filter_col="filterID", 
                                             cols_to_transform=cols_to_transform, order_by = "sourceID")
    
    joined = source.join(detection_array_valued, on="sourceID")
    
    shutil.rmtree(Path(warehouse_path).joinpath("temp_table"), ignore_errors=True)
    joined.write.mode("overwrite").saveAsTable("temp_table")
                  
    spark.sql(f"USE {db_name}")
    spark.sql(f"""
            INSERT INTO {table_name}
            SELECT *
            FROM temp_table
        """)


def transform_mods(mods: list[str], table_name: str, cols_to_transform: list[str], source_path: str, detection_path: str, warehouse_path: str, db_name: str, spark: SparkSession):
    """Iterate over mod partitions and transform each"""

    # Create database if not exists
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {db_name}
        LOCATION '{Path(warehouse_path).joinpath(db_name + ".db")}'
    """)

    # Use the database
    spark.sql(f"USE {db_name}")
    
    if not spark.catalog.tableExists(table_name):
        empty_df = spark.createDataFrame([], schema=schema)
        empty_df.write.format("parquet").mode("overwrite").saveAsTable(table_name)

    # Create processed mods tracking table if it doesn't exist
    processed_table = table_name + "_mods_processed"
    if not spark.catalog.tableExists(processed_table):
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

    print(f"Skipping {len(mods) - len(mods_to_process)} mods already processed.")
    print(f"Processing {len(mods_to_process)} new mods...")

    # Process new mods
    for mod in tqdm.tqdm(mods_to_process):
        transform_single_mod(mod=mod, table_name=table_name, cols_to_transform=cols_to_transform, source_path=source_path, detection_path=detection_path, warehouse_path=warehouse_path, db_name=db_name, spark=spark)

        # Record this mod as processed in the tracking table
        spark.sql(f"""
            INSERT INTO {processed_table} VALUES ('{mod}', current_timestamp())
        """)

    print("Processing complete.")


def get_mods(detection_path: str, source_path: str) -> list[str]:
    """Get list of mods that are available from both source and detection"""
    detection_mods = [modpath.split("/")[-1] for modpath in glob.glob(detection_path)]
    source_mods = [modpath.split("/")[-1] for modpath in glob.glob(source_path)]
    mods = list(set(detection_mods).intersection(set(source_mods)))
    return mods


def pipeline(config_file: str = "config.toml", spark: SparkSession = None) -> None:
    with open(config_file, "rb") as f:
        configs = tomllib.load(f)

    if not spark:
        spark = (
            SparkSession.builder
            .appName(f"ScienceArchives_{db_name}")
            .config("spark.sql.warehouse.dir", warehouse_path)
            .enableHiveSupport()
            .getOrCreate()
        )
        return spark

    
    logger = get_logger(config["logfile"])

    mods = get_mods(detection_path = config["parquet_paths"]["detection"],
                    source_path = config["parquet_paths"]["source"])

    transform_mods(
        mods = mods,
        table_name = config["run_configs"]["table_name"],
        cols_to_transform = config["transform"]["columns_to_array_value"],
        source_path = config["parquet_paths"]["source"],
        detection_path = config["parquet_paths"]["detection"],
        warehouse_path = config["spark-warehouse"]["path"],
        db_name = config["spark-warehouse"]["db-name"],
        spark = spark)

    validate(logger = logger, config = config)
        


    

