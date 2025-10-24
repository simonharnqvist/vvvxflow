# vvvxflow

Data processing pipeline for VVVX dataset. Specifically, performs a conversion to array-valued columns, and then joining source and detection data. 

If stopped, the workflow will only process unprocessed partitions on restart, thanks to metadata tracking in a SparkSQL table.

### Installation
From this repo:
```bash
pip install git+https://github.com/simonharnqvist/vvvxflow
```

### Usage 

#### 1. Write a config file in TOML:
```toml
title = "VVVX pipeline config"

[run_configs]
logfile = "run_20251024_1409"
output_table_name = "run_20251024_1409"

[spark_warehouse]
path = "/mnt/vvvx-processed-data-ceph/spark-warehouse"
overwrite = true
db_name = "vvvx"

[parquet_paths]
detection = "/mnt/vvvx-source-data-ceph/JoinedQPPV/"
source = "/mnt/vvvx-source-data-ceph/vvvSrc5/"

[table_names]
source = "source"
detection = "detection"
source_detection = "source_detection"

[partitioning]
n_buckets = 8

[transform]
columns_to_array_value = [
        "mjd",
        "aperMag1",
        "aperMag1err",
        "aperMag2",
        "aperMag2err",
        "aperMag3",
        "aperMag3err",
        "errBits",
        "averageConf",
        "class",
        "classStat",
        "deprecated",
        "ppErrBits",
        "objID",
        "multiframeID",
        "extNum",
        "seqNum",
        "flag",
        "modelDistSecs",
]
```

### 2. Run pipeline
```python
from astroflow_spark_gaia import spark # or configure suitable Spark instance
from vvvxflow.transform_source_detection import pipeline

pipeline(config_file='config.toml', spark=spark)
```

This will write a logfile to the location specified in the config:
```log
2025-10-24 13:11:50 [INFO] Created db vvvx at /mnt/vvvx-processed-data-ceph/spark-warehouse/vvvx.db
2025-10-24 13:11:55 [INFO] Skipping 0 mods already processed.
2025-10-24 13:11:55 [INFO] Processing 1280 new mods...
2025-10-24 13:14:08 [INFO] Processed mod mod0493
2025-10-24 13:15:03 [INFO] Processed mod mod0149
2025-10-24 13:16:26 [INFO] Processed mod mod0966
2025-10-24 13:17:40 [INFO] Processed mod mod0404
2025-10-24 13:18:56 [INFO] Processed mod mod0528
2025-10-24 13:20:13 [INFO] Processed mod mod0737
2025-10-24 13:21:31 [INFO] Processed mod mod0365
2025-10-24 13:22:48 [INFO] Processed mod mod0748
2025-10-24 13:24:02 [INFO] Processed mod mod0563
2025-10-24 13:24:45 [INFO] Processed mod mod0274
2025-10-24 13:26:01 [INFO] Processed mod mod0233 

(stopped here)

2025-10-24 13:26:41 [INFO] Created db vvvx at /mnt/vvvx-processed-data-ceph/spark-warehouse/vvvx.db
2025-10-24 13:26:42 [INFO] Skipping 11 mods already processed.
2025-10-24 13:26:42 [INFO] Processing 1269 new mods...
```
Note that the re-creation of the database is a Spark quirk and doesn't mean that any data is overwritten.