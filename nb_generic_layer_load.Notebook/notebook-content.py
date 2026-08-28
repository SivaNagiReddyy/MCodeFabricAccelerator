# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "31dbc1ae-cdad-4fb1-803e-41bc959a88d2",
# META       "default_lakehouse_name": "HYDRA_SILVER_LK",
# META       "default_lakehouse_workspace_id": "53747e8d-990c-48d0-a357-52aa3cf64833",
# META       "known_lakehouses": [
# META         {
# META           "id": "31dbc1ae-cdad-4fb1-803e-41bc959a88d2"
# META         },
# META         {
# META           "id": "8d96b1a2-1764-46da-9ecf-05128fb61f2e"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

control_id           = ""# @item().control_id
run_id                = ""# @pipeline().RunId - same value passed to every row this run
sql_file_path          = ""# e.g. 'sql/silver/Customers/dim_lessee.sql'
sql_lakehouse           = ""# lakehouse the .sql file itself lives in, e.g. 'lh_sr_staging'
target_lakehouse         = ""# e.g. 'SMBCAC_SILVER_LH'
target_schema              = ""
target_table                 = ""
load_type                     = "Full"  # 'Full' | 'Incremental'
key_columns                    = ""     # comma-separated, required for Incremental
incremental_column               = ""   # e.g. 'DW_TimeStamp' - required for Incremental
watermark_value                    = ""# required for Incremental

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

STAGING_LAKEHOUSE = "HYDRA_SILVER_LK"  # fixed - where every domain's run results stage, regardless of this row's own target

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timezone
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, LongType, StringType
from delta.tables import DeltaTable

def now_iso():
    return datetime.now(timezone.utc).isoformat()

# Explicit schema - with only one row per write, Spark's type inference
# fails outright whenever any column is None (no non-null value to infer
# from, e.g. new_watermark on a Full load or error_message on success):
# PySparkValueError: [CANNOT_DETERMINE_TYPE]

STAGING_SCHEMA = StructType([
    StructField("control_id", LongType(), True),
    StructField("run_id", StringType(), True),
    StructField("activity_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("row_count", LongType(), True),
    StructField("error_message", StringType(), True),
    StructField("new_watermark", StringType(), True),
    StructField("logged_at", StringType(), True),
])

staging_abfss = mssparkutils.lakehouse.get("HYDRA_SILVER_LK")["properties"]["abfsPath"]
staging_path = f"{staging_abfss}/Tables/metadata/pipeline_run_staging"

if not DeltaTable.isDeltaTable(spark, staging_path):
    mssparkutils.fs.mkdirs(f"{staging_abfss}/Tables/metadata")
    spark.createDataFrame([], STAGING_SCHEMA).write.format("delta").mode("overwrite").save(staging_path)
    print("created empty staging table")
else:
    print("already exists, nothing to do")


def stage_result(status, row_count, error_message, new_watermark, start_time, end_time):
    staging_abfss = mssparkutils.lakehouse.get(STAGING_LAKEHOUSE)["properties"]["abfsPath"]
    staging_path = f"{staging_abfss}/Tables/metadata/pipeline_run_staging"
    mssparkutils.fs.mkdirs(f"{staging_abfss}/Tables/metadata")
    row = spark.createDataFrame([Row(
        control_id=int(control_id),
        run_id=run_id,
        activity_name="Load Silver Object",
        status=status,
        start_time=start_time,
        end_time=end_time,
        row_count=int(row_count) if row_count is not None else None,
        error_message=error_message,
        new_watermark=new_watermark,
        logged_at=now_iso(),
    )], schema=STAGING_SCHEMA)
    (row.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(staging_path))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_time = now_iso()

try:
    # ---- Read the complete file, as-is ----
    sql_abfss = mssparkutils.lakehouse.get(sql_lakehouse)["properties"]["abfsPath"]
    query = "\n".join(row.value for row in spark.read.text(f"{sql_abfss}/Files/{sql_file_path}").collect())

    # ---- Run it ----
    df = spark.sql(query)

    # ---- Apply the watermark filter (Incremental only) ----
    if load_type == "Incremental":
        df = df.filter(f"`{incremental_column}` > '{watermark_value}'")

    # ---- Store the result ----
    target_abfss = mssparkutils.lakehouse.get(target_lakehouse)["properties"]["abfsPath"]
    target_path = f"{target_abfss}/Tables/{target_schema}/{target_table}"
    mssparkutils.fs.mkdirs(f"{target_abfss}/Tables/{target_schema}")

    if load_type == "Full" or not DeltaTable.isDeltaTable(spark, target_path):
        # Column Mapping required: our column names contain spaces (e.g.
        # "Lessee ID NK"), which plain Delta rejects
        # (DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES) unless this is set at
        # table creation.
        (df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("delta.columnMapping.mode", "name")
            .option("delta.minReaderVersion", "2")
            .option("delta.minWriterVersion", "5")
            .save(target_path))
    else:
        merge_condition = " AND ".join(
            f"t.`{c.strip()}` = s.`{c.strip()}`" for c in key_columns.split(",")
        )
        (DeltaTable.forPath(spark, target_path).alias("t")
            .merge(df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())

    # ---- Row count + watermark, both from a fresh post-write read of the
    #      target (not the write-time dataframe) ----
    target_df = spark.read.format("delta").load(target_path)
    row_count = target_df.count()

    new_watermark = None
    if load_type == "Incremental":
        new_watermark = target_df.agg({incremental_column: "max"}).collect()[0][0]
        new_watermark = str(new_watermark) if new_watermark is not None else None

    end_time = now_iso()
    stage_result("Succeeded", row_count, None, new_watermark, start_time, end_time)
    print(f"{sql_file_path} -> {target_lakehouse}.{target_schema}.{target_table}: {row_count} rows ({load_type}), new_watermark={new_watermark}")

except Exception as e:
    end_time = now_iso()
    stage_result("Failed", None, str(e), None, start_time, end_time)
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
