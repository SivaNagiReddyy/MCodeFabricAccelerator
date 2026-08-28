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
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

run_id = ""  # @pipeline().RunId - same value every row in this run staged under
pipeline_name = "pl_medallion_orchestrator"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from delta.tables import DeltaTable

STAGING_LAKEHOUSE = "HYDRA_SILVER_LK"
staging_abfss = mssparkutils.lakehouse.get(STAGING_LAKEHOUSE)["properties"]["abfsPath"]
staging_path = f"{staging_abfss}/Tables/metadata/pipeline_run_staging"

if DeltaTable.isDeltaTable(spark, staging_path):
    rows = (spark.read.format("delta").load(staging_path)
            .filter(f"run_id = '{run_id}'")
            .collect())
else:
    rows = []

def sql_str(v):
    return "NULL" if v is None else "'" + str(v).replace("'", "''") + "'"

def sql_num(v):
    return "NULL" if v is None else str(v)

statements = []
for r in rows:
    statements.append(
        "EXEC metadata.usp_log_pipeline_run "
        f"@control_id = {r['control_id']}, "
        f"@run_id = {sql_str(r['run_id'])}, "
        f"@parent_run_id = {sql_str(r['run_id'])}, "
        f"@pipeline_name = {sql_str(pipeline_name)}, "
        f"@activity_name = {sql_str(r['activity_name'])}, "
        f"@start_time = {sql_str(r['start_time'])}, "
        f"@end_time = {sql_str(r['end_time'])}, "
        f"@status = {sql_str(r['status'])}, "
        f"@row_count = {sql_num(r['row_count'])}, "
        f"@error_message = {sql_str(r['error_message'])}, "
        f"@new_watermark = {sql_str(r['new_watermark'])};"
    )

batch_sql = "\n".join(statements) if statements else "SELECT 1;  -- nothing staged for this run_id"
print(f"{len(statements)} run(s) staged for run_id={run_id} - batched into one script")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(batch_sql)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
