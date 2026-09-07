# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ## nb_m_to_sql  -  Agent 2b: build plan -> Spark SQL + control-table seed
#
# Second of the two M -> Spark SQL agents. It consumes the **complete analysis**
# written by **nb_m_analyze** (Agent 2a):
#
#   - `Files/analysis/analysis.json`        - the merged, authoritative build plan
#   - `Files/analysis/queries/<Query>.json` - the per-M-code analysis, one per M file
#   - `Files/m_extract/<Query>.m`           - the original M, for exact expressions
#
# An LLM agent (**deepagents** + the gateway LLM via **nb_bedrock**) turns each
# `tables[]` entry into a single **`spark.sql()`-executable `SELECT`**:
#
#   - it does **not** re-classify layers or invent tables - the analysis decides
#     `layer`, `target_table`, `reads_from`, `output_columns`, `grain`, ...,
#   - gold statements read silver only; silver split tables read Bronze.
#
# The plan is re-validated on load (duplicate names, missing columns, and the
# gold-never-reads-Bronze rule) so a hand-edited `analysis.json` is still safe.
#
# Outputs, under `<out_lakehouse>/<sql_out_subdir>/`:
#   - `silver/<t>.sql`, `gold/<t>.sql`   (consumed as-is by nb_generic_layer_load)
#   - `layering.md`
#   - `pipeline_control.csv` + `pipeline_control_insert.sql` - one row per .sql,
#     taken **straight from each entry's `control` block**, ready for
#     `metadata.pipeline_control`
#
# ### Portability
# Medallion targets come from `analysis.json`; every lakehouse is resolved by
# name at run time. Deploy into any workspace, point the params at its lakehouses.
#
# ### Prerequisites
# `nb_m_analyze` has run; **py-packages** attached (`deepagents`,
# `langchain-anthropic`); **nb_bedrock** in the same workspace with its
# `llm_config.json` written once (see nb_bedrock).

# PARAMETERS CELL ********************

# --- inputs -----------------------------------------------------------------
analysis_lakehouse = "HYDRA_BRONZE_LK"   # lakehouse holding analysis.json (from nb_m_analyze)
analysis_subdir    = "Files/analysis"
m_lakehouse        = "HYDRA_BRONZE_LK"   # lakehouse holding the extracted M files
m_subdir           = "Files/m_extract"

# --- outputs ------------------------------------------------------------
out_lakehouse  = "HYDRA_BRONZE_LK"       # lakehouse to write the .sql + csv into
sql_out_subdir = "Files/sql"             # -> Files/sql/silver/*.sql , Files/sql/gold/*.sql , Files/sql/pipeline_control.csv

# --- medallion target overrides (blank = take from analysis.json) ------
bronze_lakehouse = ""
bronze_schema    = ""
silver_lakehouse = ""
silver_schema    = ""
gold_lakehouse   = ""
gold_schema      = ""

# --- control-table seed -----------------------------------------------
# layer / domain / target_* / load_type / key_columns / batch_group all come from
# each plan entry's `control` block (built by nb_m_analyze). Only this override:
control_sql_lakehouse = ""               # lakehouse the .sql files live in (blank = the plan's value)

# --- LLM: resolved by nb_bedrock from the saved llm_config.json -----
llm_config_lakehouse = "HYDRA_BRONZE_LK"          # lakehouse holding Files/config/llm_config.json
llm_config_path      = "Files/config/llm_config.json"
llm_provider         = ""        # "" = use the config file; else "anthropic" | "openai"
llm_base_url         = ""        # "" = use the config file
llm_api_key          = ""        # "" = use the config file (keep blank - never commit a key)
llm_model            = ""        # "" = use the config file
temperature          = 0
max_tokens           = 8192
recursion_limit      = 200

# --- misc -----------------------------------------------------------
run_explain_check = True
overwrite         = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 1. load the COMPLETE analysis + the M source -------------------
# analysis.json  = the merged, flattened build plan (with a `control` block per table)
# queries/*.json = nb_m_analyze's per-M-query analysis, one file per M code
import json

a_abfss = mssparkutils.lakehouse.get(analysis_lakehouse)["properties"]["abfsPath"]
a_base = f"{a_abfss}/{analysis_subdir.strip().strip('/')}"
analysis = json.loads(mssparkutils.fs.head(f"{a_base}/analysis.json", 8 * 1024 * 1024))

per_query = {}
try:
    for fi in mssparkutils.fs.ls(f"{a_base}/queries"):
        if fi.name.lower().endswith(".json"):
            per_query[fi.name[:-5]] = mssparkutils.fs.head(fi.path, 1024 * 1024)
except Exception as e:                               # noqa: BLE001
    print(f"no per-query analyses found ({type(e).__name__}) - using analysis.json only")

med = analysis["medallion"]
BRONZE_LH = bronze_lakehouse or med["bronze"]["lakehouse"]
BRONZE_SCH = bronze_schema or med["bronze"]["schema"]
SILVER_LH = silver_lakehouse or med["silver"]["lakehouse"]
SILVER_SCH = silver_schema or med["silver"]["schema"]
GOLD_LH = gold_lakehouse or med["gold"]["lakehouse"]
GOLD_SCH = gold_schema or med["gold"]["schema"]

tables = analysis["tables"]
n_s = sum(1 for t in tables if t["layer"] == "silver")
n_g = sum(1 for t in tables if t["layer"] == "gold")
print(f"analysis.json: {len(tables)} tables ({n_s} silver, {n_g} gold), "
      f"{len(per_query)} per-query analyses, generated_at={analysis.get('generated_at', '?')}")
print(f"  bronze={BRONZE_LH}.{BRONZE_SCH} silver={SILVER_LH}.{SILVER_SCH} gold={GOLD_LH}.{GOLD_SCH}")

m_abfss = mssparkutils.lakehouse.get(m_lakehouse)["properties"]["abfsPath"]
m_dir = f"{m_abfss}/{m_subdir.strip().strip('/')}"
m_queries = {}
for fi in mssparkutils.fs.ls(m_dir):
    if fi.name.lower().endswith(".m"):
        m_queries[fi.name[:-2]] = mssparkutils.fs.head(fi.path, 1024 * 1024)
print(f"loaded {len(m_queries)} M source files for reference")

# --- re-validate the plan we were handed (it may have been hand-edited) ---
problems, seen = [], set()
for t in tables:
    tt = t.get("target_table")
    if not tt or t.get("layer") not in ("silver", "gold"):
        problems.append(f"bad entry: {t}")
        continue
    if tt in seen:
        problems.append(f"duplicate target_table: {tt}")
    seen.add(tt)
    if not t.get("output_columns"):
        problems.append(f"{tt}: no output_columns")
    if not t.get("control"):
        problems.append(f"{tt}: no control block - re-run nb_m_analyze")
    if t["layer"] == "gold":
        bad = [r for r in t.get("reads_from") or []
               if r.split(".")[0] == BRONZE_LH
               or r.lower().startswith(f"{BRONZE_LH}.{BRONZE_SCH}".lower())]
        if bad:
            problems.append(f"GOLD {tt} reads Bronze {bad} - fix the plan before converting")

stale = sorted(set(m_queries) - set(analysis.get("m_queries") or []))
if stale:
    print(f"  WARNING: {len(stale)} M quer(y|ies) not covered by this plan "
          f"({', '.join(stale)}) - re-run nb_m_analyze to include them")

import pandas as pd
display(pd.DataFrame([{
    "target_table": t["target_table"], "layer": t["layer"],
    "origin": (t.get("origin") or {}).get("type"),
    "m_query": (t.get("origin") or {}).get("m_query"),
    "reads_from": ", ".join(t.get("reads_from") or []),
    "cols": len(t.get("output_columns") or []),
    "sql_file_path": (t.get("control") or {}).get("sql_file_path"),
} for t in tables]))

if problems:
    print("\n!! PLAN PROBLEMS:")
    for p in problems:
        print("  -", p)
    raise RuntimeError(f"{len(problems)} problem(s) in analysis.json (see above)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 2. load the shared LLM factory -------------------------------
%run nb_bedrock

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 2b. build the chat model -------------------------------------
llm = get_chat_model(
    config_lakehouse=llm_config_lakehouse,
    config_path=llm_config_path,
    provider=llm_provider or None,
    base_url=llm_base_url or None,
    api_key=llm_api_key or None,
    model=llm_model or None,
    temperature=temperature,
    max_tokens=max_tokens,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 3. converter instructions ----------------------------------
B = f"{BRONZE_LH}.{BRONZE_SCH}"
S = f"{SILVER_LH}.{SILVER_SCH}"

EXAMPLE = f"""-- silver/stg_address.sql   (plan: target_table=stg_address, grain=[AddressID])
WITH src AS (
    SELECT
        CAST(AddressID AS BIGINT)                     AS AddressID,
        CAST(AddressLine1 AS STRING)                  AS AddressLine1,
        TRIM(CAST(City AS STRING))                    AS City,
        TRIM(CAST(StateProvince AS STRING))          AS State,
        TRY_CAST(ModifiedDate AS TIMESTAMP)         AS LastModifiedDate
    FROM {B}.Address
),
dedup AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY AddressID ORDER BY LastModifiedDate DESC NULLS LAST) AS _rn
    FROM src
)
SELECT AddressID, AddressLine1, City, State, LastModifiedDate,
       concat_ws(', ', AddressLine1, City, State) AS FullAddress
FROM dedup WHERE _rn = 1"""

INSTRUCTIONS = f"""
You are the CONVERTER. You are given an authoritative medallion build plan
(`analysis.json`) and the original M source (`m/*.m`). For every entry in
`analysis.json.tables[]` you emit ONE `spark.sql()`-executable `SELECT`.

## Do NOT
- do NOT change a table's `layer`, `target_table`, `reads_from`, or
  `output_columns` - the analysis is final,
- do NOT add or drop tables,
- do NOT let a `gold` statement reference `{BRONZE_LH}` / `{B}` - gold reads the
  silver tables listed in its `reads_from` only,
- no `CREATE` / `INSERT` / `MERGE` / `USE`, no trailing semicolon, ONE statement.

## Do
- FROM / JOIN exactly the tables in that entry's `reads_from`
  (bronze `{B}.<T>`, silver `{S}.<t>`).
- Return the entry's `output_columns`, in that order, with those exact
  case-sensitive names as the final SELECT list.
- Implement `grain` / `dedup` with
  `ROW_NUMBER() OVER (PARTITION BY <keys> ORDER BY <dedup.order_by>) = 1`.
- Implement `filters`, `renames`, `type_casts`, `joins`
  (`LEFT`/`INNER` per `kind`), and `derived_columns` (`logic` -> Spark expr;
  nested if/then/else -> `CASE WHEN ... END`).
- For the EXACT expression text (rounding, status-code decoding, concat
  separators, date coercion) READ the matching `m/<origin.m_query>.m` file.
- `origin.type == "split"`: build the staging SELECT from
  `{B}.<origin.raw_bronze_table>` (that raw pull only - select / cast / clean /
  filter / dedup); the gold entry that needs it already points at your silver table.

## M -> Spark quick map
Int64.Type->BIGINT, `type number`->DOUBLE, `type text`->STRING, `type logical`->BOOLEAN,
`type date`->DATE, `type datetime`->TIMESTAMP.
`... type datetime` then `Date.From` -> `CAST(TRY_CAST(x AS TIMESTAMP) AS DATE)`.
`DateTime.Date(DateTime.LocalNow())` -> `current_date()`.
`Text.Combine(list,sep)` -> `concat_ws(sep, ...)`; `Text.Trim` -> `TRIM`;
`Table.ReplaceValue(t,null,X,..,{{c}})` -> `COALESCE(c, X) AS c`.
Calendar (`List.Dates`/`#date`/min-max of a date col) ->
`explode(sequence(make_date(year(min(d)),1,1), make_date(year(max(d)),12,31), interval 1 day))`;
`Date.DayOfWeek(d,Day.Monday) >= 5` -> `weekday(d) >= 5`;
`Date.ToText(d,"MMMM"|"yyyy-MM"|"dddd")` -> `date_format(d,'MMMM'|'yyyy-MM'|'EEEE')`;
`"Q" & Number.ToText(Date.QuarterOfYear(d))` -> `concat('Q', quarter(d))`.

## Style reference
```sql
{EXAMPLE}
```

## The filesystem you are given
- `analysis.json`        - the merged build plan; `tables[]` is your work list.
- `analysis/<Query>.json` - the per-M-query analysis nb_m_analyze produced for
  each M code (same table specs, plus that query's own notes). Read the one for
  an entry's `origin.m_query` when you want more detail than `analysis.json` has.
- `m/<Query>.m`          - the original Power Query source.

## What to do
1. `read_file` `analysis.json`. For each entry also `read_file`
   `analysis/<origin.m_query>.json` and `m/<origin.m_query>.m`.
2. For each `tables[]` entry `write_file` its SQL to `silver/<target_table>.sql`
   or `gold/<target_table>.sql` (matches `layer`; lowercase; no `m/` prefix).
   Ignore each entry's `control` block - that is downstream metadata, not SQL.
3. `write_file` `layering.md`: markdown table `table | layer | reads_from`,
   one row per file you wrote.
Reply DONE with the silver/gold file counts. Do not ask questions.
"""
print(f"instructions: {len(INSTRUCTIONS)} chars ; bronze={B} silver={S}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 4. run the converter agent -------------------------------
from deepagents import create_deep_agent

# the agent gets the COMPLETE analysis: the merged plan, every per-M-query
# analysis, and the original M source for exact expression text.
seed_files = {"analysis.json": json.dumps(analysis, indent=2)}
seed_files.update({f"analysis/{name}.json": text for name, text in per_query.items()})
seed_files.update({f"m/{name}.m": code for name, code in m_queries.items()})

agent = create_deep_agent(tools=[], instructions=INSTRUCTIONS, model=llm)

expected = sorted(f"{t['layer']}/{t['target_table']}.sql" for t in tables)
result = agent.invoke(
    {"messages": [{"role": "user", "content":
                   "Convert every entry in analysis.json.tables[] to Spark SQL per your "
                   f"instructions. Expected files: {expected}. Write them all + layering.md, "
                   "then reply DONE with the counts."}],
     "files": seed_files},
    config={"recursion_limit": recursion_limit},
)
files_out = result.get("files", {}) or {}
sql_files = {k: v for k, v in files_out.items()
             if k.startswith(("silver/", "gold/")) and k.endswith(".sql")}
print(result["messages"][-1].content[:1000])
print(f"\nagent wrote {len(sql_files)}/{len(expected)} expected .sql files")

missing = sorted(set(expected) - set(sql_files))
extra = sorted(set(sql_files) - set(expected))
if missing:
    print("  MISSING:", ", ".join(missing))
if extra:
    print("  UNEXPECTED (not in plan):", ", ".join(extra))
if "layering.md" in files_out:
    print("\n--- layering.md ---\n" + files_out["layering.md"])
if not sql_files:
    raise RuntimeError("agent produced no silver/*.sql or gold/*.sql files")
if missing:
    raise RuntimeError(f"agent skipped {len(missing)} planned table(s): {missing}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 5. persist the .sql files ------------------------------
out_abfss = mssparkutils.lakehouse.get(out_lakehouse)["properties"]["abfsPath"]
_rel_root = sql_out_subdir.strip().strip("/")
out_base = f"{out_abfss}/{_rel_root}"
for sub in ("silver", "gold"):
    mssparkutils.fs.mkdirs(f"{out_base}/{sub}")

written = []
for rel, text in sorted(sql_files.items()):
    mssparkutils.fs.put(f"{out_base}/{rel}", text.rstrip() + "\n", overwrite)
    written.append(rel)
if "layering.md" in files_out:
    mssparkutils.fs.put(f"{out_base}/layering.md", files_out["layering.md"], overwrite)

print(f"wrote {len(written)} files under {out_base}/")
for r in written:
    print("  ", r)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 6. EXPLAIN sanity-check (best-effort) ------------------
if run_explain_check:
    bad = []
    for rel, text in sorted(sql_files.items()):
        try:
            spark.sql("EXPLAIN " + text.rstrip().rstrip(";"))
        except Exception as e:                       # noqa: BLE001
            bad.append((rel, str(e).splitlines()[0][:200]))
    if bad:
        print(f"{len(bad)} statement(s) did not parse/resolve:")
        for rel, msg in bad:
            print(f"  {rel}: {msg}")
        print("\n(gold resolution errors are expected before silver has loaded, "
              "or if the target lakehouses are not attached to this notebook.)")
    else:
        print(f"all {len(sql_files)} statements parsed OK")
else:
    print("run_explain_check = False - skipped")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 7. control-table seed: pipeline_control.csv + INSERT ----
# Rows come straight from each plan entry's `control` block (built by
# nb_m_analyze) - nothing is re-derived here. control_id (IDENTITY) and the
# last_* runtime columns are omitted; created_on is stamped now.
import csv
import io
from datetime import datetime, timezone

CSV_COLUMNS = [
    "layer", "domain", "object_name", "source_type",
    "source_connection_name", "source_workspace", "source_lakehouse",
    "source_schema", "source_object_name",
    "sql_file_path", "sql_lakehouse",
    "target_lakehouse", "target_schema", "target_table",
    "load_type", "key_columns", "incremental_column",
    "incremental_column_type", "watermark_value", "depends_on_control_ids",
    "batch_group", "is_active", "created_on",
]
_now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
_written_set = set(written)
_sql_path_root = _rel_root[6:] if _rel_root.lower().startswith("files/") else _rel_root

control_rows = []
for t in tables:                                   # plan order: silver first, then gold
    rel = f"{t['layer']}/{t['target_table']}.sql"
    if rel not in _written_set:                    # cell 4 already raises on a miss
        continue
    ctl = dict(t["control"])
    # the .sql actually landed under this notebook's out path - keep the row honest
    ctl["sql_file_path"] = f"{_sql_path_root}/{rel}"
    ctl["sql_lakehouse"] = control_sql_lakehouse or ctl.get("sql_lakehouse") or out_lakehouse
    ctl["created_on"] = _now_iso
    control_rows.append({c: ctl.get(c, "") for c in CSV_COLUMNS})

_buf = io.StringIO()
_w = csv.DictWriter(_buf, fieldnames=CSV_COLUMNS)
_w.writeheader()
for r in control_rows:
    _w.writerow(r)
mssparkutils.fs.put(f"{out_base}/pipeline_control.csv", _buf.getvalue(), overwrite)

_ins_cols = ["layer", "domain", "object_name", "source_type", "sql_file_path",
             "sql_lakehouse", "target_lakehouse", "target_schema", "target_table",
             "load_type", "batch_group", "is_active", "created_on"]


def _lit(v):
    return "'" + str(v).replace("'", "''") + "'"


_vals = ["(" + ", ".join([
    _lit(r["layer"]), _lit(r["domain"]), _lit(r["object_name"]), _lit(r["source_type"]),
    _lit(r["sql_file_path"]), _lit(r["sql_lakehouse"]), _lit(r["target_lakehouse"]),
    _lit(r["target_schema"]), _lit(r["target_table"]), _lit(r["load_type"]),
    str(r["batch_group"]), str(r["is_active"]), "SYSUTCDATETIME()"]) + ")"
    for r in control_rows]
insert_sql = ("-- seed for metadata.pipeline_control - generated by nb_m_to_sql\n"
              "INSERT INTO metadata.pipeline_control\n(" + ", ".join(_ins_cols) + ")\nVALUES\n"
              + ",\n".join(_vals) + ";\n")
mssparkutils.fs.put(f"{out_base}/pipeline_control_insert.sql", insert_sql, overwrite)

print(f"wrote {len(control_rows)} control rows ->\n  {out_base}/pipeline_control.csv"
      f"\n  {out_base}/pipeline_control_insert.sql")
display(pd.DataFrame(control_rows)[
    ["layer", "object_name", "sql_file_path", "sql_lakehouse",
     "target_lakehouse", "target_schema", "target_table", "load_type", "batch_group"]])
print("\n--- pipeline_control_insert.sql ---\n" + insert_sql)
print("Load via Hydra_DW: run the INSERT above, or COPY INTO the CSV:\n"
      f"  COPY INTO metadata.pipeline_control ({', '.join(CSV_COLUMNS)})\n"
      f"  FROM '<https OneLake url to {_sql_path_root}/pipeline_control.csv>'\n"
      "  WITH (FILE_TYPE='CSV', FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0A');")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
