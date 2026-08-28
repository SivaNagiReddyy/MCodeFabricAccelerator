# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ## nb_m_to_sql  -  Agent 2: M -> Spark SQL, with medallion layering
# Reads the Power Query (M) files produced by **nb_extract_mcode**
# (`<m_lakehouse>/<m_subdir>/*.m`), runs a **deepagents** agent (LLM via
# `langchain-openai`) that:
#   1. reads each M query end-to-end,
#   2. decides its medallion layer - **silver** (reads Bronze only) or
#      **gold** (reads Silver only, never Bronze); raw source pulls that gold
#      needs are split out into new silver staging tables,
#   3. converts the transformation logic to a single `spark.sql()`-executable
#      `SELECT` statement,
#   4. writes `silver/<table>.sql` / `gold/<table>.sql` + `layering.md`.
# The `.sql` files are written to `<out_lakehouse>/<sql_out_subdir>/{silver,gold}/`
# and are consumed as-is by **nb_generic_layer_load**.
# Requires the **py-packages** environment (deepagents, langchain-openai) attached,
# plus an LLM endpoint + key (params below).

# PARAMETERS CELL ********************

# --- inputs / outputs ---
m_lakehouse      = "HYDRA_BRONZE_LK"     # lakehouse holding the extracted M files
m_subdir         = "Files/m_extract"     # folder under it
out_lakehouse    = "HYDRA_BRONZE_LK"     # lakehouse to write the .sql files into
sql_out_subdir   = "Files/sql"           # -> Files/sql/silver/*.sql , Files/sql/gold/*.sql

# --- medallion targets referenced inside the generated SQL ---
bronze_lakehouse = "HYDRA_BRONZE_LK"
bronze_schema    = "SalesLT"
silver_lakehouse = "HYDRA_SILVER_LK"
silver_schema    = "silver"
gold_lakehouse   = "HYDRA_GOLD_LK"
gold_schema      = "sales"

# --- LLM (langchain-openai) ---
llm_provider     = "azure"              # "azure" | "openai"
llm_model        = "gpt-4o"             # OpenAI model name, or Azure deployment name
llm_base_url     = ""                   # openai: custom base_url (blank = api.openai.com)
azure_endpoint   = ""                   # azure: https://<res>.openai.azure.com
azure_api_version = "2024-10-21"        # azure only
key_vault_name   = ""                   # optional: Key Vault to pull the api key from
api_key_secret   = "OPENAI-API-KEY"     # secret name in that Key Vault
temperature      = 0

overwrite        = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 1. load the extracted M queries -----------------------------------
m_abfss = mssparkutils.lakehouse.get(m_lakehouse)["properties"]["abfsPath"]
m_dir = f"{m_abfss}/{m_subdir.strip().strip('/')}"

m_queries = {}
for fi in mssparkutils.fs.ls(m_dir):
    if fi.name.lower().endswith(".m"):
        name = fi.name[:-2]
        m_queries[name] = mssparkutils.fs.head(fi.path, 1024 * 1024)

if not m_queries:
    raise ValueError(f"no .m files found in {m_dir} - run nb_extract_mcode first")
print(f"loaded {len(m_queries)} M queries: {', '.join(sorted(m_queries))}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 2. build the LLM (langchain-openai) ------------------------------
import os


def _api_key():
    if key_vault_name:
        return mssparkutils.credentials.getSecret(key_vault_name, api_key_secret)
    return os.environ.get("OPENAI_API_KEY") or os.environ.get("AZURE_OPENAI_API_KEY") or os.environ.get("LLM_API_KEY")


key = _api_key()
if not key:
    raise RuntimeError("no LLM api key - set key_vault_name/api_key_secret or the OPENAI_API_KEY env var")

if llm_provider == "azure":
    from langchain_openai import AzureChatOpenAI
    llm = AzureChatOpenAI(
        azure_deployment=llm_model,
        azure_endpoint=azure_endpoint,
        api_version=azure_api_version,
        api_key=key,
        temperature=temperature,
        timeout=180,
        max_retries=3,
    )
else:
    from langchain_openai import ChatOpenAI
    llm = ChatOpenAI(
        model=llm_model,
        base_url=llm_base_url or None,
        api_key=key,
        temperature=temperature,
        timeout=180,
        max_retries=3,
    )
print("LLM ready:", llm_provider, llm_model)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 3. agent instructions -------------------------------------------
B = f"{bronze_lakehouse}.{bronze_schema}"      # e.g. HYDRA_BRONZE_LK.SalesLT
S = f"{silver_lakehouse}.{silver_schema}"      # e.g. HYDRA_SILVER_LK.silver

EXAMPLE = f"""-- silver.stg_address   <-  M query: Stg_Address
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
You convert Power Query (M) queries from a Power BI semantic model into
Microsoft Fabric **Spark SQL**, and you assign each one a medallion layer.

## Medallion rules (hard)
- BRONZE already exists: raw tables at `{B}.<Table>` (e.g. `{B}.Customer`,
  `{B}.SalesOrderHeader`). A M step `fnGetSalesLTTable("X")` OR
  `Sql.Database(...) ... Item="X"` means: read `{B}.X`.
- SILVER = cleaned / typed / conformed. A silver query MAY read Bronze. Write it
  to `{S}.<snake_case_name>`.
- GOLD = the star schema (dims + fact). A gold query MUST read only SILVER
  (`{S}.<...>`). **It may never reference `{bronze_lakehouse}` / `{B}`.**
- If a query that belongs in GOLD reads a raw Bronze table directly (e.g.
  `FactSales` pulls `SalesOrderHeader` and `SalesOrderDetail`; `DimDate` pulls
  `SalesOrderHeader`), you MUST split that raw pull into a NEW silver staging
  table (`{S}.stg_<thing>`) that does the select/type/clean, then have the gold
  query read that silver table instead.
- Parameters (`SQLServerName`, `SQLDatabaseName`) and connection-only functions
  (`fnGetSalesLTTable`) are NOT tables - do not emit SQL for them.

## Layer assignment
- `Stg_*` M queries -> SILVER, table `{S}.stg_*`.
- `Dim*` / `Fact*` M queries -> GOLD, snake_case (`DimProduct` -> `dim_product`,
  `FactSales` -> `fact_sales`).
- Any new split-out raw pull -> SILVER, `{S}.stg_*`.

## Spark SQL requirements
- ONE statement, runnable by `spark.sql(text)`. `WITH ... SELECT` is fine.
  NO `CREATE` / `INSERT` / `MERGE` / `USE` / trailing semicolon.
- The final `SELECT` must return the COMPLETE target dataset with the exact
  output column names the M query produces (case-sensitive).
- Table refs: bronze `{B}.<T>`, silver `{S}.<t>`. Never hard-code a database
  other than these.

## M -> Spark mapping
- `Table.SelectRows(t, each [c] = "x")` -> `WHERE c = 'x'`;  `[c] = null` -> `c IS NULL`;
  `[c] <> null` -> `c IS NOT NULL`.
- `Table.SelectColumns` / `Table.RemoveColumns` -> explicit SELECT list.
- `Table.RenameColumns` -> `AS` aliases.
- `Table.AddColumn(..., each <expr>)` -> `<expr> AS NewCol`; nested `if/then/else`
  -> `CASE WHEN ... END`.
- Types: `Int64.Type` -> BIGINT, `type number` -> DOUBLE, `type text` -> STRING,
  `type logical` -> BOOLEAN, `type date` -> DATE, `type datetime` -> TIMESTAMP.
- Date coercion `... type datetime` then `Date.From` -> `CAST(TRY_CAST(x AS TIMESTAMP) AS DATE)`.
- `DateTime.Date(DateTime.LocalNow())` -> `current_date()`.
- `Text.Combine(list, sep)` -> `concat_ws(sep, ...)`;  `Text.Trim` -> `TRIM`.
- `Table.Distinct(t, {keys})` -> keep one row per key via
  `ROW_NUMBER() OVER (PARTITION BY keys ORDER BY <LastModifiedDate DESC NULLS LAST, else a stable col>) = 1`.
- `Table.NestedJoin(..., JoinKind.LeftOuter)` + `Table.ExpandTableColumn` ->
  `LEFT JOIN` and select the expanded columns; `JoinKind.Inner` -> `JOIN`.
- Calendar (`List.Dates` / `#date` / `List.Min`/`List.Max` of a date column) ->
  `explode(sequence(make_date(year(min(d)),1,1), make_date(year(max(d)),12,31), interval 1 day))`.
  `Date.DayOfWeek(d, Day.Monday) >= 5` -> `weekday(d) >= 5`.
  `Date.ToText(d,"MMMM")` -> `date_format(d,'MMMM')`; `"yyyy-MM"` -> `'yyyy-MM'`;
  `"dddd"` -> `'EEEE'`.  `"Q" & Number.ToText(Date.QuarterOfYear(d))` -> `concat('Q', quarter(d))`.

## Reference (style you must match)
```sql
{EXAMPLE}
```

## What to do
The virtual filesystem has one file per M query under `m/` (e.g. `m/Stg_Product.m`).
1. `ls` and `read_file` every file under `m/`.
2. Decide layers; note any GOLD query that needs a new SILVER split.
3. For every resulting table, `write_file` its SQL to `silver/<name>.sql` or
   `gold/<name>.sql` (no `m/` prefix, lowercase snake_case filename).
4. `write_file` `layering.md`: a markdown table `table | layer | reads_from`
   covering every file you wrote (reads_from = the tables its SQL selects from).
Do not ask questions - make the standard medallion choice and proceed.
"""
print(f"instructions: {len(INSTRUCTIONS)} chars ; example bronze prefix = {B}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 4. run the deep agent ------------------------------------------
from deepagents import create_deep_agent

seed_files = {f"m/{name}.m": code for name, code in m_queries.items()}

agent = create_deep_agent(tools=[], instructions=INSTRUCTIONS, model=llm)

user_msg = (
    "Convert every M query in m/ to Spark SQL and assign medallion layers, "
    "following your instructions. Write all output files, then reply DONE with a "
    "one-line summary of how many silver and gold files you wrote."
)

result = agent.invoke(
    {"messages": [{"role": "user", "content": user_msg}], "files": seed_files},
    config={"recursion_limit": 150},
)

files_out = result.get("files", {}) or {}
sql_files = {k: v for k, v in files_out.items()
             if k.startswith(("silver/", "gold/")) and k.endswith(".sql")}
print(result["messages"][-1].content[:1000])
print(f"\nagent wrote {len(sql_files)} .sql files:",
      ", ".join(sorted(sql_files)) or "(none)")
if "layering.md" in files_out:
    print("\n--- layering.md ---\n" + files_out["layering.md"])

if not sql_files:
    raise RuntimeError("agent produced no silver/*.sql or gold/*.sql files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 5. persist the .sql files to the lakehouse --------------------
out_abfss = mssparkutils.lakehouse.get(out_lakehouse)["properties"]["abfsPath"]
out_base = f"{out_abfss}/{sql_out_subdir.strip().strip('/')}"
for sub in ("silver", "gold"):
    mssparkutils.fs.mkdirs(f"{out_base}/{sub}")

written = []
for rel, text in sorted(sql_files.items()):
    text = text.rstrip() + "\n"
    mssparkutils.fs.put(f"{out_base}/{rel}", text, overwrite)
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

# ---- 6. sanity-check: EXPLAIN every generated statement -----------
bad = []
for rel, text in sorted(sql_files.items()):
    try:
        spark.sql("EXPLAIN " + text.rstrip().rstrip(";"))
    except Exception as e:
        bad.append((rel, str(e).splitlines()[0][:200]))

if bad:
    print(f"{len(bad)} file(s) failed to parse/resolve:")
    for rel, msg in bad:
        print(f"  {rel}: {msg}")
    print("\n(resolution errors are expected if the upstream silver tables "
          "don't exist yet - re-run this cell after the silver layer has loaded.)")
else:
    print(f"all {len(sql_files)} statements parsed OK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
