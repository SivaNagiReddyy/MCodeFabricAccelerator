# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ## nb_m_analyze  -  Agent 2a: analyze each M query, emit a build plan
#
# First of the two M -> Spark SQL agents. It writes **no SQL**.
#
# It reads the Power Query (M) files produced by **nb_extract_mcode**
# (`<m_lakehouse>/<m_subdir>/*.m`) and, **one M query at a time**, asks the LLM
# (via **nb_llm_client**) to describe what that query must become. Each M file gets
# its own analysis file:
#
# ```
# Files/analysis/queries/<QueryName>.json     <- one per M code
# Files/analysis/analysis.json                <- merged, flattened build plan
# Files/analysis/analysis.md                  <- human-readable summary
# ```
#
# Per query the LLM returns the table(s) that query becomes: normally one, but a
# `Dim*` / `Fact*` query that pulls raw Bronze inline also returns the new
# **silver split tables** its raw pulls must be carved into. A merge step then
# flattens every per-query result into `analysis.json`, de-duplicating split
# tables that two gold queries share (unioning their columns).
#
# For every table in the plan the notebook also computes a **`control` block** -
# the complete `metadata.pipeline_control` row (layer, domain, object_name,
# source_type, sql_file_path, sql_lakehouse, target_*, load_type, key_columns,
# batch_group, is_active). **nb_m_to_sql** (Agent 2b) reads the whole analysis,
# generates the `.sql` files, and emits the control CSV straight from these
# blocks - it never re-derives them.
#
# Splitting analysis from conversion lets you review / hand-edit the plan before
# any SQL exists, and lets each half use its own model.
#
# ### Portability
# Every lakehouse is resolved by **name** at run time; every target is a
# parameter. Deploy into any workspace and point the parameters at its lakehouses.
#
# ### Prerequisites
# - the **py-packages** environment attached (`deepagents`, `langchain-anthropic`),
# - **nb_llm_client** in the same workspace, and its `llm_config.json` written once
#   (gateway `base_url` / `api_key` / `model`, saved in a lakehouse - see nb_llm_client).

# PARAMETERS CELL ********************

# --- inputs / outputs -------------------------------------------------------
m_lakehouse        = "HYDRA_BRONZE_LK"   # lakehouse holding the extracted M files
m_subdir           = "Files/m_extract"   # folder under it
analysis_lakehouse = "HYDRA_BRONZE_LK"   # lakehouse to write the analysis into
analysis_subdir    = "Files/analysis"    # -> Files/analysis/queries/*.json , analysis.json , analysis.md

# --- medallion config (recorded into analysis.json for Agent 2b) --------
bronze_lakehouse = "HYDRA_BRONZE_LK"
bronze_schema    = "SalesLT"
silver_lakehouse = "HYDRA_SILVER_LK"
silver_schema    = "silver"
gold_lakehouse   = "HYDRA_GOLD_LK"
gold_schema      = "sales"

# --- control-table details baked into the plan (metadata.pipeline_control) ---
domain                = "Sales"
control_sql_lakehouse = "HYDRA_BRONZE_LK"  # lakehouse where nb_m_to_sql writes the .sql files
sql_path_root         = "sql"              # -> sql_file_path = sql/<layer>/<table>.sql
default_load_type     = "Full"             # 'Full' | 'Incremental'
silver_batch_group    = 1
gold_batch_group      = 2

# --- LLM: resolved by nb_llm_client from the saved llm_config.json ---------
llm_config_lakehouse = "HYDRA_BRONZE_LK"          # lakehouse holding Files/config/llm_config.json
llm_config_path      = "Files/config/llm_config.json"
llm_provider         = ""        # "" = use the config file; else "anthropic" | "openai"
llm_base_url         = ""        # "" = use the config file
llm_api_key          = ""        # "" = use the config file (keep blank - never commit a key)
llm_model            = ""        # "" = use the config file
temperature          = 0
max_tokens           = 8192
json_retries         = 2         # re-asks per M query if the reply is not valid JSON

overwrite = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 1. load the extracted M queries ----------------------------------
m_abfss = mssparkutils.lakehouse.get(m_lakehouse)["properties"]["abfsPath"]
m_dir = f"{m_abfss}/{m_subdir.strip().strip('/')}"

m_queries = {}
for fi in mssparkutils.fs.ls(m_dir):
    if fi.name.lower().endswith(".m"):
        m_queries[fi.name[:-2]] = mssparkutils.fs.head(fi.path, 1024 * 1024)

if not m_queries:
    raise ValueError(f"no .m files found in {m_dir} - run nb_extract_mcode first")
print(f"loaded {len(m_queries)} M queries: {', '.join(sorted(m_queries))}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 2. deterministic pre-analysis (no LLM) -------------------------
# Ground-truth facts handed to the LLM per query so it does not have to guess:
# raw-Bronze references, name-based layer, cross-query (M -> M) dependencies.
import re

_RAW_BRONZE_RE = re.compile(r'fnGetSalesLTTable\(\s*"([^"]+)"\s*\)|Item\s*=\s*"([^"]+)"')


def raw_bronze_refs(code: str):
    return sorted({m.group(1) or m.group(2) for m in _RAW_BRONZE_RE.finditer(code)})


def name_layer(name: str) -> str:
    n = name.lower()
    return "gold" if (n.startswith("dim") or n.startswith("fact")) else "silver"


def snake(name: str) -> str:
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return re.sub(r"_+", "_", s).strip("_").lower()


facts = {}
for name in sorted(m_queries):
    code = m_queries[name]
    layer = name_layer(name)
    raw = raw_bronze_refs(code)
    deps = sorted(q for q in m_queries
                  if q != name and re.search(rf"\b{re.escape(q)}\b", code))
    facts[name] = {"query": name, "layer": layer, "raw_bronze": raw,
                   "m_deps": deps, "needs_split": layer == "gold" and bool(raw)}

import pandas as pd
print("pre-analysis:")
display(pd.DataFrame(list(facts.values())))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 3. load the shared LLM factory ---------------------------------
%run nb_llm_client

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 3b. build the chat model ---------------------------------------
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

# ---- 4. the per-query analyzer --------------------------------------
import json

B = f"{bronze_lakehouse}.{bronze_schema}"
S = f"{silver_lakehouse}.{silver_schema}"

TABLE_SCHEMA_DOC = """
{
  "m_query": <str>,                                // the M query you analyzed
  "tables": [                                      // 1 entry normally; a gold query needing splits returns the gold table + its silver split tables
    {
      "target_table": <snake_case str>,           // "stg_address", "dim_product", "fact_sales"
      "layer": "silver" | "gold",
      "origin": {
        "type": "m_query" | "split",              // "split" = a NEW silver staging table carved out of this gold query
        "m_query": <str>,                         // this M query's name
        "raw_bronze_table": <str|null>            // for "split": the raw table it wraps, e.g. "SalesOrderHeader"
      },
      "reads_from": [<fully.qualified.table>, ...],// what the generated SQL will SELECT FROM.
                                                  //   silver: bronze tables and/or other silver tables
                                                  //   gold  : silver tables ONLY - never a bronze table
      "output_columns": [<str>, ...],             // exact, case-sensitive, in final order
      "grain": [<str>, ...],                      // uniqueness key(s); [] if none
      "dedup": {"keys": [<str>,...], "order_by": <str>} | null,   // e.g. "LastModifiedDate DESC NULLS LAST"
      "filters": [<str>, ...],                    // "OrderQty > 0", "AddressType = 'Main Office'"
      "renames": [{"from": <str>, "to": <str>}, ...],
      "type_casts": [{"column": <str>, "to": "BIGINT|DOUBLE|STRING|BOOLEAN|DATE|TIMESTAMP"}, ...],
      "joins": [{"kind": "LEFT|INNER", "right": <table>, "on": <str>, "brings": [<str>,...]}, ...],
      "derived_columns": [{"name": <str>, "logic": <str>}, ...],  // plain words or a Spark expression
      "notes": <str>
    }
  ]
}
"""

SYSTEM_RULES = f"""
You are the ANALYZER for a Power BI -> Microsoft Fabric medallion migration.
Given ONE Power Query (M) query you describe the table(s) it must become.
**You never write SQL.** You return JSON only.

## Medallion rules (hard)
- BRONZE already exists: raw tables at `{B}.<Table>`. An M step
  `fnGetSalesLTTable("X")` or `Sql.Database(...) Item="X"` means: read `{B}.X`.
- SILVER = cleaned / typed / conformed, table `{S}.<snake_case>`. A silver query
  MAY read Bronze.
- GOLD = the star schema, table `{gold_lakehouse}.{gold_schema}.<snake_case>`.
  A gold query MUST read only SILVER. **It may NEVER reference `{bronze_lakehouse}` / `{B}`.**
- If this query is GOLD and pulls a raw Bronze table inline, you MUST return an
  EXTRA table entry per raw pull: a new SILVER staging table
  `stg_<raw_table_snake_case>` (origin.type = "split") that does the
  select / type / clean / de-dup, and the gold entry's `reads_from` must point
  at those silver tables instead of Bronze.
- Parameters (`SQLServerName`, `SQLDatabaseName`) and connection-only functions
  (`fnGetSalesLTTable`) are NOT tables - never return an entry for them.

## Naming
- `Stg_*`  -> silver, `stg_*`.        e.g. Stg_Product -> stg_product
- `Dim*` / `Fact*` -> gold, snake_case. e.g. DimProduct -> dim_product, FactSales -> fact_sales
- split-out raw pull -> silver, `stg_<raw table in snake_case>`.

## Fidelity
- `output_columns` must be what the M query actually returns, in order,
  case-sensitive (respect the final `Table.ReorderColumns` / expand steps -
  ReorderColumns moves the listed columns to the front and KEEPS the rest after).
- `Table.Distinct(t, {{keys}})` -> set `grain` and `dedup` (order_by a
  LastModifiedDate-like column DESC NULLS LAST, else a stable column).
- Record filters / renames / casts / joins / derived columns faithfully; put the
  exact expression in `derived_columns[].logic` when you can.

Return ONE JSON object, this exact shape, and NOTHING else - no prose, no
markdown fences:
{TABLE_SCHEMA_DOC}
"""


def _msg_text(resp):
    """AIMessage.content is a str (openai) or a list of blocks (anthropic)."""
    c = resp.content
    if isinstance(c, list):
        return "".join(b.get("text", "") for b in c if isinstance(b, dict))
    return c


def _salvage_json(text: str):
    t = (text or "").strip()
    if t.startswith("```"):
        t = t.split("```", 2)[1]
        t = t[4:].strip() if t.lower().startswith("json") else t.strip()
    try:
        return json.loads(t)
    except Exception:                               # noqa: BLE001
        i, j = t.find("{"), t.rfind("}")
        if i >= 0 and j > i:
            return json.loads(t[i:j + 1])
        raise


def analyze_one(name, code, fact, retries=2):
    """Ask the LLM to analyze ONE M query. Returns the parsed dict."""
    prompt = f"""{SYSTEM_RULES}

## The M query to analyze: {name}

Ground truth already computed for it (trust this):
- name-based layer : {fact['layer'].upper()}
- raw Bronze refs  : {fact['raw_bronze'] or 'none'}
- other M queries it reads : {fact['m_deps'] or 'none'}
- needs split      : {'YES - return the gold entry PLUS a silver stg_* entry per raw Bronze table above'
                      if fact['needs_split'] else 'no'}

Those "other M queries it reads" are already tables in SILVER - reference them as
`{S}.<snake_case of that query name>` (e.g. Stg_Product -> `{S}.stg_product`).

```m
{code}
```
"""
    last = None
    for attempt in range(retries + 1):
        msg = prompt if attempt == 0 else (
            prompt + f"\n\nYour previous reply was not valid JSON ({last}). "
                     "Return ONLY the JSON object, no fences, no prose.")
        try:
            return _salvage_json(_msg_text(llm.invoke(msg)))
        except Exception as e:                       # noqa: BLE001
            last = str(e)[:200]
            print(f"    [{name}] attempt {attempt + 1} bad JSON: {last}")
    raise RuntimeError(f"{name}: analyzer returned no valid JSON after {retries + 1} attempts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 5. analyze EVERY M query, one at a time, one file each ---------
a_abfss = mssparkutils.lakehouse.get(analysis_lakehouse)["properties"]["abfsPath"]
a_base = f"{a_abfss}/{analysis_subdir.strip().strip('/')}"
q_base = f"{a_base}/queries"
mssparkutils.fs.mkdirs(q_base)

per_query = {}
for i, name in enumerate(sorted(m_queries), 1):
    print(f"[{i}/{len(m_queries)}] analyzing {name} ...")
    result = analyze_one(name, m_queries[name], facts[name], json_retries)
    result["m_query"] = name
    result.setdefault("tables", [])
    per_query[name] = result
    mssparkutils.fs.put(f"{q_base}/{name}.json",
                        json.dumps(result, indent=2) + "\n", overwrite)
    tbls = ", ".join(f"{t.get('target_table')}({t.get('layer')})" for t in result["tables"])
    print(f"    -> {len(result['tables'])} table(s): {tbls}")

print(f"\nwrote {len(per_query)} per-query analyses under {q_base}/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 6. merge into the flattened plan + control-table details -------
# de-duplicate split tables two gold queries share (union their columns),
# then attach the complete metadata.pipeline_control row to every table.
plan = {}
for name in sorted(per_query):
    for t in per_query[name]["tables"]:
        tt = (t.get("target_table") or "").strip()
        if not tt:
            print(f"  ! {name}: entry without target_table, skipped")
            continue
        if tt not in plan:
            t.setdefault("output_columns", [])
            t.setdefault("reads_from", [])
            t.setdefault("grain", [])
            plan[tt] = t
        else:
            ex = plan[tt]
            for c in t.get("output_columns") or []:
                if c not in ex["output_columns"]:
                    ex["output_columns"].append(c)
            for r in t.get("reads_from") or []:
                if r not in ex["reads_from"]:
                    ex["reads_from"].append(r)
            ex.setdefault("also_required_by", []).append(name)
            print(f"  merged duplicate '{tt}' (also declared by {name})")

# silver first, then gold - stable within a layer
ordered = ([t for t in plan.values() if t["layer"] == "silver"]
           + [t for t in plan.values() if t["layer"] == "gold"])

for t in ordered:
    is_silver = t["layer"] == "silver"
    tt = t["target_table"]
    t["depends_on_tables"] = [r for r in t.get("reads_from", [])
                              if r.split(".")[0] != bronze_lakehouse]
    t["control"] = {
        "layer": "Silver" if is_silver else "Gold",
        "domain": domain,
        "object_name": tt,
        "source_type": "SparkSQL",
        "source_connection_name": "",
        "source_workspace": "",
        "source_lakehouse": "",
        "source_schema": "",
        "source_object_name": "",
        "sql_file_path": f"{sql_path_root.strip('/')}/{t['layer']}/{tt}.sql",
        "sql_lakehouse": control_sql_lakehouse,
        "target_lakehouse": silver_lakehouse if is_silver else gold_lakehouse,
        "target_schema": silver_schema if is_silver else gold_schema,
        "target_table": tt,
        "load_type": default_load_type,
        "key_columns": ", ".join(t.get("grain") or []),
        "incremental_column": "",
        "incremental_column_type": "",
        "watermark_value": "",
        "depends_on_control_ids": "",
        "batch_group": silver_batch_group if is_silver else gold_batch_group,
        "is_active": 1,
    }

from datetime import datetime, timezone

analysis = {
    "version": 1,
    "generated_by": "nb_m_analyze",
    "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "llm": getattr(llm, "model", None) or getattr(llm, "model_name", "unknown"),
    "medallion": {
        "bronze": {"lakehouse": bronze_lakehouse, "schema": bronze_schema},
        "silver": {"lakehouse": silver_lakehouse, "schema": silver_schema},
        "gold":   {"lakehouse": gold_lakehouse,   "schema": gold_schema},
    },
    "m_queries": sorted(m_queries),
    "tables": ordered,
    "load_order": [t["target_table"] for t in ordered],
    "warnings": [],
}
print(f"plan: {len(ordered)} tables "
      f"({sum(1 for t in ordered if t['layer'] == 'silver')} silver, "
      f"{sum(1 for t in ordered if t['layer'] == 'gold')} gold)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 7. validate the plan ------------------------------------------
problems = []
seen = set()
for t in analysis["tables"]:
    tt = t.get("target_table")
    if not tt or t.get("layer") not in ("silver", "gold"):
        problems.append(f"bad entry: {t}")
        continue
    if tt in seen:
        problems.append(f"duplicate target_table: {tt}")
    seen.add(tt)
    if not t.get("output_columns"):
        problems.append(f"{tt}: no output_columns")
    if t["layer"] == "gold":
        bad = [r for r in t.get("reads_from") or []
               if r.split(".")[0] == bronze_lakehouse
               or r.lower().startswith(f"{bronze_lakehouse}.{bronze_schema}".lower())]
        if bad:
            problems.append(f"GOLD {tt} reads Bronze {bad} - must be split into silver")
    for r in t.get("depends_on_tables") or []:
        ref = r.split(".")[-1]
        if ref not in seen and ref not in {x["target_table"] for x in analysis["tables"]}:
            analysis["warnings"].append(f"{tt} reads {r} which is not a planned table")

display(pd.DataFrame([{
    "target_table": t["target_table"], "layer": t["layer"],
    "origin": (t.get("origin") or {}).get("type"),
    "m_query": (t.get("origin") or {}).get("m_query"),
    "reads_from": ", ".join(t.get("reads_from") or []),
    "grain": ", ".join(t.get("grain") or []),
    "cols": len(t.get("output_columns") or []),
    "batch_group": t["control"]["batch_group"],
    "sql_file_path": t["control"]["sql_file_path"],
} for t in analysis["tables"]]))

for w in analysis["warnings"]:
    print("  warning:", w)
if problems:
    print("\n!! PLAN PROBLEMS - re-run, change the model, or hand-edit the per-query JSON:")
    for p in problems:
        print("  -", p)
    raise RuntimeError(f"{len(problems)} problem(s) in the plan (see above)")
print("plan validated OK")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 8. persist analysis.json + analysis.md ------------------------
mssparkutils.fs.put(f"{a_base}/analysis.json",
                    json.dumps(analysis, indent=2) + "\n", overwrite)

lines = ["| target_table | layer | origin | m_query | reads_from | grain | # cols | batch | sql_file_path |",
         "|---|---|---|---|---|---|---|---|---|"]
for t in analysis["tables"]:
    o = t.get("origin") or {}
    lines.append("| {tt} | {l} | {o} | {q} | {rf} | {g} | {c} | {b} | {p} |".format(
        tt=t["target_table"], l=t["layer"], o=o.get("type", ""), q=o.get("m_query", ""),
        rf="; ".join(x.split(".")[-1] for x in t.get("reads_from") or []),
        g="; ".join(t.get("grain") or []) or "-",
        c=len(t.get("output_columns") or []),
        b=t["control"]["batch_group"], p=t["control"]["sql_file_path"]))
md = "\n".join(lines) + "\n"
mssparkutils.fs.put(f"{a_base}/analysis.md", md, overwrite)

print(f"wrote:\n  {q_base}/<QueryName>.json   ({len(per_query)} files)"
      f"\n  {a_base}/analysis.json\n  {a_base}/analysis.md")
print("\nnext: run nb_m_to_sql (Agent 2b) - it reads the whole analysis and "
      "generates the .sql files + the control-table CSV.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
