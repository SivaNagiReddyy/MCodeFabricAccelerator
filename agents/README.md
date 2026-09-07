# M-to-Medallion agent pipeline

Converts the Power Query (M) in the semantic model into Spark SQL that
[`nb_generic_layer_load`](../nb_generic_layer_load.Notebook/notebook-content.py)
runs via `spark.sql()`, and keeps that SQL correct.

| # | Agent | Form | Input | Output |
|---|-------|------|-------|--------|
| 1 | Extract M | [`nb_extract_mcode`](nb_extract_mcode.Notebook/notebook-content.py) - deterministic Python | any semantic model | `HYDRA_BRONZE_LK/Files/m_extract/<QueryName>.m` |
| 2a | Analyze each M -> build plan | [`nb_m_analyze`](nb_m_analyze.Notebook/notebook-content.py) - per-query LLM calls (gateway LLM via `nb_bedrock`) | `Files/m_extract/*.m` | `Files/analysis/queries/<Query>.json` (one per M code), `analysis.json`, `analysis.md` |
| 2b | Full analysis -> Spark SQL + control seed | [`nb_m_to_sql`](nb_m_to_sql.Notebook/notebook-content.py) - **deepagents** agent (gateway LLM via `nb_bedrock`) | `analysis.json` + `queries/*.json` + `Files/m_extract/*.m` | `Files/sql/silver/*.sql`, `Files/sql/gold/*.sql`, `layering.md`, `Files/sql/pipeline_control.csv`, `Files/sql/pipeline_control_insert.sql` |
| 3 | Read log errors, fix SQL | LLM (not built) | `metadata.pipeline_control_log` + `sql/**` | patched `sql/**` |
| 4 | Validate M vs Spark SQL | LLM + Spark (not built) | M result vs `spark.sql()` result | validation report |

## Layout

Every agent notebook lives in this folder; Fabric Git integration maps it to a
workspace **folder** called `agents`:

```
agents/
  README.md
  nb_bedrock.Notebook/          shared LLM client  (%run'd by the agents)
  nb_extract_mcode.Notebook/    agent 1
  nb_m_analyze.Notebook/        agent 2a
  nb_m_to_sql.Notebook/         agent 2b
```

The runtime notebooks stay at the repo root because the data pipelines invoke
them: [`nb_generic_layer_load`](../nb_generic_layer_load.Notebook/notebook-content.py)
and [`nb_log_collector`](../nb_log_collector.Notebook/notebook-content.py).

Nothing here references another notebook by path - `%run nb_bedrock` and the
pipeline activities both resolve by **item name / id**, so the folder move is
transparent to execution.

Agent 2 is split so the medallion plan can be reviewed / hand-edited (it is just
`analysis.json`) before any SQL is generated, and so each half can use its own
model. 2a runs the deterministic regex pre-pass (name-based layer, raw Bronze
refs via `fnGetSalesLTTable("X")` / `Item="X"`, M->M deps) and feeds those facts
to the LLM as ground truth. Both halves get their model from `nb_bedrock`.

## Agent 2a - `nb_m_analyze`  (analyzer, no SQL)

Analyzes **one M query at a time** - each M code gets its own analysis file -
then merges them into a single build plan:

```
Files/analysis/queries/<QueryName>.json   <- one per M code
Files/analysis/analysis.json              <- merged, flattened plan
Files/analysis/analysis.md                <- human-readable summary
```

Per query the LLM returns the table(s) it must become, each with `layer`
(silver/gold), `target_table`, `origin` (`m_query` or `split`), `reads_from`,
exact `output_columns`, `grain` / `dedup`, and the `filters` / `renames` /
`type_casts` / `joins` / `derived_columns` in structured form.

- `Stg_*` -> one **silver** `stg_*` (may read Bronze).
- `Dim*` / `Fact*` -> one **gold** table (reads **silver only**).
- A gold M query that pulls raw Bronze inline (`FactSales` ->
  `SalesOrderHeader` + `SalesOrderDetail`; `DimDate` -> `SalesOrderHeader`)
  returns *extra* entries: new **silver** `stg_*` tables (`origin.type=split`)
  for those raw pulls, with the gold entry's `reads_from` pointing at them.

The merge step (cell 6) de-duplicates a split table two gold queries both need -
`stg_sales_order_header` is emitted **once**, with the union of the columns each
caller wants - then attaches a **`control` block** to every table: the complete
`metadata.pipeline_control` row (layer, domain, object_name, source_type,
sql_file_path, sql_lakehouse, target_lakehouse/schema/table, load_type,
key_columns from `grain`, batch_group, is_active). Cell 7 validates (duplicate
names, missing `output_columns`, **no gold entry reads Bronze**) and raises so
you fix it before any SQL exists.

## Agent 2b - `nb_m_to_sql`  (converter)

Reads the **complete analysis** - `analysis.json`, every
`queries/<Query>.json`, and the `m/*.m` source - and re-validates the plan on
load, so a hand-edited `analysis.json` is still safe. For each `tables[]` entry
it emits ONE `spark.sql()`-executable `SELECT`:

1. does **not** re-classify layers or add/drop tables - FROM/JOIN exactly the
   entry's `reads_from`, return exactly its `output_columns`, implement its
   `grain` / `filters` / `joins` / `derived_columns`;
2. reads `analysis/<origin.m_query>.json` and `m/<origin.m_query>.m` for the
   detail and the exact expression text (rounding, CASE status decoding, concat
   separators, date coercion);
3. gold statements never reference Bronze; `origin.type=split` entries build
   from `<bronze>.<raw_bronze_table>` only;
4. writes `silver/<t>.sql` / `gold/<t>.sql` + `layering.md` to
   `<out_lakehouse>/Files/sql/{silver,gold}/`; the notebook checks every planned
   file was produced (raises on a miss), then **EXPLAIN-checks** each
   (best-effort), then emits **`pipeline_control.csv`** +
   **`pipeline_control_insert.sql`** straight from the plan's `control` blocks -
   nothing is re-derived.

## Shared LLM client - `nb_bedrock`

Both agents (and 3/4 later) get their model from **one** factory so credential
handling lives in exactly one place:

```python
%run nb_bedrock
llm = get_chat_model(config_lakehouse=..., config_path=..., temperature=..., max_tokens=...)
```

The LLM is reached through an **Anthropic-/OpenAI-compatible gateway** (a
LiteLLM proxy in front of Bedrock). Its `base_url` / `api_key` / `model` are
saved **inside Fabric** as `<lakehouse>/Files/config/llm_config.json` - OneLake,
workspace-permissioned, and **not** in the Git repo. No Key Vault required.

One-time setup: open `nb_bedrock`, fill the `cfg_*` parameters (paste the
gateway key into `cfg_api_key`), set `write_config = True`, run, then set it
back to `False`. Per-field resolution in `get_chat_model` is: explicit argument
-> `llm_config.json` -> environment variable (`ANTHROPIC_BASE_URL` /
`ANTHROPIC_AUTH_TOKEN` / `OPENAI_API_KEY` / `LLM_MODEL`). `provider="anthropic"`
(default) builds a `ChatAnthropic`; `provider="openai"` a `ChatOpenAI`.

Prerequisites: the **py-packages** environment attached (`deepagents`,
`langchain-anthropic`, `langchain-openai`), and `nb_bedrock` present in the
same workspace (`%run` resolves it by name).

**Workspace-agnostic:** 2a records the medallion config into `analysis.json`;
2b reads it back (params override only if set). Every lakehouse is resolved by
name at run time - deploy into any workspace and point the params at its
lakehouses.

[`../sql/silver/stg_address.sql`](../sql/silver/stg_address.sql) is a hand-written
reference target used as the few-shot example in the 2b agent prompt.

## Downstream wiring

Each generated `.sql` becomes one `metadata.pipeline_control` row - emitted
directly by the notebook as `pipeline_control.csv` /
`pipeline_control_insert.sql` (`layer` = Silver/Gold, `source_type` = SparkSQL,
`sql_file_path` = `sql/silver/<t>.sql`, `sql_lakehouse`, `target_lakehouse`,
`target_schema`, `target_table`, `load_type` = Full, `batch_group` 1=silver /
2=gold, `is_active` = 1). Load it with the `INSERT` script from `Hydra_DW`, or
`COPY INTO` the CSV. `Master_Orchestration_Pipeline` then invokes the
orchestrator once per layer (Silver then Gold).
