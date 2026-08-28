# M-to-Medallion agent pipeline

Converts the Power Query (M) in the semantic model into Spark SQL that
[`nb_generic_layer_load`](../nb_generic_layer_load.Notebook/notebook-content.py)
runs via `spark.sql()`, and keeps that SQL correct.

| # | Agent | Form | Input | Output |
|---|-------|------|-------|--------|
| 1 | Extract M | [`nb_extract_mcode`](../nb_extract_mcode.Notebook/notebook-content.py) - deterministic Python | any semantic model | `HYDRA_BRONZE_LK/Files/m_extract/<QueryName>.m` |
| 2 | M -> Spark SQL + layering | [`nb_m_to_sql`](../nb_m_to_sql.Notebook/notebook-content.py) - **deepagents** agent (LLM via `langchain-openai`) | `Files/m_extract/*.m` | `Files/sql/silver/*.sql`, `Files/sql/gold/*.sql`, `layering.md` |
| 3 | Read log errors, fix SQL | LLM (not built) | `metadata.pipeline_control_log` + `sql/**` | patched `sql/**` |
| 4 | Validate M vs Spark SQL | LLM + Spark (not built) | M result vs `spark.sql()` result | validation report |

## Agent 2 - `nb_m_to_sql`

Runs a `deepagents` agent that reads every extracted `.m` file, then:

1. **assigns a medallion layer** -
   - `Stg_*` -> **silver** (`HYDRA_SILVER_LK.silver.stg_*`), reads Bronze only.
   - `Dim*` / `Fact*` -> **gold** (`HYDRA_GOLD_LK.sales.<snake_case>`), reads
     **silver only - never Bronze**.
   - Raw Bronze pulls that a gold query does inline (`FactSales` ->
     `SalesOrderHeader` + `SalesOrderDetail`; `DimDate` -> `SalesOrderHeader`)
     are **split into new silver staging tables**, and the gold query is
     rewritten to read those.
2. **converts** the transformation logic to a single `spark.sql()`-executable
   `SELECT` (CTEs allowed; no DDL/DML).
3. **writes** `silver/<t>.sql` / `gold/<t>.sql` + a `layering.md` table
   (`table | layer | reads_from`), persisted to
   `HYDRA_BRONZE_LK/Files/sql/{silver,gold}/`.
4. **EXPLAIN-checks** every generated statement (gold ones only fully resolve
   once silver has loaded).

Prerequisites: the **py-packages** environment attached (`deepagents`,
`langchain-openai`), and an LLM endpoint + key (Azure OpenAI by default -
see the parameters cell; key via Key Vault or the `OPENAI_API_KEY` env var).

[`../sql/silver/stg_address.sql`](../sql/silver/stg_address.sql) is a hand-written
reference target used as the few-shot example in the agent prompt.

## Downstream wiring

Each generated `.sql` becomes one `metadata.pipeline_control` row
(`layer` = Silver/Gold, `sql_file_path` = `sql/silver/<t>.sql`, `target_lakehouse`,
`target_schema`, `target_table`, `load_type` = Full). `Master_Orchestration_Pipeline`
invokes the orchestrator once per layer (Silver then Gold).
