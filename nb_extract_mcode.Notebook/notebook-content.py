# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ## nb_extract_mcode
# Extracts Power Query (M) *transformation* queries from a semantic model into
# one file per query, named after the query:  `<output_subdir>/<QueryName>.m`
# Model-agnostic - set `dataset` to any semantic model. Pulls the model
# definition as TMSL/BIM (Semantic Link) and reads `model.expressions[]` plus
# every table partition whose `source.type == "m"`.
# With `transforms_only = True` (default) it keeps only queries that actually
# transform data (they call `Table.*`) and drops raw source-table reads
# (`Sql.Database` + navigation), parameters, connection-only functions and
# auto-generated `Errors in *` queries. Direct Lake / DAX partitions have no M.

# PARAMETERS CELL ********************

dataset          = "HydraReport"       # semantic model: display name or GUID
workspace         = ""                  # blank = current workspace; else name or GUID (for the model fetch)
target_lakehouse  = "HYDRA_BRONZE_LK"   # lakehouse to write the .m files into
output_subdir     = "Files/m_extract"   # path under that lakehouse
transforms_only   = True                # True = only queries that call Table.* ; skip raw source reads / params / functions
overwrite         = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 1. get the model definition as TMSL/BIM JSON --------------------------
import json


def load_model_json(dataset, workspace):
    ws = workspace or None
    try:
        import sempy_labs as labs
        bim = labs.get_semantic_model_bim(dataset=dataset, workspace=ws)
        return bim if isinstance(bim, dict) else json.loads(bim)
    except ModuleNotFoundError:
        pass  # sempy_labs only present when the 'py-packages' environment is attached
    import sempy.fabric as fabric
    return json.loads(fabric.get_tmsl(dataset, workspace=ws))


model_doc = load_model_json(dataset, workspace)
print("loaded model:", model_doc.get("name") or dataset)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 2. the extractor (pure Python, reusable) ----------------------------
import re


def _text(v):
    """TMSL stores multi-line code as a list of strings."""
    if isinstance(v, list):
        return "\n".join(str(x) for x in v)
    return "" if v is None else str(v)


def extract_m(doc):
    """dict(TMSL/BIM) -> {query_name: m_code} for every M query in the model."""
    model = doc.get("model")
    if model is None:
        for k in ("create", "createOrReplace", "database"):
            node = doc.get(k)
            if isinstance(node, dict):
                model = node.get("database", node).get("model")
                if model:
                    break
    model = model or doc

    out = {}
    for e in model.get("expressions", []) or []:
        if e.get("kind", "m") == "m":
            out[e.get("name", "?")] = _text(e.get("expression"))

    for t in model.get("tables", []) or []:
        tname = t.get("name", "?")
        m_parts = [p for p in (t.get("partitions", []) or [])
                   if (p.get("source", {}) or {}).get("type", "m") == "m"]
        for p in m_parts:
            key = tname if len(m_parts) == 1 else "{}.{}".format(tname, p.get("name"))
            out[key] = _text((p.get("source", {}) or {}).get("expression"))
    return out


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 3. filter to real Power Query transformations, then write one .m each ----
all_m = {k: v for k, v in extract_m(model_doc).items() if v and v.strip()}


def is_pq_transform(code):
    """A real transformation query calls Table.* ; raw source reads
    (Sql.Database + navigation), parameters and connection-only functions do not."""
    return bool(re.search(r"\bTable\.[A-Za-z]\w*\s*\(", code))


if transforms_only:
    queries = {k: v for k, v in all_m.items()
               if is_pq_transform(v) and not k.startswith("Errors in ")}
else:
    queries = all_m

dropped = sorted(set(all_m) - set(queries))
if dropped:
    print("skipped {} non-transform quer{}: {}".format(
        len(dropped), "y" if len(dropped) == 1 else "ies", ", ".join(dropped)))
if not queries:
    raise ValueError("no M transformation queries found in '{}'".format(dataset))

# resolve the target lakehouse by name -> absolute abfss path
# (same pattern as nb_generic_layer_load / nb_log_collector; a bare 'Files/...'
#  path does NOT reliably bind to the lakehouse from fs.put)
abfss = mssparkutils.lakehouse.get(target_lakehouse)["properties"]["abfsPath"]
base = "{}/{}".format(abfss, output_subdir.strip().strip("/"))
mssparkutils.fs.mkdirs(base)

_bad = re.compile(r'[\\/:*?"<>|\r\n\t]+')
rows = []
for name in sorted(queries):
    safe = _bad.sub("_", name).strip() or "_"
    code = queries[name]
    code = code if code.endswith("\n") else code + "\n"
    mssparkutils.fs.put("{}/{}.m".format(base, safe), code, overwrite)
    rows.append({"query": name, "file": safe + ".m", "lines": len(code.splitlines())})

import pandas as pd
summary = pd.DataFrame(rows)
print("{} M quer{}  ->  {}".format(len(rows), "y" if len(rows) == 1 else "ies", base))
display(summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
