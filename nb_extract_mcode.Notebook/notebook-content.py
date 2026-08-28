# Fabric notebook source


# MARKDOWN ********************

# ## nb_extract_mcode
# # Extracts every Power Query (**M**) query from a semantic model into one file
# per query, named after the query:  `<output_dir>/<QueryName>.m`
# # Model-agnostic - pass any semantic model name. It pulls the model definition
# as TMSL/BIM (via Semantic Link) and reads:
#   * `model.expressions[]`  - parameters, functions, shared/staging queries
#   * `model.tables[].partitions[]` where `source.type == "m"`  - table queries
# # Direct Lake / calculated / DAX partitions have no M and are skipped.

# PARAMETERS CELL ********************

dataset    = "HydraReport"        # semantic model: display name or GUID
workspace  = ""                   # blank = current workspace; else name or GUID
output_dir = "Files/m_extract"    # lakehouse-relative path or abfss://... ; blank = don't write, just list
overwrite  = True

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
    # Semantic Link Labs (present when the 'py-packages' environment is attached)
    try:
        import sempy_labs as labs
        bim = labs.get_semantic_model_bim(dataset=dataset, workspace=ws)
        return bim if isinstance(bim, dict) else json.loads(bim)
    except Exception as e:
        print("sempy_labs path unavailable, falling back to sempy.fabric:", e)
    # core Semantic Link - available on the base Fabric runtime
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

# ---- 3. write one <QueryName>.m per query -------------------------------
import notebookutils

queries = {k: v for k, v in extract_m(model_doc).items() if v and v.strip()}
if not queries:
    raise ValueError("no M queries found in '{}' (Direct Lake / DAX-only model?)".format(dataset))

_bad = re.compile(r'[\\/:*?"<>|\r\n\t]+')
rows = []
for name in sorted(queries):
    safe = _bad.sub("_", name).strip() or "_"
    code = queries[name]
    code = code if code.endswith("\n") else code + "\n"
    if output_dir.strip():
        notebookutils.fs.put("{}/{}.m".format(output_dir.rstrip("/"), safe), code, overwrite)
    rows.append({"query": name, "file": safe + ".m", "lines": len(code.splitlines())})

import pandas as pd
summary = pd.DataFrame(rows)
where = "written to {}/".format(output_dir.rstrip("/")) if output_dir.strip() else "(output_dir blank - not written)"
print("{} M quer{} {}".format(len(rows), "y" if len(rows) == 1 else "ies", where))
display(summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
