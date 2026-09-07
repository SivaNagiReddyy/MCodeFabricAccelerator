# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ## nb_llm_client  -  shared LLM client for every agent
# # One reusable factory, **`get_chat_model(...)`**, returning a LangChain chats
# model ready for `deepagents`. Every agent notebook (`nb_m_analyze`,
# `nb_m_to_sql`, later 3/4) does `%run nb_llm_client` then calls this one function,
# so credential handling and model wiring live in exactly one place.
# # The LLM here is reached through an **Anthropic-/OpenAI-compatible gateway**
# (e.g. a LiteLLM proxy in front of Bedrock). You provide three things once -
# `base_url`, `api_key`, `model` - and they are saved **inside Fabric** as a
# JSON file in a lakehouse (`<lakehouse>/Files/config/llm_config.json`), which is
# OneLake-permissioned and is **not** part of the Git repo. No Key Vault needed.
# # ### One-time setup
# Open this notebook, set the `cfg_*` parameters (paste your gateway key into
# `cfg_api_key`), set `write_config = True`, run. It writes `llm_config.json`.
# Set `write_config = False` again afterwards.
# # ### How an agent notebook uses it
# ```python
# %run nb_llm_client
# ```
# ```python
# llm = get_chat_model(
#     config_lakehouse = llm_config_lakehouse,   # where llm_config.json lives
#     config_path      = llm_config_path,
#     temperature      = temperature,
#     max_tokens       = max_tokens,
#     # any of provider / base_url / api_key / model can be passed to override the file
# )
# ```
# `%run` executes this notebook's cells in the caller's session, so
# `get_chat_model` (and `json`, `os`) become available to the caller. The
# `write_config` / `self_test` cells are guarded and do nothing under `%run`.
# # ### Value resolution per field (first wins)
# 1. explicit argument to `get_chat_model(...)`
# 2. the saved `llm_config.json` in the lakehouse
# 3. environment variables - `base_url`: `ANTHROPIC_BASE_URL` / `OPENAI_BASE_URL`;
#    `api_key`: `ANTHROPIC_AUTH_TOKEN` / `ANTHROPIC_API_KEY` / `OPENAI_API_KEY`;
#    `model`: `LLM_MODEL`
# # ### Prerequisites
# the **py-packages** environment attached - `langchain-anthropic` for
# `provider="anthropic"` (native Claude, default), `langchain-openai` for
# `provider="openai"` (OpenAI-compatible route).

# PARAMETERS CELL ********************

# where the saved config lives (OneLake, not Git)
LLM_CONFIG_LAKEHOUSE = "HYDRA_BRONZE_LK"
LLM_CONFIG_PATH      = "Files/config/llm_config.json"

# --- one-time writer: set these, set write_config = True, run once -----
write_config    = False
cfg_provider    = "anthropic"    # "anthropic" (native Claude, via <base>/v1/messages) | "openai" (OpenAI-compatible route)
cfg_base_url    = "https://litellm.path.app.presidio.com"   # gateway root (no trailing /v1)
cfg_api_key     = ""             # <-- paste the gateway key (sk-...) here, only when write_config = True
cfg_model       = "533387313095/us.claude-sonnet-4-6"
cfg_temperature = 0
cfg_max_tokens  = 8192

# --- standalone smoke test (ignored under %run) --------------------
self_test = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- the shared factory ------------------------------------------------
import json
import os


def _fsutil():
    try:
        import notebookutils
        return notebookutils
    except Exception:                                # noqa: BLE001
        return mssparkutils                          # legacy alias  # noqa: F821


def _lakehouse_abfss(name):
    return _fsutil().lakehouse.get(name)["properties"]["abfsPath"]


def _config_full_path(lakehouse, path):
    return f"{_lakehouse_abfss(lakehouse)}/{path.strip().strip('/')}"


def read_llm_config(lakehouse=None, path=None):
    """Return the saved llm_config.json as a dict, or {} if it is not there yet."""
    lakehouse = lakehouse or LLM_CONFIG_LAKEHOUSE
    path = path or LLM_CONFIG_PATH
    try:
        txt = _fsutil().fs.head(_config_full_path(lakehouse, path), 64 * 1024)
        return json.loads(txt)
    except Exception as e:                           # noqa: BLE001
        print(f"[nb_llm_client] no config at {lakehouse}/{path} ({type(e).__name__}); "
              "using explicit args / env vars")
        return {}


def write_llm_config(payload, lakehouse=None, path=None):
    """Persist the gateway details as JSON inside a lakehouse (OneLake, not Git)."""
    lakehouse = lakehouse or LLM_CONFIG_LAKEHOUSE
    path = path or LLM_CONFIG_PATH
    missing = [k for k in ("base_url", "api_key", "model") if not payload.get(k)]
    if missing:
        raise ValueError(f"[nb_llm_client] cannot write config - missing: {', '.join(missing)}")
    full = _config_full_path(lakehouse, path)
    _fsutil().fs.mkdirs(full.rsplit("/", 1)[0])
    _fsutil().fs.put(full, json.dumps(payload, indent=2) + "\n", True)
    k = payload["api_key"]
    print(f"[nb_llm_client] wrote {lakehouse}/{path}  "
          f"(provider={payload.get('provider', 'anthropic')}, model={payload['model']}, "
          f"key=***{k[-4:] if len(k) >= 4 else '?'})")
    return full


_ENV_FALLBACK = {
    "base_url": ("ANTHROPIC_BASE_URL", "OPENAI_BASE_URL", "OPENAI_API_BASE"),
    "api_key":  ("ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_API_KEY", "OPENAI_API_KEY"),
    "model":    ("LLM_MODEL",),
}


def _env_value(field):
    for name in _ENV_FALLBACK.get(field, ()):
        if os.environ.get(name):
            return os.environ[name]
    return None


def get_chat_model(
    *,
    config_lakehouse=None,
    config_path=None,
    provider=None,
    base_url=None,
    api_key=None,
    model=None,
    temperature=None,
    max_tokens=None,
    max_retries=2,
    timeout=120,
    allow_openai_fallback=False,
    verbose=True,
    **model_kwargs,
):
    """Build a LangChain chat model for the gateway LLM, ready for deepagents.

    Each of provider / base_url / api_key / model / temperature / max_tokens is
    taken from (in order): the explicit argument here, the saved
    `llm_config.json`, then an environment variable (see notebook header).
    Returns a `ChatAnthropic` (provider="anthropic", default) or `ChatOpenAI`
    (provider="openai").
    """
    cfg = read_llm_config(config_lakehouse, config_path)

    def pick(name, arg, default=None):
        if arg is not None:
            return arg
        if name in cfg and cfg[name] not in (None, ""):
            return cfg[name]
        env = _env_value(name)
        return env if env is not None else default

    provider = (provider or cfg.get("provider") or "anthropic").lower()
    base_url = pick("base_url", base_url)
    api_key = pick("api_key", api_key)
    model = pick("model", model)
    temperature = pick("temperature", temperature, 0)
    max_tokens = pick("max_tokens", max_tokens, 8192)

    if not (base_url and api_key and model):
        raise RuntimeError(
            "[nb_llm_client] missing base_url / api_key / model - run this notebook once "
            "with write_config = True, or pass them explicitly to get_chat_model().")

    base_url = base_url.rstrip("/")
    common = dict(model=model, api_key=api_key, base_url=base_url,
                  temperature=temperature, max_tokens=max_tokens,
                  max_retries=max_retries, **model_kwargs)

    if provider == "anthropic":
        try:
            from langchain_anthropic import ChatAnthropic
        except ModuleNotFoundError:
            if not allow_openai_fallback:
                raise RuntimeError(
                    "[nb_llm_client] langchain-anthropic is not installed in this session.\n"
                    "  preferred : republish the py-packages environment (it pins\n"
                    "              langchain-anthropic) and re-attach it to this notebook.\n"
                    "  right now : put  %pip install langchain-anthropic  in the FIRST cell\n"
                    "              of the calling notebook, run it, then re-run.\n"
                    "  last resort: get_chat_model(..., allow_openai_fallback=True) uses the\n"
                    "              gateway's OpenAI-compatible route instead - not recommended\n"
                    "              for a Claude model driving deepagents (tool calls get\n"
                    "              translated through the OpenAI schema)."
                ) from None
            print("[nb_llm_client] langchain-anthropic missing and allow_openai_fallback=True "
                  "- using the OpenAI-compatible route")
            provider = "openai"
        else:
            # default_request_timeout, not timeout - without it the Anthropic SDK
            # waits ~10 min per attempt, so a blocked call looks like a hang.
            llm = ChatAnthropic(default_request_timeout=timeout,
                                **common)           # hits <base_url>/v1/messages

    if provider in ("openai", "openai_compatible", "litellm"):
        from langchain_openai import ChatOpenAI
        llm = ChatOpenAI(timeout=timeout,
                         **common)                   # hits <base_url>/chat/completions
    elif provider != "anthropic":
        raise ValueError(f"[nb_llm_client] unknown provider {provider!r} (use 'anthropic' or 'openai')")

    if verbose:
        print(f"[nb_llm_client] {provider} chat model ready: model={model} "
              f"base_url={base_url} key=***{api_key[-4:] if len(api_key) >= 4 else '?'}")
    return llm

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- one-time: save the gateway details into the lakehouse -----------
# set the cfg_* parameters above + write_config = True, run this once, then
# set write_config = False. The key never enters the Git repo.
if write_config:
    write_llm_config({
        "provider": cfg_provider,
        "base_url": cfg_base_url,
        "api_key": cfg_api_key,
        "model": cfg_model,
        "temperature": cfg_temperature,
        "max_tokens": cfg_max_tokens,
    })
else:
    _existing = read_llm_config()
    if _existing:
        print(f"[nb_llm_client] config present: provider={_existing.get('provider')} "
              f"model={_existing.get('model')} base_url={_existing.get('base_url')}")
    else:
        print("[nb_llm_client] no config yet - set cfg_* + write_config = True and re-run")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- smoke test (only when run directly; %run leaves self_test = False) ----
if self_test:
    _llm = get_chat_model()
    print("invoke ->", _llm.invoke("Reply with the single word: OK").content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
