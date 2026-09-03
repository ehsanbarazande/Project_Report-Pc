import os
import time
from pathlib import Path
from threading import Lock
from collections import defaultdict
from typing import Optional
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
PARQUET_DIR = BASE_DIR / "parquet_data"
_VERSION_FILE = PARQUET_DIR / ".data_version"

# Cache for dataframe reads
_CACHE = {}
_LOCK = Lock()

# File map from app initialization
_FILE_MAP = {}

# Cache for grouped docs used by changed-docs endpoint
_GROUPED_DOCS_CACHE = None
_GROUPED_DOCS_LOCK = Lock()


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _parquet_path(name: str) -> Path:
    return PARQUET_DIR / f"{name}.parquet"


def _read_excel_normalized(excel_path: Path) -> pd.DataFrame:
    df = pd.read_excel(excel_path, engine="openpyxl")
    return _normalize_columns(df)


def _write_parquet(df: pd.DataFrame, name: str):
    parquet_path = _parquet_path(name)
    try:
        df.to_parquet(parquet_path, index=False, engine="pyarrow", compression="snappy")
    except Exception as e:
        # ===== مقاوم‌سازی: بعضی فایل‌های اکسل تو یک ستون هم عدد هم متن
        # دارن (مثلاً External Revision یا Issued Date) که pyarrow نمی‌تونه
        # یک نوعِ واحد براشون تشخیص بده و کل تبدیل رو با خطا متوقف می‌کنه.
        # به‌جای کرش کردن، ستون‌های object (ترکیبی) رو به رشته یکسان‌سازی
        # می‌کنیم (بدون دست‌زدن به NaN واقعی) و دوباره تلاش می‌کنیم.
        print(f"[WARN] تبدیل مستقیم {name} به Parquet شکست خورد ({e})؛ در حال یکسان‌سازی نوع ستون‌ها و تلاش مجدد...")
        safe_df = df.copy()
        for col in safe_df.columns:
            if safe_df[col].dtype == object:
                safe_df[col] = safe_df[col].apply(lambda v: v if pd.isna(v) else str(v))
        safe_df.to_parquet(parquet_path, index=False, engine="pyarrow", compression="snappy")
        print(f"[INFO] تبدیل {name} با یکسان‌سازی نوع ستون‌ها موفق شد.")


def _needs_rebuild(target_path: Path, source_paths: list) -> bool:
    """
    Returns True if target parquet does not exist
    or any source file is newer than target.
    """
    if not target_path.exists():
        return True

    target_mtime = os.path.getmtime(target_path)
    for source_path in source_paths:
        if source_path is None:
            continue
        p = Path(source_path)
        if p.exists() and os.path.getmtime(p) > target_mtime:
            return True

    return False


def _combine_datasets(dataset_names: list[str]) -> pd.DataFrame:
    frames = []

    for name in dataset_names:
        parquet_path = _parquet_path(name)
        if parquet_path.exists():
            df = pd.read_parquet(parquet_path, engine="pyarrow")
            df = _normalize_columns(df)
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True, sort=False)


def _write_data_version(token: Optional[str] = None) -> str:
    PARQUET_DIR.mkdir(exist_ok=True)
    if not token:
        token = str(int(time.time() * 1000))
    _VERSION_FILE.write_text(token, encoding="utf-8")
    return token


def _ensure_data_version() -> str:
    if _VERSION_FILE.exists():
        token = _VERSION_FILE.read_text(encoding="utf-8").strip()
        if token:
            return token
    mtimes = [os.path.getmtime(p) for p in PARQUET_DIR.glob("*.parquet")]
    token = str(int(max(mtimes))) if mtimes else str(int(time.time()))
    return _write_data_version(token)


def initialize(file_map: dict):
    """
    Build/update parquet files from excel sources if needed.
    Also builds aggregate parquet files used by app.
    """
    global _FILE_MAP
    _FILE_MAP = dict(file_map)
    PARQUET_DIR.mkdir(exist_ok=True)

    rebuilt = False
    dataset_names = ["person", "master", "history", "vendor_master", "vendor_history"]

    for name in dataset_names:
        excel_path = _FILE_MAP.get(name)
        parquet_path = _parquet_path(name)

        if excel_path is None:
            continue

        excel_path = Path(excel_path)
        if not excel_path.exists():
            continue

        if _needs_rebuild(parquet_path, [excel_path]):
            print(f"[INFO] در حال تبدیل {excel_path.name} به Parquet...")
            try:
                df = _read_excel_normalized(excel_path)
                _write_parquet(df, name)
                print(f"[INFO] ذخیره شد: {parquet_path.name}")
                rebuilt = True
            except Exception as e:
                print(f"[ERROR] در تبدیل {excel_path.name} به Parquet: {e}")

    # Optional aggregate parquet files
    aggregate_specs = {
        "all_docs": ["master", "vendor_master"],
        "all_history": ["history", "vendor_history"],
    }

    for aggregate_name, source_names in aggregate_specs.items():
        aggregate_path = _parquet_path(aggregate_name)
        source_excel_paths = [_FILE_MAP.get(name) for name in source_names if _FILE_MAP.get(name)]

        if _needs_rebuild(aggregate_path, source_excel_paths):
            combined_df = _combine_datasets(source_names)
            _write_parquet(combined_df, aggregate_name)
            print(f"[INFO] ذخیره شد: {aggregate_path.name}")
            rebuilt = True

    if rebuilt:
        clear_cache()
        _write_data_version()
    else:
        _ensure_data_version()


def get_dataframe(name: str, columns=None) -> pd.DataFrame:
    """
    Read parquet with cache validation by file mtime.
    """
    parquet_path = _parquet_path(name)
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found for dataset '{name}': {parquet_path}")

    cols_key = tuple(columns) if columns else None
    cache_key = (name, cols_key)
    parquet_mtime = os.path.getmtime(parquet_path)
    now = time.time()

    with _LOCK:
        item = _CACHE.get(cache_key)
        if item and item["mtime"] == parquet_mtime:
            return item["df"]

        df = pd.read_parquet(parquet_path, columns=columns, engine="pyarrow")
        df = _normalize_columns(df)

        _CACHE[cache_key] = {
            "df": df,
            "mtime": parquet_mtime,
            "ts": now,
        }
        return df


def get_grouped_docs() -> dict:
    """
    Returns:
        {
          "DOC-001": [version1_dict, version2_dict, ...],
          "DOC-002": [...],
          ...
        }

    Data sources:
      master + history + vendor_master + vendor_history
    Cached in memory for fast repeated access.
    """
    global _GROUPED_DOCS_CACHE

    with _GROUPED_DOCS_LOCK:
        if _GROUPED_DOCS_CACHE is not None:
            return _GROUPED_DOCS_CACHE

        frames = []
        for name in ["master", "history", "vendor_master", "vendor_history"]:
            parquet_path = _parquet_path(name)
            if parquet_path.exists():
                try:
                    frames.append(get_dataframe(name))
                except Exception as e:
                    print(f"[WARN] خطا در بارگذاری {name}.parquet: {e}")

        if not frames:
            _GROUPED_DOCS_CACHE = {}
            return _GROUPED_DOCS_CACHE

        combined_df = pd.concat(frames, ignore_index=True, sort=False)

        grouped = defaultdict(list)
        for rec in combined_df.to_dict(orient="records"):
            doc_no = str(rec.get("document_no", "")).strip()
            if doc_no and doc_no.lower() != "nan":
                grouped[doc_no].append(rec)

        _GROUPED_DOCS_CACHE = dict(grouped)
        return _GROUPED_DOCS_CACHE


def get_data_version() -> str:
    """
    نسخه داده فقط وقتی عوض می‌شود که parquet واقعاً از روی Excel بازسازی شده باشد.
    این مقدار برای کلید کش استفاده می‌شود تا کش با مرور زمان یا ری‌استارت سرور باطل نشود.
    """
    try:
        return _ensure_data_version()
    except Exception:
        return str(int(time.time()))


def clear_cache():
    """
    Clear all in-memory caches.
    Call this after upload/replace/rebuild operations.
    """
    global _GROUPED_DOCS_CACHE

    with _LOCK:
        _CACHE.clear()

    with _GROUPED_DOCS_LOCK:
        _GROUPED_DOCS_CACHE = None
