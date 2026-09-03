"""
ایندکس متنی قرارداد/الحاقیه‌های هر پروژه (PDF/Word/اکسل)، برای پرسش‌های
متنی («طبق قرارداد مسئولیت تأخیر با کیه؟»). فقط برای سؤال‌های کیفی/متنی
استفاده می‌شود — هیچ عدد مالی (مبلغ، تاریخ) از این‌جا استخراج نمی‌شود؛
اعداد همیشه از فایل project_finance.py (ساختاریافته) می‌آیند.

هر پاسخی که از این ایندکس ساخته می‌شود باید همراه با هشدار «حتماً خودِ
سند را چک کنید» باشد — استخراج متن از PDF قابل‌اتکای صددرصد نیست.
"""
from __future__ import annotations

import re
from pathlib import Path

import document_knowledge as dk

BASE_DIR = Path(__file__).resolve().parent
CONTRACTS_DIR = BASE_DIR / "contract_docs"

_INDEX_CACHE: dict = {}


def _safe_project_dirname(project: str) -> str:
    safe = "".join(c for c in (project or "") if c.isalnum() or c in (" ", "-", "_")).strip()
    return safe or "unknown"


def project_dir(project: str) -> Path:
    return CONTRACTS_DIR / _safe_project_dirname(project)


def ensure_project_dir(project: str) -> Path:
    d = project_dir(project)
    d.mkdir(parents=True, exist_ok=True)
    return d


def _file_token(project: str) -> str:
    d = project_dir(project)
    if not d.exists():
        return "missing"
    parts = []
    for path in d.rglob("*"):
        if path.is_file():
            try:
                st = path.stat()
                parts.append(f"{path.name}:{int(st.st_mtime)}:{st.st_size}")
            except OSError:
                continue
    return "|".join(sorted(parts)) or "empty"


def build_index(project: str, force: bool = False) -> dict:
    token = _file_token(project)
    cached = _INDEX_CACHE.get(project)
    if not force and cached and cached["mtime_token"] == token:
        return {"ready": True, "files": len({c["path"] for c in cached["chunks"]}), "chunks": len(cached["chunks"])}

    d = project_dir(project)
    chunks = []
    if d.exists():
        for path in d.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in dk.INDEXED_EXT:
                continue
            ext = path.suffix.lower()
            rel = str(path.relative_to(d))
            text = ""
            if ext in dk.SUPPORTED_TEXT:
                text = dk._read_text_file(path)
            elif ext == ".pdf":
                text = dk._read_pdf(path)
            elif ext == ".docx":
                text = dk._read_docx(path)
            elif ext in {".xlsx", ".xls"}:
                text = dk._read_excel(path)

            pieces = dk._chunk_text(text) or [f"فایل {rel} ایندکس شد ولی متن قابل استخراج نبود."]
            for idx, piece in enumerate(pieces):
                chunks.append({"path": rel, "ext": ext, "chunk": idx, "text": piece})

    _INDEX_CACHE[project] = {"mtime_token": token, "chunks": chunks}
    return {"ready": True, "files": len({c["path"] for c in chunks}), "chunks": len(chunks)}


def search(project: str, question: str, limit: int = 6) -> list:
    build_index(project)
    tokens = [t for t in re.findall(r"[\w\u0600-\u06FF]{2,}", (question or "").lower()) if t]
    chunks = _INDEX_CACHE.get(project, {}).get("chunks", [])
    scored = []
    for chunk in chunks:
        hay = f"{chunk['path']} {chunk['text']}".lower()
        score = sum(hay.count(t) for t in tokens) if tokens else 0
        if not tokens:
            score = 1
        if score > 0:
            scored.append((score, chunk))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [c for _, c in scored[:limit]]


def list_files(project: str) -> list:
    d = project_dir(project)
    if not d.exists():
        return []
    out = []
    for path in d.rglob("*"):
        if path.is_file():
            try:
                st = path.stat()
                out.append({"name": path.name, "size": st.st_size})
            except OSError:
                continue
    return out
