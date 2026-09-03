"""ایندکس متنی مدارک مهندسی داخل پوشه engineering_docs برای پرسش از Qwen."""
from __future__ import annotations

import os
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DOCS_DIR = BASE_DIR / "engineering_docs"

SUPPORTED_TEXT = {".txt", ".md", ".csv", ".dxf"}
SUPPORTED_OFFICE = {".xlsx", ".xls", ".docx"}
SUPPORTED_PDF = {".pdf"}
INDEXED_EXT = SUPPORTED_TEXT | SUPPORTED_OFFICE | SUPPORTED_PDF | {".dwg"}

_INDEX = {"mtime_token": None, "chunks": []}


def _read_text_file(path: Path) -> str:
    for encoding in ("utf-8", "cp1256", "latin-1"):
        try:
            return path.read_text(encoding=encoding, errors="ignore")
        except Exception:
            continue
    return ""


def _read_pdf(path: Path) -> str:
    try:
        from pypdf import PdfReader
        reader = PdfReader(str(path))
        return "\n".join((page.extract_text() or "") for page in reader.pages)
    except Exception:
        pass
    try:
        from PyPDF2 import PdfReader
        reader = PdfReader(str(path))
        return "\n".join((page.extract_text() or "") for page in reader.pages)
    except Exception:
        return ""


def _read_docx(path: Path) -> str:
    try:
        import docx
        document = docx.Document(str(path))
        return "\n".join(p.text for p in document.paragraphs if p.text)
    except Exception:
        return ""


def _read_excel(path: Path) -> str:
    try:
        import pandas as pd
        frames = pd.read_excel(path, sheet_name=None, engine="openpyxl")
        parts = []
        for sheet, df in (frames or {}).items():
            parts.append(f"[برگه {sheet}]")
            parts.append(df.head(80).to_csv(index=False))
        return "\n".join(parts)
    except Exception:
        return ""


def _chunk_text(text: str, size: int = 900) -> list:
    text = re.sub(r"\s+", " ", (text or "")).strip()
    if not text:
        return []
    return [text[i:i + size] for i in range(0, len(text), size)][:12]


def _file_token() -> str:
    if not DOCS_DIR.exists():
        return "missing"
    parts = []
    for path in DOCS_DIR.rglob("*"):
        if path.is_file():
            try:
                st = path.stat()
                parts.append(f"{path.name}:{int(st.st_mtime)}:{st.st_size}")
            except OSError:
                continue
    return "|".join(sorted(parts)) or "empty"


def build_index(force: bool = False) -> dict:
    token = _file_token()
    if not force and _INDEX["mtime_token"] == token:
        return {"ready": True, "files": len({c['path'] for c in _INDEX['chunks']}), "chunks": len(_INDEX['chunks'])}

    chunks = []
    if DOCS_DIR.exists():
        for path in DOCS_DIR.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in INDEXED_EXT:
                continue
            ext = path.suffix.lower()
            rel = str(path.relative_to(DOCS_DIR))
            text = ""
            note = ""
            if ext in SUPPORTED_TEXT:
                text = _read_text_file(path)
            elif ext == ".pdf":
                text = _read_pdf(path)
            elif ext == ".docx":
                text = _read_docx(path)
            elif ext in {".xlsx", ".xls"}:
                text = _read_excel(path)
            elif ext == ".dwg":
                note = "فایل AutoCAD دودویی است؛ فقط نام و مسیر ایندکس شده."

            pieces = _chunk_text(text) or ([note] if note else [f"فایل {rel} ایندکس شد ولی متن قابل استخراج نبود."])
            for idx, piece in enumerate(pieces):
                chunks.append({
                    "path": rel,
                    "ext": ext,
                    "chunk": idx,
                    "text": piece,
                })

    _INDEX["mtime_token"] = token
    _INDEX["chunks"] = chunks
    return {"ready": True, "files": len({c['path'] for c in chunks}), "chunks": len(chunks)}


def search(question: str, limit: int = 6) -> list:
    build_index()
    tokens = [t for t in re.findall(r"[\w\u0600-\u06FF]{2,}", (question or "").lower()) if t]
    scored = []
    for chunk in _INDEX["chunks"]:
        hay = f"{chunk['path']} {chunk['text']}".lower()
        score = sum(hay.count(token) for token in tokens) if tokens else 0
        if not tokens:
            score = 1
        if score > 0:
            scored.append((score, chunk))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [item[1] for item in scored[:limit]]


def ensure_docs_dir():
    DOCS_DIR.mkdir(exist_ok=True)
    gitkeep = DOCS_DIR / ".gitkeep"
    if not gitkeep.exists():
        gitkeep.write_text("", encoding="utf-8")
