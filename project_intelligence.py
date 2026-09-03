"""
تحلیلگر پروژه برای داشبورد مدارک.

منبع حقیقت: خروجی integrate_data / آمار داشبورد (پایتون).
مدل محلی روی سیستم‌های بدون GPU برای پرسش‌وپاسخ قابل اتکا نیست؛
سؤال‌ها با بازیابی ساخت‌یافته از همین داده‌ها جواب داده می‌شوند.
"""
from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import org_structure
import person_activity

OLLAMA_URL = "http://127.0.0.1:11434/api/chat"
OLLAMA_MODEL = "qwen2.5:7b"
OLLAMA_TIMEOUT = 8
USE_LLM = os.environ.get("INTEL_USE_LLM", "").strip().lower() in ("1", "true", "yes")

ROLE_FA = {
    "specialist": "کارشناس",
    "senior": "مهندس ارشد",
    "manager": "مدیر مهندسی",
    "mdj": "کارشناس",
    "mds": "مهندس ارشد",
    "em": "مدیر مهندسی",
    "procurement": "تدارکات",
    "pmo": "دفتر مدیریت پروژه",
    "ceo": "مدیرعامل",
    "dcc": "کنترل مدارک",
    "engineer": "مهندس",
    "ctr": "پیمانکار",
}

DISCIPLINE_ALIASES = {
    "process": "process",
    "فرایند": "process",
    "فرآیند": "process",
    "پروسس": "process",
    "mechanical": "mechanical",
    "مکانیک": "mechanical",
    "مکانیکال": "mechanical",
    "electrical": "electrical",
    "برق": "electrical",
    "الکتریکال": "electrical",
    "safety": "safety",
    "ایمنی": "safety",
    "piping": "piping",
    "پایپینگ": "piping",
    "instrument": "instrument",
    "ابزار دقیق": "instrument",
    "ابزاردقیق": "instrument",
    "civil": "civil",
    "سیویل": "civil",
    "عمران": "civil",
    "سازه": "civil",
    "structure": "civil",
    "architecture": "architecture",
    "معماری": "architecture",
    "معمار": "architecture",
    "management": "management",
    "مدیریت": "management",
}

GREETINGS = (
    "سلام", "درود", "hello", "hi", "hey", "صبح بخیر", "عصر بخیر",
    "خوبی", "چطوری", "سلام علیکم",
)


def _is_yes(value) -> bool:
    return str(value).strip().lower() == "yes"


def _isna(value) -> bool:
    try:
        return value is None or pd.isna(value)
    except (ValueError, TypeError):
        return value is None


def _round_score(value) -> int:
    try:
        return int(round(float(value or 0)))
    except (TypeError, ValueError):
        return 0


def _fold(text) -> str:
    value = str(text or "").strip().lower()
    for src, dst in (("آ", "ا"), ("ي", "ی"), ("ك", "ک"), ("‌", ""), (" ", ""), ("ـ", "")):
        value = value.replace(src, dst)
    return value


def _latest_docs(integrated: dict) -> list:
    latest = {}
    for doc in (integrated.get("master_docs") or []) + (integrated.get("vendor_docs") or []):
        if not isinstance(doc, dict):
            continue
        doc_no = doc.get("document_no")
        if not doc_no:
            continue
        doc_date = doc.get("date")
        prev = latest.get(doc_no)
        if prev is None:
            latest[doc_no] = doc
            continue
        prev_date = prev.get("date")
        if _isna(prev_date) or (not _isna(doc_date) and doc_date > prev_date):
            latest[doc_no] = doc
    return list(latest.values())


def _health_score(total: int, overdue: int, hold: int, approved: int) -> Tuple[int, str]:
    if total <= 0:
        return 50, "unknown"
    overdue_rate = overdue / total
    hold_rate = hold / total
    approved_rate = approved / total
    score = 100 - (overdue_rate * 55) - (hold_rate * 20) - max(0.0, (0.35 - approved_rate) * 25)
    score = int(max(0, min(100, round(score))))
    if score >= 80:
        return score, "good"
    if score >= 60:
        return score, "fair"
    return score, "attention"


def _status_label(status: str) -> str:
    return {
        "good": "خوب",
        "fair": "نیازمند پیگیری",
        "attention": "نیازمند توجه",
        "unknown": "نامشخص",
    }.get(status, "نامشخص")


def _match_doc_type(doc, doc_type):
    if not doc_type or doc_type in ("همه", "all", "both"):
        return True
    actual = str(doc.get("doc_type") or "").strip().upper()
    if doc_type in ("اصلی", "engineering", "MASTER"):
        return actual == "MASTER"
    if doc_type in ("وندور", "vendor", "VENDOR"):
        return actual == "VENDOR"
    return True


def _doc_type_label(doc_type):
    if doc_type in ("وندور", "vendor", "VENDOR"):
        return "مدارک وندور"
    return "مدارک مهندسی"


def _type_label(doc_type: str) -> str:
    if doc_type == "client":
        return "کارفرما"
    if doc_type in ("contractor", "vendor"):
        return "پیمانکار"
    return doc_type or "نامشخص"


def _filter_overdue(rows, doc_type: str) -> list:
    return [d for d in (rows or []) if isinstance(d, dict) and _match_doc_type(d, doc_type)]


def _compact_overdue(doc) -> dict:
    days = int(doc.get("days") or 0)
    kind = str(doc.get("type") or "")
    return {
        "document_no": doc.get("document_no"),
        "document_title": doc.get("document_title") or "",
        "project": str(doc.get("project") or "نامشخص").strip() or "نامشخص",
        "discipline": org_structure.canonicalize_discipline(doc.get("discipline")),
        "days": days,
        "type": kind,
        "type_label": _type_label(kind),
        "responsible": doc.get("responsible") or "",
        "over_150": bool(doc.get("over_150")),
    }


def _compact_people(people: list) -> list:
    compact = []
    for person in people or []:
        if not isinstance(person, dict):
            continue
        compact.append({
            "name": person.get("name") or "",
            "persian_name": person.get("persian_name") or "",
            "aliases": person.get("aliases") or [],
            "discipline": person.get("discipline") or "",
            "org_role": person.get("org_role") or "",
            "org_role_label": person.get("org_role_label") or "",
            "score_role": person.get("score_role") or "",
            "score_role_label": person.get("score_role_label") or "",
            "total_score": _round_score(person.get("total_score")),
            "core_score": person.get("core_score"),
            "on_time_score": person.get("on_time_score"),
            "quality_score": person.get("quality_score"),
            "speed_score": person.get("speed_score"),
            "volume_score": person.get("volume_score"),
            "n_revisions_touched": person.get("n_revisions_touched") or 0,
            "delay_causes_count": person.get("delay_causes_count") or 0,
            "avg_revisions_needed": person.get("avg_revisions_needed"),
            "avg_duration_days": person.get("avg_duration_days"),
            "peer_avg_duration": person.get("peer_avg_duration"),
            "role_budget_days": person.get("role_budget_days"),
            "max_revisions_in_role": person.get("max_revisions_in_role") or 0,
            "is_combined_role": bool(person.get("is_combined_role")),
            "peer_group": person.get("peer_group") or "",
            "shared_distribute_points": person.get("shared_distribute_points") or 0,
            "shared_prediction_points": person.get("shared_prediction_points") or 0,
            "assignments": person.get("assignments") or [],
        })
    return compact


def build_facts(integrated: dict, doc_type: str = "اصلی", extras: dict = None) -> dict:
    """حقایق فشرده و قابل‌اتکا برای UI و پرسش ساخت‌یافته."""
    extras = extras or {}
    stats = integrated.get("stats") or {}

    client_docs = _filter_overdue(integrated.get("overdue_client"), doc_type)
    contractor_docs = _filter_overdue(integrated.get("overdue_contractor"), doc_type)
    contractor_all = _filter_overdue(integrated.get("overdue_contractor_all"), doc_type)
    contractor_over_150 = [d for d in contractor_all if d.get("over_150")]
    table_overdue = client_docs + contractor_docs
    hold_docs = _filter_overdue(integrated.get("hold_docs"), doc_type)
    latest_docs = [
        d for d in _latest_docs(integrated)
        if not _is_yes(d.get("deleted", "")) and _match_doc_type(d, doc_type)
        and not org_structure.is_excluded_discipline(d.get("discipline"))
    ]

    overdue_by_project = defaultdict(lambda: {"client": 0, "contractor": 0, "over_150": 0})
    overdue_by_discipline = defaultdict(lambda: {"client": 0, "contractor": 0, "total": 0})
    for doc in table_overdue:
        project = str(doc.get("project") or "نامشخص").strip() or "نامشخص"
        discipline = org_structure.canonicalize_discipline(doc.get("discipline"))
        if org_structure.is_excluded_discipline(discipline):
            continue
        kind = "client" if doc.get("type") == "client" else "contractor"
        overdue_by_project[project][kind] += 1
        overdue_by_discipline[discipline][kind] += 1
        overdue_by_discipline[discipline]["total"] += 1
    for doc in contractor_over_150:
        project = str(doc.get("project") or "نامشخص").strip() or "نامشخص"
        overdue_by_project[project]["over_150"] += 1

    hold_by_project = defaultdict(int)
    for doc in hold_docs:
        hold_by_project[str(doc.get("project") or "نامشخص").strip() or "نامشخص"] += 1

    project_totals = defaultdict(lambda: {"total": 0, "approved": 0, "hold": 0})
    discipline_totals = defaultdict(int)
    for doc in latest_docs:
        project = str(doc.get("project") or "نامشخص").strip() or "نامشخص"
        discipline = org_structure.canonicalize_discipline(doc.get("discipline"))
        if org_structure.is_excluded_discipline(discipline):
            continue
        project_totals[project]["total"] += 1
        discipline_totals[discipline] += 1
        if _is_yes(doc.get("hold", "")):
            project_totals[project]["hold"] += 1
        responsible = str(doc.get("responsible") or "").lower()
        progress = doc.get("doc_progress", 0) or 0
        try:
            progress_num = float(progress)
        except (TypeError, ValueError):
            progress_num = 0
        if "finished" in responsible and progress_num == 100:
            project_totals[project]["approved"] += 1

    projects = []
    for name, bucket in project_totals.items():
        split = overdue_by_project.get(name) or {"client": 0, "contractor": 0, "over_150": 0}
        overdue = split["client"] + split["contractor"]
        hold = bucket["hold"] or hold_by_project.get(name, 0)
        score, status = _health_score(bucket["total"], overdue, hold, bucket["approved"])
        projects.append({
            "name": name,
            "total": bucket["total"],
            "overdue": overdue,
            "overdue_client": split["client"],
            "overdue_contractor": split["contractor"],
            "overdue_over_150": split["over_150"],
            "hold": hold,
            "approved": bucket["approved"],
            "health": score,
            "status": status,
            "status_label": _status_label(status),
        })
    projects.sort(key=lambda item: (item["overdue"], -item["health"]), reverse=True)

    overall_total = len(latest_docs)
    overall_overdue = len(table_overdue)
    overall_hold = len(hold_docs)
    overall_approved = 0
    for d in latest_docs:
        try:
            progress_num = float(d.get("doc_progress") or 0)
        except (TypeError, ValueError):
            progress_num = 0
        if "finished" in str(d.get("responsible") or "").lower() and progress_num == 100:
            overall_approved += 1
    overall_score, overall_status = _health_score(
        overall_total, overall_overdue, overall_hold, overall_approved
    )

    overdue_index = [_compact_overdue(d) for d in table_overdue]
    overdue_index.sort(key=lambda d: d["days"], reverse=True)

    alerts = []
    critical_overdue = sum(1 for d in overdue_index if d["days"] >= 30)
    if critical_overdue:
        alerts.append({
            "level": "red",
            "text": f"{critical_overdue} مدرک بیش از ۳۰ روز تأخیر دارند.",
        })
    if projects:
        worst = projects[0]
        if worst["overdue"] > 0:
            alerts.append({
                "level": "orange",
                "text": (
                    f"پروژه {worst['name']}: {worst['overdue_client']} مدرک کارفرما "
                    f"و {worst['overdue_contractor']} مدرک پیمانکار تأخیر دارند."
                ),
            })
    top_disc = sorted(overdue_by_discipline.items(), key=lambda kv: kv[1]["total"], reverse=True)
    if top_disc and top_disc[0][1]["total"] > 0:
        name, counts = top_disc[0]
        alerts.append({
            "level": "orange",
            "text": (
                f"بیشترین تأخیر مربوط به دیسیپلین {name} است "
                f"({counts['client']} کارفرما، {counts['contractor']} پیمانکار)."
            ),
        })
    best_projects = [p for p in projects if p["status"] == "good"]
    if best_projects:
        best = max(best_projects, key=lambda p: p["health"])
        alerts.append({
            "level": "green",
            "text": f"پروژه {best['name']} با امتیاز سلامت {best['health']} وضعیت نسبتاً پایداری دارد.",
        })
    if contractor_over_150:
        alerts.append({
            "level": "red",
            "text": f"{len(contractor_over_150)} مدرک پیمانکار بیش از ۱۵۰ روز تأخیر دارند و جدا از جدول ۷ روزه شمرده می‌شوند.",
        })
    if not alerts:
        alerts.append({
            "level": "green",
            "text": "در حال حاضر مورد بحرانی در مدارک مشاهده نشد.",
        })

    inbox_people = []
    inbox = integrated.get("inbox_stats") or {}
    if isinstance(inbox, dict):
        for key, info in list(inbox.items())[:40]:
            if not isinstance(info, dict):
                continue
            inbox_people.append({
                "name": info.get("display_name") or key,
                "total": info.get("total", 0),
                "assign": info.get("Assign", 0),
                "issue": info.get("Issue", 0),
                "distribute": info.get("Distribute", 0),
                "disciplines": info.get("disciplines") or [],
            })

    top_performers = []
    for person in extras.get("top_performers") or []:
        row = dict(person)
        row["total_score"] = _round_score(row.get("total_score"))
        row["core_score"] = _round_score(row.get("core_score"))
        top_performers.append(row)

    facts = {
        "generated_at": datetime.now().isoformat(timespec="minutes"),
        "scope": _doc_type_label(doc_type),
        "doc_type": doc_type,
        "overall": {
            "total": overall_total,
            "overdue": overall_overdue,
            "overdue_client": len(client_docs),
            "overdue_contractor": len(contractor_docs),
            "overdue_over_150": len(contractor_over_150),
            "hold": overall_hold,
            "approved": overall_approved,
            "not_issued": int(stats.get("not_issued") or 0),
            "with_customer": int(stats.get("with_customer") or 0),
            "progress": stats.get("avg_progress") or stats.get("engineering_progress") or 0,
            "health": overall_score,
            "status": overall_status,
            "status_label": _status_label(overall_status),
        },
        "projects": projects[:20],
        "disciplines_overdue": [
            {"name": name, "count": counts["total"], "client": counts["client"], "contractor": counts["contractor"]}
            for name, counts in sorted(overdue_by_discipline.items(), key=lambda kv: kv[1]["total"], reverse=True)[:10]
        ],
        "overdue_index": overdue_index[:500],
        "top_overdue_docs": overdue_index[:12],
        "alerts": alerts[:6],
        "people": inbox_people[:25],
        "people_directory": _compact_people(extras.get("people_directory") or []),
        "top_performers": top_performers,
        "discipline_scores": extras.get("discipline_scores") or [],
        "period": extras.get("period") or "today",
    }
    return facts


def render_template_briefing(facts: dict, period: str = "today") -> dict:
    overall = facts.get("overall") or {}
    projects = facts.get("projects") or []
    scope = facts.get("scope") or "مدارک"
    period_label = {"today": "امروز", "week": "هفته جاری", "month": "ماه جاری"}.get(period, "امروز")
    headline = f"{scope} — گزارش {period_label}: {_status_emoji(overall.get('status'))} {overall.get('status_label', 'نامشخص')}"
    lines = [
        f"امتیاز سلامت {scope} {overall.get('health', '-')} از ۱۰۰ است. "
        f"از {overall.get('total', 0)} مدرک فعال، {overall.get('overdue_client', 0)} مدرک کارفرما "
        f"(مهلت ۱۴ روز) و {overall.get('overdue_contractor', 0)} مدرک پیمانکار (مهلت ۷ روز) تأخیر دارند. "
        f"{overall.get('hold', 0)} مدرک Hold هستند."
    ]
    performers = facts.get("top_performers") or []
    if performers:
        names = "، ".join(
            f"{p.get('person')} ({_round_score(p.get('total_score'))})"
            for p in performers[:3]
        )
        lines.append(f"نفرات برتر این بازه: {names}.")
    if projects:
        worst = max(projects, key=lambda p: p.get("overdue", 0))
        if worst.get("overdue"):
            lines.append(
                f"اولویت پیگیری: پروژه {worst['name']} "
                f"({worst.get('overdue_client', 0)} کارفرما، {worst.get('overdue_contractor', 0)} پیمانکار)."
            )
    return {
        "headline": headline,
        "briefing": " ".join(line for line in lines if line).strip(),
        "source": "template",
        "model": None,
    }


def _status_emoji(status: str) -> str:
    return {"good": "🟢", "fair": "🟠", "attention": "🔴", "unknown": "⚪"}.get(status or "", "⚪")


_standing_instructions: list = []


def set_standing_instructions(instructions: list):
    """
    قوانین رفتاری ثابتی که مدیر پروژه دستی ثبت کرده (دسته‌ی 'instruction' در
    دانش دستی) و باید همیشه به Qwen یادآوری بشن — مثل لحن پاسخ یا نکته‌ای که
    باید همیشه رعایت بشه. فقط چند تای آخر نگه داشته می‌شه تا system prompt
    زیادی بزرگ نشه.
    """
    global _standing_instructions
    _standing_instructions = [str(i).strip() for i in (instructions or []) if str(i).strip()][:8]


def _augmented_system_prompt(base: str) -> str:
    if not _standing_instructions:
        return base
    rules = " ".join(f"- {i}" for i in _standing_instructions)
    return base + "\nقوانین اضافی که مدیر پروژه ثبت کرده و باید همیشه رعایت کنی: " + rules


def call_qwen(user_prompt: str, system_prompt: str, model: str = OLLAMA_MODEL) -> Optional[str]:
    payload = {
        "model": model,
        "stream": False,
        "options": {"temperature": 0.15, "num_predict": 180},
        "messages": [
            {"role": "system", "content": _augmented_system_prompt(system_prompt)},
            {"role": "user", "content": user_prompt},
        ],
    }
    request = urllib.request.Request(
        OLLAMA_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=OLLAMA_TIMEOUT) as response:
            body = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
        return None

    message = (body or {}).get("message") or {}
    text = (message.get("content") or "").strip()
    return text or None


_SYSTEM_PROMPT = (
    "تو تحلیلگر مدیریت مدارک پروژه‌های مهندسی هستی. "
    "فقط از حقایق JSON پیوست‌شده استفاده کن. عدد جدید نساز. پاسخ کوتاه و فارسی باشد."
)


def generate_daily_briefing(facts: dict, period: str = "today") -> dict:
    rendered = render_template_briefing(facts, period=period)
    briefing_text = rendered["briefing"]
    source = "template"
    model = None
    if USE_LLM:
        period_label = {"today": "روزانه", "week": "هفتگی", "month": "ماهانه"}.get(period, "روزانه")
        compact = {
            "overall": facts.get("overall"),
            "alerts": facts.get("alerts"),
            "projects": [
                {
                    "name": p.get("name"),
                    "health": p.get("health"),
                    "overdue_client": p.get("overdue_client"),
                    "overdue_contractor": p.get("overdue_contractor"),
                }
                for p in (facts.get("projects") or [])[:6]
            ],
            "top_performers": facts.get("top_performers") or [],
        }
        user_prompt = (
            "حقایق داشبورد:\n"
            f"{json.dumps(compact, ensure_ascii=False, default=str)}\n\n"
            f"یک گزارش {period_label} در ۴ جمله بنویس. عدد جدید نساز."
        )
        llm_text = call_qwen(user_prompt, _SYSTEM_PROMPT)
        if llm_text:
            briefing_text = llm_text
            source = "qwen"
            model = OLLAMA_MODEL

    return {
        "ready": True,
        "generated_at": facts.get("generated_at"),
        "period": period,
        "scope": facts.get("scope"),
        "headline": rendered["headline"],
        "briefing": briefing_text,
        "source": source,
        "model": model,
        "overall": facts.get("overall") or {},
        "alerts": facts.get("alerts") or [],
        "projects": facts.get("projects") or [],
        "disciplines_overdue": facts.get("disciplines_overdue") or [],
        "top_performers": facts.get("top_performers") or [],
        "discipline_scores": facts.get("discipline_scores") or [],
    }


def _search_knowledge(question: str, entries: list, limit: int = 3) -> list:
    """
    یادداشت‌های دستیِ دسته‌ی 'fact' که کلیدواژه‌هاشون تو سؤال هم اومده.
    جست‌وجوی ساده‌ی کلیدواژه‌ای (مثل بقیه‌ی matcherهای همین فایل) — نه
    embedding، چون تعداد یادداشت‌ها کمه و این روش شفاف و قابل‌اتکاست.
    """
    tokens = [_fold(t) for t in re.findall(r"[\w\u0600-\u06FF]{2,}", question or "")]
    tokens = [t for t in tokens if t]
    if not tokens:
        return []
    scored = []
    for entry in entries or []:
        if entry.get("category") != "fact":
            continue
        folded_text = _fold(entry.get("text", ""))
        score = sum(1 for t in tokens if t in folded_text)
        if score > 0:
            scored.append((score, entry))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [e for _, e in scored[:limit]]


_FINANCE_TRIGGER_WORDS = (
    'مبلغ', 'قرارداد', 'الحاقیه', 'صورت وضعیت', 'صورت‌وضعیت', 'کارفرما',
    'باقی مانده', 'باقیمانده', 'چند ماه از پروژه',
)
_CONTRACT_TEXT_TRIGGER_WORDS = ('بند', 'ماده', 'تعهد', 'شرایط', 'مسئولیت', 'جریمه', 'فسخ')


def _is_finance_question(question: str) -> bool:
    q = _fold(question)
    return any(_fold(w) in q for w in _FINANCE_TRIGGER_WORDS)


def _wants_contract_text(question: str) -> bool:
    q = _fold(question)
    return any(_fold(w) in q for w in _CONTRACT_TEXT_TRIGGER_WORDS)


def _finance_or_contract_answer(question: str, project_name: str, finance_context: dict,
                                  contract_search_fn=None) -> Optional[dict]:
    """
    اول اگه سؤال به متن قرارداد اشاره داره (بند/تعهد/شرایط...) و فایلی
    ایندکس شده، از رو متن جواب می‌ده (همیشه با هشدار). وگرنه از خلاصه‌ی
    مالیِ ساختاریافته (اعداد واقعی، بدون مدل) جواب می‌ده. اگه هیچ‌کدوم
    چیزی نداشتن، None برمی‌گردونه تا resolve_question ادامه بده.
    """
    if _wants_contract_text(question) and contract_search_fn:
        chunks = contract_search_fn(project_name, question)
        if chunks:
            notes = "\n".join(f"- {c.get('text', '')[:400]}" for c in chunks[:4])
            disclaimer = "⚠️ حتماً خودِ سند قرارداد/الحاقیه را هم چک کنید — این پاسخ از روی متن استخراج‌شده است."
            if USE_LLM:
                system_prompt = (
                    "تو دستیار قراردادهای پیمانکاری مهندسی هستی. فقط از متن پیوست‌شده استفاده "
                    "کن؛ چیزی که تو متن نیست نگو. پاسخ کوتاه و فارسی."
                )
                user_prompt = f"سؤال: {question}\nمتن قرارداد/الحاقیه:\n{notes}"
                llm_text = call_qwen(user_prompt, system_prompt)
                if llm_text:
                    return {
                        "kind": "contract_text",
                        "answer": f"{llm_text}\n\n{disclaimer}",
                        "source": "qwen+contract", "model": OLLAMA_MODEL, "chunks": chunks,
                    }
            return {
                "kind": "contract_text",
                "answer": f"طبق متن قرارداد:\n{notes}\n\n{disclaimer}",
                "source": "contract", "model": None, "chunks": chunks,
            }

    import project_finance
    summary = (finance_context or {}).get(project_name)
    if summary:
        return {
            "kind": "finance",
            "answer": project_finance.render_summary_text(summary),
            "source": "data", "model": None, "summary": summary,
        }

    return None


def _is_greeting(question: str) -> bool:
    folded = _fold(question)
    if folded in {_fold(g) for g in GREETINGS}:
        return True
    tokens = question.strip().split()
    return len(tokens) <= 2 and any(_fold(g) in folded for g in GREETINGS)


def _parse_days_threshold(question: str) -> Optional[int]:
    match = re.search(r"(\d+)\s*(?:روز|day)", question, re.IGNORECASE)
    if match:
        return int(match.group(1))
    if any(word in question for word in ("تأخیر", "تاخير", "دیرکرد", "overdue", "تاخیر")):
        return 0
    return None


def _wanted_party(question: str) -> Optional[str]:
    folded = _fold(question)
    if any(key in folded for key in ("کارفرما", "client", "کافرما")):
        return "client"
    if any(key in folded for key in ("پیمانکار", "پيمانکار", "contractor")):
        return "contractor"
    return None


def _match_discipline(question: str, names: list) -> Optional[str]:
    folded_q = _fold(question)
    for alias, key in DISCIPLINE_ALIASES.items():
        if _fold(alias) in folded_q:
            for name in names:
                if key in _fold(name):
                    return name
    for name in names:
        if name and _fold(name) and _fold(name) in folded_q:
            return name
    return None


def _match_project(question: str, projects: list) -> Optional[dict]:
    folded_q = _fold(question)
    aliases = {
        "اهواز": "ahvaz",
        "آبادان": "abadan",
        "ابادان": "abadan",
        "سراجه": "serajeh",
        "آغاجاری": "aghajari",
        "اغاجاری": "aghajari",
        "پازنان": "pazanan",
    }
    for project in projects or []:
        name = project.get("name") or ""
        if name and _fold(name) in folded_q:
            return project
    for alias, key in aliases.items():
        if _fold(alias) in folded_q or key in folded_q:
            for project in projects or []:
                if key in _fold(project.get("name")):
                    return project
    return None


def _person_search_keys(person: dict) -> list:
    keys = [
        person.get("name"),
        person.get("persian_name"),
        *(person.get("aliases") or []),
    ]
    extra = []
    for key in keys:
        if not key:
            continue
        parts = str(key).replace("‌", " ").split()
        if parts:
            extra.append(parts[-1])
        extra.extend(parts)
        if len(parts) >= 2:
            extra.append(''.join(parts[-2:]))
    return [k for k in keys + extra if k]


def _find_people(question: str, directory: list) -> list:
    folded_q = _fold(question)
    if len(folded_q) < 3:
        return []
    scored = []
    for person in directory or []:
        keys = _person_search_keys(person)
        hit = None
        for key in keys:
            folded_key = _fold(key)
            if len(folded_key) < 3:
                continue
            if folded_key in folded_q or (len(folded_q) >= 4 and folded_q in folded_key):
                hit = len(folded_key)
                break
        if hit:
            scored.append((hit, person))
    scored.sort(key=lambda item: item[0], reverse=True)
    unique = []
    seen = set()
    for _, person in scored:
        name = person.get("name") or person.get("persian_name")
        if name in seen:
            continue
        seen.add(name)
        unique.append(person)
    return unique[:5]


def _format_assignments(person: dict) -> str:
    parts = []
    for item in person.get("assignments") or []:
        project = item.get("project") or "نامشخص"
        role = item.get("role_label") or item.get("role_code") or "نقش نامشخص"
        parts.append(f"{role} در پروژه {project}")
    return "؛ ".join(parts)


def _person_answer(person: dict) -> dict:
    persian = person.get("persian_name") or person.get("name")
    english = person.get("name")
    title_bits = []
    if person.get("org_role_label"):
        title_bits.append(person["org_role_label"])
    if person.get("discipline"):
        title_bits.append(f"دیسیپلین {person['discipline']}")
    assignments = _format_assignments(person)
    lines = [f"{persian}" + (f" ({english})" if english and english != persian else "") + "."]
    if title_bits:
        lines.append("نقش سازمانی: " + "، ".join(title_bits) + ".")
    if assignments:
        lines.append("حضور در پروژه‌ها: " + assignments + ".")
    if person.get("total_score"):
        score_role = person.get("score_role_label") or ""
        lines.append(f"امتیاز رقابتی اخیر: {person['total_score']}" + (f" ({score_role})" if score_role else "") + ".")
    if not assignments and not title_bits:
        lines.append("در ساختار سازمانی یا تاریخچه مدارک نقش ثبت‌شده‌ای پیدا نشد.")
    return {
        "kind": "person",
        "answer": " ".join(lines),
        "person": person,
        "source": "directory",
        "model": None,
    }


def _overdue_answer(rows: list, question: str, threshold: int, discipline=None, project=None, party=None) -> dict:
    title_bits = []
    if discipline:
        title_bits.append(f"دیسیپلین {discipline}")
    if project:
        title_bits.append(f"پروژه {project}")
    if party == "client":
        title_bits.append("کارفرما")
    elif party == "contractor":
        title_bits.append("پیمانکار")
    if threshold:
        title_bits.append(f"بیش از {threshold} روز")
    heading = "مدارک دارای تأخیر" + ((" — " + "، ".join(title_bits)) if title_bits else "")
    if not rows:
        return {
            "kind": "overdue_list",
            "answer": heading + ": موردی با این فیلتر پیدا نشد.",
            "table": [],
            "source": "data",
            "model": None,
        }
    preview = rows[:40]
    lines = [f"{heading}: {len(rows)} مورد."]
    for row in preview[:8]:
        title = f" — {row['document_title']}" if row.get("document_title") else ""
        lines.append(
            f"{row.get('document_no')}{title} | {row.get('project')} | {row.get('discipline')} | "
            f"{row.get('type_label')} | {row.get('days')} روز"
        )
    if len(rows) > 8:
        lines.append(f"... و {len(rows) - 8} مورد دیگر در جدول زیر.")
    return {
        "kind": "overdue_list",
        "answer": "\n".join(lines),
        "table": preview,
        "total": len(rows),
        "source": "data",
        "model": None,
    }


def _contains_any(question: str, keys) -> bool:
    folded = _fold(question)
    return any(_fold(key) in folded for key in keys)


def _is_score_explain_question(question: str, people_hits: list) -> bool:
    if not people_hits:
        return False
    if _contains_any(question, ("امتیاز سلامت", "health score")):
        return False
    return _contains_any(question, (
        "امتیاز", "score", "نمره",
        "چطور حساب", "چجوری حساب", "نحوه محاسبه", "چگونه محاسبه",
    ))


def _num(value, digits=1):
    try:
        n = float(value)
    except (TypeError, ValueError):
        return 0.0
    if digits == 0:
        return int(round(n))
    return round(n, digits)


def _ratio(value) -> float:
    try:
        return round(float(value or 0), 3)
    except (TypeError, ValueError):
        return 0.0


def _weighted_points(ratio, weight) -> float:
    return round(_ratio(ratio) * float(weight) * 100, 2)


_PERIOD_LABELS = {"today": "۷ روز اخیر", "week": "۷ روز اخیر", "month": "۳۰ روز اخیر"}


def _score_explain_answer(person: dict, period: str = "today") -> dict:
    from scoring import SCORE_WEIGHTS
    period_label = _PERIOD_LABELS.get(period, "۷ روز اخیر")

    persian = person.get("persian_name") or person.get("name")
    english = person.get("name")
    display = persian + (f" ({english})" if english and english != persian else "")
    role = person.get("score_role_label") or person.get("org_role_label") or person.get("score_role") or "نامشخص"
    role_key = (person.get("score_role") or person.get("org_role") or "").lower()
    peer_group = (person.get("peer_group") or role_key or "").lower()
    combined = bool(person.get("is_combined_role")) or peer_group == "senior_combined"

    revisions = int(person.get("n_revisions_touched") or 0)
    delays = int(person.get("delay_causes_count") or 0)
    on_time_count = max(0, revisions - delays)
    max_revs = int(person.get("max_revisions_in_role") or 0)
    avg_needed = _num(person.get("avg_revisions_needed"), 2)
    avg_days = _num(person.get("avg_duration_days"), 2)
    peer_avg = _num(person.get("peer_avg_duration"), 2)
    budget = person.get("role_budget_days")
    if budget is None:
        budget = {"specialist": 3.5, "senior": 1.0, "senior_combined": 4.5, "manager": 0.5}.get(
            "senior_combined" if combined else role_key, None
        )
    budget = _num(budget, 1) if budget is not None else None

    on_time = _ratio(person.get("on_time_score"))
    quality = _ratio(person.get("quality_score"))
    speed = _ratio(person.get("speed_score"))
    volume = _ratio(person.get("volume_score"))
    w = SCORE_WEIGHTS
    wv = {k: f"{v:.2f}" for k, v in w.items()}
    on_time_pts = _weighted_points(on_time, w["on_time"])
    quality_pts = _weighted_points(quality, w["quality"])
    speed_pts = _weighted_points(speed, w["speed"])
    volume_pts = _weighted_points(volume, w["volume"])
    parts_sum = round(on_time_pts + quality_pts + speed_pts + volume_pts, 2)
    stored_core = person.get("core_score")
    core = _num(stored_core, 1) if stored_core is not None else parts_sum
    shared_d = int(person.get("shared_distribute_points") or 0)
    shared_p = int(person.get("shared_prediction_points") or 0)
    stored_total = person.get("total_score")
    reconstructed = round(core + shared_d + shared_p, 1)
    total = _num(stored_total, 1) if stored_total is not None else reconstructed

    peer_label = {
        "specialist": "کارشناس‌ها",
        "senior": "مهندس‌های ارشد (فقط بازبینی)",
        "senior_combined": "ارشدهای تک‌نفره (کارشناس+ارشد با هم)",
        "manager": "مدیران مهندسی",
    }.get("senior_combined" if combined else role_key, "هم‌رده‌ها")

    if revisions <= 0 or person.get("on_time_score") is None:
        return {
            "kind": "score_explain",
            "answer": (
                f"برای {display} در بازه‌ی {period_label} رویژنی در امتیازدهی ثبت نشده؛ "
                "بنابراین جزئیات محاسبه وجود ندارد."
            ),
            "person": person,
            "source": "data",
            "model": None,
        }

    budget_txt = f"{budget} روز" if budget is not None else "سهمیه نقش"
    if combined:
        budget_txt += " (تیم تک‌نفره: کارشناس ۳.۵ + ارشد ۱)"
    elif role_key == "specialist":
        budget_txt += " (۷۰٪ از ۵ روز کاری مدرک)"
    elif role_key == "senior":
        budget_txt += " (۲۰٪ از ۵ روز کاری مدرک)"
    elif role_key == "manager":
        budget_txt += " (فقط کنترل مدرک، ۱۰٪ از ۵ روز)"

    lines = [
        f"امتیاز {display} برای نقش {role} در بازه‌ی {period_label} این‌طور ریز حساب شده است. مقایسه فقط داخل {peer_label} است.",
        "",
        f"۱) حجم کار (وزن {wv['volume']}):",
        f"پرکارترین فرد همین رده در این بازه {max_revs} رویژن داشته. {persian} {revisions} رویژن کار کرده.",
        f"امتیاز حجم = {revisions} ÷ {max_revs or 1} = {volume}",
        f"سهم در هسته = {volume} × {wv['volume']} × ۱۰۰ = {volume_pts}",
        "",
        f"۲) رعایت موعد (وزن {wv['on_time']}):",
        f"سهمیه زمانی این نقش {budget_txt} است. از {revisions} رویژن، در {on_time_count} مورد داخل سهمیه مانده و در {delays} مورد زمانش از سهمیه بیشتر شده (علت تأخیر).",
        f"امتیاز موعد = ۱ − ({delays} ÷ {revisions}) = {on_time}",
        f"سهم در هسته = {on_time} × {wv['on_time']} × ۱۰۰ = {on_time_pts}",
        "",
        f"۳) کیفیت (وزن {wv['quality']}):",
        f"مدارکی که {persian} رویشان کار کرده، به‌طور میانگین {avg_needed} رویژن تا تأیید لازم داشته‌اند (کمتر = بهتر).",
        f"امتیاز کیفیت = ۱ ÷ {avg_needed or 1} = {quality}",
        f"سهم در هسته = {quality} × {wv['quality']} × ۱۰۰ = {quality_pts}",
        "",
        f"۴) سرعت خالص (وزن {wv['speed']}):",
        f"میانگین زمان {persian} روی هر رویژن {avg_days} روز بوده؛ میانگین {peer_label} {peer_avg} روز.",
        f"امتیاز سرعت نسبت به هم‌رده (سقف ۱) = {speed}",
        f"سهم در هسته = {speed} × {wv['speed']} × ۱۰۰ = {speed_pts}",
        "",
        f"هسته فردی = {volume_pts} + {on_time_pts} + {quality_pts} + {speed_pts} = {parts_sum}"
        + (f"؛ مقدار ثبت‌شده در سیستم بعد از گرد کردن {core} است." if abs(parts_sum - core) >= 0.05 else "."),
    ]
    if role_key == "manager":
        lines.append("مدیر مهندسی مدرک تولید نمی‌کند؛ فقط کنترل می‌کند و دیسیپلین Management در جمع دیسیپلین‌ها وارد نمی‌شود.")
    if shared_d or shared_p:
        lines.extend([
            "",
            f"جایزه تیمی دیسیپلین (خارج از چهار جزء بالا، یک‌بار برای کل تیم): Distribute {shared_d:+d} و پیش‌بینی صدور {shared_p:+d}.",
            f"امتیاز نمایش‌داده‌شده = {core} + ({shared_d}) + ({shared_p}) = {total}.",
        ])
    else:
        lines.extend([
            "",
            f"جایزه تیمی Distribute و پیش‌بینی برای این فرد صفر است؛ امتیاز نهایی همان هسته یعنی {total} است.",
        ])
    return {
        "kind": "score_explain",
        "answer": "\n".join(lines),
        "person": person,
        "source": "data",
        "model": None,
    }


def _is_best_discipline_question(question: str) -> bool:
    has_disc = _contains_any(question, ("دیسیپلین", "discipline", "رشته"))
    has_best = _contains_any(question, ("بهترین", "برتر", "قوی", "سالم", "از نظر تو", "پیشنهاد"))
    has_worst = _contains_any(question, ("بدترین", "ضعیف", "بحرانی"))
    return has_disc and (has_best or has_worst)


def _is_priority_question(question: str) -> bool:
    return _contains_any(question, (
        "اولویت", "زمان بندی", "زمان‌بندی", "زمانبندي",
        "پیگیری", "الان چی", "باید انجام", "شروع کنیم",
        "کدام کار", "چی کار کنیم", "توصیه", "پیشنهاد پیگیری",
    ))


def _discipline_rank_answer(question: str, facts: dict) -> dict:
    scores = list(facts.get("discipline_scores") or [])
    overdue = {d.get("name"): d for d in (facts.get("disciplines_overdue") or []) if d.get("name")}
    names = set()
    for row in scores:
        if row.get("discipline"):
            names.add(row["discipline"])
    names.update(overdue.keys())
    ranking = []
    for name in names:
        score_row = next((s for s in scores if s.get("discipline") == name), {})
        over = overdue.get(name) or {}
        if org_structure.is_excluded_discipline(name):
            continue
        ranking.append({
            "discipline": name,
            "total_score": _round_score(score_row.get("total_score")),
            "member_count": score_row.get("member_count") or 0,
            "overdue": int(over.get("count") or 0),
            "client": int(over.get("client") or 0),
            "contractor": int(over.get("contractor") or 0),
        })
    if not ranking:
        return {
            "kind": "discipline_rank",
            "answer": "برای رتبه‌بندی دیسیپلین هنوز داده رقابتی یا تأخیر کافی نیست.",
            "table": [],
            "source": "data",
            "model": None,
        }

    want_worst = _contains_any(question, ("بدترین", "ضعیف", "بحرانی"))
    by_score = sorted(ranking, key=lambda r: r["total_score"], reverse=not want_worst)
    by_overdue = sorted(ranking, key=lambda r: r["overdue"], reverse=want_worst)
    pick = by_score[0] if any(r["total_score"] for r in ranking) else by_overdue[0]
    label = "ضعیف‌ترین" if want_worst else "بهترین"
    reason = (
        f"{label} دیسیپلین از نظر داده داشبورد «{pick['discipline']}» است "
        f"(امتیاز رقابتی {pick['total_score']}، تأخیر کارفرما {pick['client']}، "
        f"پیمانکار {pick['contractor']}). "
        "این نظر مدل نیست؛ از امتیاز رقابتی همین بازه و تعداد مدارک تأخیری همین لحظه آمده است."
    )
    table = sorted(ranking, key=lambda r: (-r["total_score"], r["overdue"]))[:8]
    return {
        "kind": "discipline_rank",
        "answer": reason,
        "table": table,
        "source": "data",
        "model": None,
    }


def _priority_docs_answer(facts: dict, project=None, discipline=None) -> dict:
    rows = list(facts.get("overdue_index") or [])
    if project:
        rows = [d for d in rows if d.get("project") == project]
    if discipline:
        rows = [
            d for d in rows
            if _fold(d.get("discipline")) == _fold(discipline) or _fold(discipline) in _fold(d.get("discipline"))
        ]

    def sort_key(doc):
        party = 0 if doc.get("type") != "client" else 1
        days = -int(doc.get("days") or 0)
        return (party, days)

    ranked = sorted(rows, key=sort_key)
    preview = []
    for idx, doc in enumerate(ranked[:25], start=1):
        if doc.get("type") == "client":
            why = "پاسخ کارفرما عقب است؛ پیگیری comms"
        else:
            why = "کار پیمانکار عقب است؛ باید در برنامه تیم قرار بگیرد"
        item = dict(doc)
        item["rank"] = idx
        item["reason"] = why
        preview.append(item)

    if not preview:
        return {
            "kind": "priority",
            "answer": "مدارک تأخیری برای اولویت‌بندی پیدا نشد.",
            "table": [],
            "source": "data",
            "model": None,
        }

    top = preview[0]
    answer = (
        "اولویت انجام از روی زمان‌بندی فعلی (نه حدس) این است: اول مدارک پیمانکار با بیشترین روز تأخیر، "
        "بعد پیگیری مدارک کارفرما. "
        f"فوری‌ترین مورد {top.get('document_no')} در پروژه {top.get('project')} "
        f"دیسیپلین {top.get('discipline')} با {top.get('days')} روز تأخیر ({top.get('type_label')}) است."
    )
    return {
        "kind": "priority",
        "answer": answer,
        "table": preview,
        "total": len(ranked),
        "source": "data",
        "model": None,
    }


def resolve_question(question: str, facts: dict, document_chunks: Optional[list] = None,
                      activity_df=None, knowledge_entries=None,
                      finance_context=None, contract_search_fn=None) -> dict:
    question = (question or "").strip()
    if not question:
        return {"kind": "empty", "answer": "سؤال خالی است.", "source": "template", "model": None}

    overall = facts.get("overall") or {}
    projects = facts.get("projects") or []
    directory = facts.get("people_directory") or []
    overdue_index = facts.get("overdue_index") or []
    sources = [c.get("path") for c in (document_chunks or [])[:6] if c.get("path")]

    if _is_greeting(question):
        return {
            "kind": "greeting",
            "answer": (
                f"سلام، من خلاصه وضعیت {facts.get('scope', 'مدارک')} هستم. "
                f"الان {overall.get('overdue_client', 0)} مدرک کارفرما و "
                f"{overall.get('overdue_contractor', 0)} مدرک پیمانکار تأخیر دارند. "
                "می‌توانید نام فرد، نحوه محاسبه امتیاز، بهترین دیسیپلین، یا اولویت مدارک تأخیری را بپرسید."
            ),
            "source": "directory",
            "model": None,
        }

    if _is_best_discipline_question(question):
        return _discipline_rank_answer(question, facts)

    discipline_names = list({d.get("discipline") for d in overdue_index if d.get("discipline")})
    discipline_names.extend(d.get("discipline") for d in (facts.get("discipline_scores") or []) if d.get("discipline"))
    discipline = _match_discipline(question, [n for n in discipline_names if n])
    project = _match_project(question, projects)

    # ===== سؤال مالی/قراردادی («مبلغ قرارداد چقدره؟»، «طبق قرارداد مسئولیت
    # تأخیر با کیه؟») — فقط اگه finance_context از app.py رسیده باشه (یعنی
    # کاربر تو لیست دسترسی مالیه). قبل از priority/person چک می‌شه چون
    # پروژه‌محوره، نه فرد‌محور.
    if project and finance_context is not None and (_is_finance_question(question) or _wants_contract_text(question)):
        finance_result = _finance_or_contract_answer(
            question, project.get("name"), finance_context, contract_search_fn
        )
        if finance_result:
            return finance_result

    if _is_priority_question(question):
        return _priority_docs_answer(facts, project=(project or {}).get("name"), discipline=discipline)

    people_hits = _find_people(question, directory)
    if _is_score_explain_question(question, people_hits):
        result = _score_explain_answer(people_hits[0], period=facts.get("period") or "today")
        if len(people_hits) > 1:
            others = "، ".join((p.get("persian_name") or p.get("name")) for p in people_hits[1:])
            result["answer"] += f" اگر منظورتان فرد دیگری است: {others}."
        return result

    # ===== سؤال باز درباره‌ی فعالیت/پیشرفت یک نفر (مثل «فلانی چیکار کرده تو
    # آبادان؟» یا «فلانی چند روز فعال بوده؟») — قبل از پاسخ عمومیِ بیوگرافی
    # بررسی می‌شود، چون این‌ها سؤال مشخص‌تری هستند و باید عدد واقعی بگیرند،
    # نه معرفی کلی فرد. فقط وقتی فعال می‌شود که activity_df در دسترس باشد.
    if people_hits and activity_df is not None and not activity_df.empty \
            and person_activity.wants_activity_query(question):
        person = people_hits[0]
        person_label = person.get("persian_name") or person.get("name")
        project_name = (project or {}).get("name") if project else None
        date_from, date_to = person_activity.parse_date_range(question)
        stats = person_activity.run_person_activity_query(
            activity_df, _person_search_keys(person),
            project=project_name, discipline=discipline,
            date_from=date_from, date_to=date_to,
        )
        result = person_activity.generate_activity_answer(
            person_label, stats, project=project_name, discipline=discipline,
            use_llm=USE_LLM, call_qwen_fn=call_qwen, model_name=OLLAMA_MODEL,
        )
        if len(people_hits) > 1:
            others = "، ".join((p.get("persian_name") or p.get("name")) for p in people_hits[1:])
            result["answer"] += f" اگر منظورتان فرد دیگری است: {others}."
        return result

    looks_like_person = people_hits and not any(
        word in question for word in ("تأخیر", "تاخير", "دیرکرد", "مدرک", "overdue", "لیست", "امتیاز سلامت", "اولویت", "دیسیپلین")
    )
    if looks_like_person:
        result = _person_answer(people_hits[0])
        if len(people_hits) > 1:
            others = "، ".join((p.get("persian_name") or p.get("name")) for p in people_hits[1:])
            result["answer"] += f" افراد مشابه: {others}."
        return result

    threshold = _parse_days_threshold(question)
    party = _wanted_party(question)
    wants_list = threshold is not None or discipline or any(
        word in question for word in ("کدام مدارک", "چه مدارکی", "لیست", "فهرست", "بیش از")
    )

    if wants_list:
        rows = list(overdue_index)
        if discipline:
            rows = [d for d in rows if _fold(d.get("discipline")) == _fold(discipline) or _fold(discipline) in _fold(d.get("discipline"))]
        if project:
            rows = [d for d in rows if d.get("project") == project.get("name")]
        if party == "client":
            rows = [d for d in rows if d.get("type") == "client"]
        elif party == "contractor":
            rows = [d for d in rows if d.get("type") != "client"]
        if threshold:
            rows = [d for d in rows if int(d.get("days") or 0) >= threshold]
        return _overdue_answer(
            rows, question, threshold or 0,
            discipline=discipline,
            project=(project or {}).get("name"),
            party=party,
        )

    if project:
        p = project
        return {
            "kind": "project",
            "answer": (
                f"پروژه {p['name']}: امتیاز سلامت {p.get('health')}. "
                f"تأخیر کارفرما (۱۴ روز) {p.get('overdue_client', 0)} مدرک، "
                f"تأخیر پیمانکار (۷ روز) {p.get('overdue_contractor', 0)} مدرک"
                + (f"، جدا از آن {p.get('overdue_over_150', 0)} مدرک پیمانکار بیش از ۱۵۰ روز." if p.get("overdue_over_150") else ".")
            ),
            "project": p,
            "source": "data",
            "model": None,
        }

    if people_hits:
        return _person_answer(people_hits[0])

    # ===== دانش دستی: یادداشت‌هایی که مدیر پروژه خودش ثبت کرده و به سؤال
    # مربوطن. قبل از افتادن تو جواب کلی/بدون‌اطلاعات چک می‌شه.
    knowledge_hits = _search_knowledge(question, knowledge_entries)
    if knowledge_hits:
        notes_text = " ".join(f"- {e.get('text', '')}" for e in knowledge_hits)
        if USE_LLM:
            system_prompt = (
                "تو دستیار داشبورد مدارک مهندسی هستی. فقط از یادداشت‌های زیر که "
                "مدیر پروژه دستی ثبت کرده استفاده کن؛ چیزی که تو یادداشت‌ها نیست "
                "نگو. پاسخ کوتاه و فارسی."
            )
            user_prompt = f"سؤال: {question}\nیادداشت‌های ثبت‌شده:\n{notes_text}"
            llm_text = call_qwen(user_prompt, system_prompt)
            if llm_text:
                return {
                    "kind": "knowledge", "answer": llm_text, "source": "qwen+notes",
                    "model": OLLAMA_MODEL, "notes": knowledge_hits,
                }
        return {
            "kind": "knowledge",
            "answer": "طبق یادداشت‌های ثبت‌شده: " + " ".join(e.get("text", "") for e in knowledge_hits),
            "source": "notes", "model": None, "notes": knowledge_hits,
        }

    if document_chunks:
        snippet = (document_chunks[0].get("text") or "")[:400]
        return {
            "kind": "files",
            "answer": f"از فایل‌های پوشه مدارک: {snippet}",
            "sources": sources,
            "source": "files",
            "model": None,
        }

    performers = facts.get("top_performers") or []
    fallback = (
        f"در {facts.get('scope', 'داشبورد')} الان {overall.get('overdue_client', 0)} مدرک کارفرما "
        f"و {overall.get('overdue_contractor', 0)} مدرک پیمانکار تأخیر دارند. "
        f"امتیاز سلامت {overall.get('health', '-')} است."
    )
    if performers:
        fallback += " نفرات برتر: " + "، ".join(
            f"{p.get('person')} ({_round_score(p.get('total_score'))})"
            for p in performers[:3] if p.get("person")
        ) + "."
    return {"kind": "stats", "answer": fallback, "source": "data", "model": None, "sources": sources}


def answer_question(question: str, facts: dict, project: Optional[str] = None,
                    document_chunks: Optional[list] = None, activity_df=None,
                    knowledge_entries=None, finance_context=None, contract_search_fn=None) -> dict:
    scoped = facts
    if project and project != "همه":
        scoped = dict(facts)
        scoped["projects"] = [p for p in (facts.get("projects") or []) if p.get("name") == project] or facts.get("projects")
        scoped["overdue_index"] = [d for d in (facts.get("overdue_index") or []) if str(d.get("project")) == project]
    return resolve_question(question, scoped, document_chunks=document_chunks,
                             activity_df=activity_df, knowledge_entries=knowledge_entries,
                             finance_context=finance_context, contract_search_fn=contract_search_fn)
