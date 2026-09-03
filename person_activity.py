"""
ایندکس فعالیت افراد — پایه‌ی پاسخ به سؤال‌های باز مثل:
«عیسی‌زاده چیکار کرده تو ماه گذشته؟»، «عیسی‌زاده چند درصد پیشرفت داشته
تو آبادان؟»، «کیمیا چند روز تو ماه فعال بوده؟»

چرا این فایل جداست: project_intelligence.py با facts از قبل خلاصه‌شده کار
می‌کند (برای سرعت و برای سؤال‌های پرتکرار از پیش برنامه‌ریزی‌شده). اما
سؤال‌های واقعاً باز (ترکیب دلخواه فرد × پروژه × دیسیپلین × بازه‌ی زمانی)
نیاز به داده‌ی خام تراکنش‌ها (History/Vendor History) دارند، نه خلاصه.

اصل طراحی (بدون تغییر نسبت به بقیه‌ی پروژه): تمام اعداد اینجا با pandas
محاسبه می‌شوند. مدل زبانی (Qwen) فقط اجازه دارد همین اعداد آماده را به
جمله‌ی فارسی تبدیل کند — هیچ عددی توسط مدل ساخته نمی‌شود.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

_NAME_CODE_RE = re.compile(r'^(.*?)\s*\(([^)]+)\)\s*$')


def _fold(text) -> str:
    value = str(text or '').strip().lower()
    for src, dst in (('آ', 'ا'), ('ي', 'ی'), ('ك', 'ک'), ('‌', ''), (' ', ''), ('ـ', '')):
        value = value.replace(src, dst)
    return value


def _extract_name(name_with_code) -> Optional[str]:
    """'نام (MDJ)' -> 'نام'. اگه پرانتز نداشت، همون رشته برمی‌گرده."""
    if not isinstance(name_with_code, str) or not name_with_code.strip():
        return None
    m = _NAME_CODE_RE.match(name_with_code.strip())
    return (m.group(1).strip() if m else name_with_code.strip()) or None


def _melt_history(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """
    هر سطر History یک تراکنش دونفره (From -> To) روی یک مدرک است. این تابع
    هر تراکنش را به دو رکورد فعالیت تبدیل می‌کند (یکی برای فرستنده، یکی
    برای گیرنده) تا هرکس در تراکنش دخیل بوده، در ایندکس دیده شود.
    """
    cols_needed = ['Document No.', 'Action Date', 'From Name', 'To Name', 'Log Status']
    if df is None or df.empty or any(c not in df.columns for c in cols_needed):
        return pd.DataFrame(columns=['person', 'document_no', 'action_type', 'action_date', 'source'])

    base = df[cols_needed].copy()
    base['Action Date'] = pd.to_datetime(base['Action Date'], errors='coerce')
    base = base.dropna(subset=['Action Date', 'Document No.'])

    from_rows = pd.DataFrame({
        'person': base['From Name'].apply(_extract_name),
        'document_no': base['Document No.'].astype(str),
        'action_type': base['Log Status'],
        'action_date': base['Action Date'],
    })
    to_rows = pd.DataFrame({
        'person': base['To Name'].apply(_extract_name),
        'document_no': base['Document No.'].astype(str),
        'action_type': base['Log Status'],
        'action_date': base['Action Date'],
    })
    out = pd.concat([from_rows, to_rows], ignore_index=True)
    out = out.dropna(subset=['person'])
    out['source'] = source
    return out


def _latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    """آخرین ردیف (بر اساس Date) برای هر Document No. — مستر یه لاگ روزانه‌ی انباشتیه."""
    if df is None or df.empty or 'Document No.' not in df.columns:
        return pd.DataFrame()
    d = df.dropna(subset=['Document No.'])
    if 'Date' in d.columns:
        d = d.sort_values('Date').groupby('Document No.', as_index=False).last()
    else:
        d = d.drop_duplicates(subset=['Document No.'], keep='last')
    return d


def build_activity_index(history_df: pd.DataFrame, vendor_history_df: pd.DataFrame,
                          master_df: pd.DataFrame, vendor_master_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    جدول تخت فعالیت افراد: هر ردیف = یک نفر که در یک تراکنش (Assign/Issue/
    Comment/Distribute و...) روی یک مدرک دخیل بوده، به‌همراه پروژه/دیسیپلین/
    پیشرفتِ فعلیِ همون مدرک (از آخرین اسنپ‌شات Master — نه از ستون‌های خودِ
    History، چون تو history.xlsx حروف بزرگ/کوچیک ستون project ناهماهنگه).
    """
    activity = pd.concat(
        [_melt_history(history_df, 'client'), _melt_history(vendor_history_df, 'vendor')],
        ignore_index=True,
    )
    if activity.empty:
        return activity
    # اطمینان از نوع datetime واقعی: اگه یکی از دو منبع (مثلاً vendor_history)
    # خالی باشه، pd.concat گاهی کل ستون رو به object تنزل می‌ده و بعداً
    # .dt.date با خطا مواجه می‌شه.
    activity['action_date'] = pd.to_datetime(activity['action_date'], errors='coerce')

    frames = []
    for df in (master_df, vendor_master_df):
        latest = _latest_snapshot(df)
        if latest.empty:
            continue
        cols = ['Document No.', 'Project', 'Discipline', 'Category', 'Document Progress']
        available = [c for c in cols if c in latest.columns]
        if 'Document No.' in available:
            frames.append(latest[available])

    if frames:
        doc_context = pd.concat(frames, ignore_index=True, sort=False)
        doc_context['Document No.'] = doc_context['Document No.'].astype(str)
        doc_context = doc_context.drop_duplicates(subset=['Document No.'], keep='last')
        doc_context = doc_context.rename(columns={
            'Document No.': 'document_no', 'Project': 'project', 'Discipline': 'discipline',
            'Category': 'category', 'Document Progress': 'doc_progress',
        })
        activity = activity.merge(doc_context, on='document_no', how='left')
    else:
        for c in ('project', 'discipline', 'category', 'doc_progress'):
            activity[c] = None

    return activity


# ==================== تشخیص بازه‌ی زمانی از روی متن سؤال ====================
def parse_date_range(question: str):
    """
    بازه‌ی زمانی رو از روی کلیدواژه‌های فارسی تشخیص می‌ده. اگه هیچ‌کدوم پیدا
    نشه، (None, None) برمی‌گرده — یعنی کل تاریخچه، بدون فیلتر زمانی.
    """
    today = datetime.now().date()
    folded = _fold(question)

    m = re.search(r'(\d+)\s*روز\s*(گذشته|اخیر|قبل)', question)
    if m:
        n = int(m.group(1))
        return today - timedelta(days=n), today

    m = re.search(r'(\d+)\s*ماه\s*(گذشته|اخیر|قبل)', question)
    if m:
        n = int(m.group(1))
        return today - timedelta(days=30 * n), today

    if 'امروز' in folded:
        return today, today
    if 'دیروز' in folded:
        y = today - timedelta(days=1)
        return y, y
    if any(k in folded for k in ('هفتهگذشته', 'هفتهپیش', 'هفتهاخیر', 'اینهفته')):
        return today - timedelta(days=7), today
    if any(k in folded for k in ('ماهگذشته', 'ماهپیش', 'ماهاخیر', 'اینماه', 'یکماه')):
        return today - timedelta(days=30), today
    if any(k in folded for k in ('سهماه', '3ماه')):
        return today - timedelta(days=90), today
    if 'امسال' in folded:
        return today.replace(month=1, day=1), today

    return None, None


_ACTIVITY_TRIGGER_WORDS = (
    'چکار', 'چیکار', 'چه کاری', 'فعالیت', 'درصد پیشرفت', 'چند درصد',
    'پیشرفت داشته', 'چند روز', 'فعال بوده', 'روی چی', 'چند تا مدرک',
    'مشغول', 'انجام داده', 'کار کرده',
)


def wants_activity_query(question: str) -> bool:
    folded = _fold(question)
    return any(_fold(w) in folded for w in _ACTIVITY_TRIGGER_WORDS)


# ==================== اجرای پرس‌وجو (تماماً با pandas) ====================
def run_person_activity_query(activity_df: pd.DataFrame, person_keys: list,
                                project: Optional[str] = None, discipline: Optional[str] = None,
                                date_from=None, date_to=None) -> dict:
    """
    فعالیت‌های یک نفر (با چند کلید جست‌وجوی معادل، مثل نام فارسی/انگلیسی)
    رو فیلتر و یک بسته‌ی آماری کامل برمی‌گردونه — نه یک عدد تکی — تا لایه‌ی
    روایت (قالب یا مدل) هرچی لازمه رو از داخلش انتخاب کنه.
    """
    empty_result = {
        'found': False, 'doc_count': 0, 'active_days': 0, 'action_counts': {},
        'projects': {}, 'avg_progress': None,
        'date_from': str(date_from) if date_from else None,
        'date_to': str(date_to) if date_to else None,
        'sample_docs': [],
    }
    if activity_df is None or activity_df.empty or not person_keys:
        return empty_result

    folded_keys = [_fold(k) for k in person_keys if k and len(_fold(k)) >= 2]
    if not folded_keys:
        return empty_result

    df = activity_df.copy()
    df['_folded_person'] = df['person'].apply(_fold)
    mask = df['_folded_person'].apply(lambda p: any(k in p or p in k for k in folded_keys))
    df = df[mask]

    if project:
        df = df[df['project'].apply(lambda p: _fold(project) in _fold(p))]
    if discipline:
        df = df[df['discipline'].apply(lambda d: _fold(discipline) in _fold(d))]
    if date_from is not None:
        df = df[df['action_date'] >= pd.Timestamp(date_from)]
    if date_to is not None:
        df = df[df['action_date'] < pd.Timestamp(date_to) + pd.Timedelta(days=1)]

    if df.empty:
        empty_result['found'] = True  # فرد پیدا شد، فقط تو این بازه/فیلتر فعالیتی نداشته
        return empty_result

    doc_map = df.drop_duplicates(subset=['document_no'])[['document_no', 'project', 'doc_progress']]
    try:
        avg_progress = pd.to_numeric(doc_map['doc_progress'], errors='coerce').mean()
        avg_progress = round(float(avg_progress), 1) if pd.notna(avg_progress) else None
    except Exception:
        avg_progress = None

    action_counts = df['action_type'].value_counts().to_dict()
    project_counts = doc_map['project'].value_counts().to_dict()
    active_days = df['action_date'].dt.date.nunique()

    sample = (
        df.drop_duplicates(subset=['document_no'])
        .sort_values('action_date', ascending=False)
        .head(8)
    )
    sample_docs = [{
        'document_no': r['document_no'],
        'project': r.get('project'),
        'doc_progress': r.get('doc_progress'),
        'action_date': r['action_date'].strftime('%Y-%m-%d') if pd.notna(r['action_date']) else None,
    } for _, r in sample.iterrows()]

    return {
        'found': True,
        'doc_count': int(doc_map['document_no'].nunique()),
        'active_days': int(active_days),
        'action_counts': {str(k): int(v) for k, v in action_counts.items() if k},
        'projects': {str(k): int(v) for k, v in project_counts.items() if k},
        'avg_progress': avg_progress,
        'date_from': str(date_from) if date_from else None,
        'date_to': str(date_to) if date_to else None,
        'sample_docs': sample_docs,
    }


# ==================== روایت نتیجه (قالب — همیشه در دسترس، بدون مدل) ====================
def _period_label(stats: dict) -> str:
    if stats.get('date_from') and stats.get('date_to'):
        return f"بازه‌ی {stats['date_from']} تا {stats['date_to']}"
    return "کل تاریخچه‌ی ثبت‌شده"


def render_activity_answer(person_label: str, stats: dict,
                             project: Optional[str] = None, discipline: Optional[str] = None) -> str:
    if not stats.get('found'):
        return f"فعالیتی برای «{person_label}» در تاریخچه پیدا نشد."

    scope_bits = []
    if project:
        scope_bits.append(f"در پروژه {project}")
    if discipline:
        scope_bits.append(f"در دیسیپلین {discipline}")
    scope_txt = (" " + " و ".join(scope_bits)) if scope_bits else ""
    period_txt = _period_label(stats)

    if stats.get('doc_count', 0) == 0:
        return f"{person_label}{scope_txt} در {period_txt} هیچ فعالیت ثبت‌شده‌ای نداشته."

    parts = [f"{person_label}{scope_txt} در {period_txt} روی {stats['doc_count']} مدرک فعالیت داشته"]
    if stats.get('active_days'):
        parts[-1] += f" ({stats['active_days']} روزِ فعال)."
    else:
        parts[-1] += "."

    if stats.get('avg_progress') is not None:
        parts.append(f"میانگین پیشرفت فعلیِ همین مدارک {stats['avg_progress']}٪ است.")

    action_counts = stats.get('action_counts') or {}
    if action_counts:
        top = sorted(action_counts.items(), key=lambda x: -x[1])[:4]
        parts.append("نوع فعالیت‌ها: " + "، ".join(f"{v} بار {k}" for k, v in top) + ".")

    projects = stats.get('projects') or {}
    if not project and len(projects) > 1:
        top_projects = sorted(projects.items(), key=lambda x: -x[1])[:4]
        parts.append("پراکندگی پروژه‌ها: " + "، ".join(f"{k} ({v} مدرک)" for k, v in top_projects) + ".")

    return " ".join(parts)


# ==================== روایت نتیجه (مدل زبانی، اختیاری) ====================
def generate_activity_answer(person_label: str, stats: dict, project: Optional[str] = None,
                               discipline: Optional[str] = None, use_llm: bool = False,
                               call_qwen_fn=None, model_name: str = None) -> dict:
    """
    همیشه یک متن قالب‌محور (بدون مدل) آماده می‌کند؛ اگر use_llm فعال باشد و
    مدل جواب بدهد، همان متن قالب رو با نسخه‌ی روان‌تر مدل جایگزین می‌کند —
    اما مدل فقط اجازه دارد از همین آمار JSON روایت کند، نه عددسازی.
    """
    template_text = render_activity_answer(person_label, stats, project=project, discipline=discipline)
    result = {'kind': 'activity', 'answer': template_text, 'stats': stats, 'source': 'template', 'model': None}

    if use_llm and call_qwen_fn and stats.get('found') and stats.get('doc_count', 0) > 0:
        import json
        system_prompt = (
            "تو دستیار گزارش‌گیری پروژه‌های مهندسی هستی. فقط از اعداد JSON پیوست‌شده "
            "استفاده کن، هیچ عدد یا فعالیتی که تو JSON نیست نساز، و پاسخ را در ۲ تا ۳ "
            "جمله‌ی فارسیِ روان بنویس."
        )
        user_prompt = (
            f"سؤال درباره‌ی: {person_label}"
            + (f" (پروژه: {project})" if project else "")
            + (f" (دیسیپلین: {discipline})" if discipline else "")
            + f"\nداده‌های واقعی:\n{json.dumps(stats, ensure_ascii=False, default=str)}\n"
            "این داده‌ها را به یک پاسخ کوتاه فارسی تبدیل کن."
        )
        llm_text = call_qwen_fn(user_prompt, system_prompt)
        if llm_text:
            result['answer'] = llm_text
            result['source'] = 'qwen'
            result['model'] = model_name

    return result
