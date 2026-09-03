"""
منطق «پیش‌بینی تاریخ صدور مدرک».

نکته‌ی کلیدی: master.xlsx یک لاگ روزانه‌ی انباشتی است (هر روز یک ردیف جدید
برای هر مدرک اضافه می‌شود، نه جایگزین). برای وضعیت *فعلیِ* هر مدرک، همیشه
باید فقط آخرین ردیف (بر اساس ستون Date) برای هر Document No. در نظر گرفته شود.

قانون «مدرک فعال» (باید تو جدول پیش‌بینی باشد):
    Responsible != 'Finished'  AND  Responsible does NOT start with 'Client'
یعنی فقط مدارکی که هنوز دست خودِ دیسیپلین/کانترکتوره (چه اولین‌بار در
انتظار صدورن، چه منتظر رویژن جدید بعد از کامنت کارفرما).

چرخه‌ی رویژن: هر بار که Comment Date روی یک مدرک تاریخ جدیدی بگیرد (یعنی
کارفرما پاسخ داده و مدرک دوباره وارد وضعیت Contractor شده)، این یک «چرخه‌ی
جدید» محسوب می‌شود و باید بشود دوباره پیش‌بینی گذاشت (پیش‌بینیِ چرخه‌ی قبل
دیگر معتبر نیست). برای همین، هر پیش‌بینی به یک "cycle_anchor" (آخرین
Comment Date شناخته‌شده در لحظه‌ی ثبت پیش‌بینی) گره می‌خورد.
"""
import pandas as pd


ACTIVE_RESPONSIBLE_PREFIXES_TO_EXCLUDE = ('Client',)
FINISHED_RESPONSIBLE = 'Finished'


def get_latest_snapshot(master_df: pd.DataFrame) -> pd.DataFrame:
    """آخرین ردیف (بر اساس Date) برای هر Document No. را برمی‌گرداند."""
    df = master_df.dropna(subset=['Document No.'])
    return df.sort_values('Date').groupby('Document No.', as_index=False).last()


def _apply_filter(df: pd.DataFrame, column: str, value) -> pd.DataFrame:
    """
    فیلتر روی یک ستون، با پشتیبانی از هم رشته‌ی تکی (رفتار قدیمی) و هم
    لیست/مجموعه‌ای از مقادیر (برای فیلترهای مولتی‌سلکت). 'همه' یا مقدار
    خالی یعنی بدون فیلتر.
    """
    if value is None:
        return df
    if isinstance(value, (list, tuple, set)):
        values = [str(v).strip() for v in value if str(v).strip() and str(v).strip() != 'همه']
        if not values:
            return df
        return df[df[column].astype(str).str.strip().isin(values)]
    value = str(value).strip()
    if not value or value == 'همه':
        return df
    return df[df[column].astype(str).str.strip() == value]


def get_active_documents(master_df: pd.DataFrame, project=None,
                           category=None, discipline=None) -> pd.DataFrame:
    """
    مدارکی که هنوز 'فعال' هستند (نه Finished، نه دست کارفرما) را برمی‌گرداند،
    با فیلترهای اختیاری پروژه/دسته/دیسیپلین. هر کدام از این سه می‌تواند یک
    رشته‌ی تکی ('پروژه X' یا 'همه') یا یک لیست از رشته‌ها باشد (مولتی‌سلکت).
    """
    latest = get_latest_snapshot(master_df)

    mask = (latest['Responsible'] != FINISHED_RESPONSIBLE)
    for prefix in ACTIVE_RESPONSIBLE_PREFIXES_TO_EXCLUDE:
        mask &= ~latest['Responsible'].astype(str).str.startswith(prefix)

    # مدارک حذف‌شده یا Hold هم از لیست کنار گذاشته می‌شن
    if 'Deleted' in latest.columns:
        mask &= (latest['Deleted'] != 'Yes')

    active = latest[mask].copy()

    active = _apply_filter(active, 'Project', project)
    active = _apply_filter(active, 'Category', category)
    active = _apply_filter(active, 'Discipline', discipline)

    return active


def compute_cycle_anchor(row) -> str:
    """
    شناسه‌ی چرخه‌ی فعلیِ یک مدرک: آخرین Comment Date شناخته‌شده (یا اگه
    هنوز کامنتی نخورده، رشته‌ی ثابت 'first-cycle'). وقتی این مقدار عوض بشه،
    یعنی مدرک وارد چرخه/رویژن جدیدی شده و پیش‌بینیِ قبلی دیگه معتبر نیست.
    """
    comment_date = row.get('Comment Date')
    if pd.isna(comment_date):
        return 'first-cycle'
    if hasattr(comment_date, 'strftime'):
        return comment_date.strftime('%Y-%m-%d')
    return str(comment_date)


def compute_days_overdue(predicted_date, today=None) -> int:
    """چند روز از تاریخ پیش‌بینی‌شده گذشته (مثبت = دیرکرد، منفی/صفر = هنوز نرسیده)."""
    if predicted_date is None:
        return None
    if today is None:
        today = pd.Timestamp.now().date()
    if hasattr(predicted_date, 'date'):
        predicted_date = predicted_date.date()
    return (today - predicted_date).days


PREDICTION_LEAD_DAYS = 5
PREDICTION_ON_TIME_POINTS = 5
PREDICTION_LATE_POINTS = -5


def _as_date(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (ValueError, TypeError):
        pass
    if hasattr(value, 'date') and not isinstance(value, type(pd.Timestamp.now().date())):
        try:
            return value.date()
        except Exception:
            pass
    parsed = pd.to_datetime(value, errors='coerce')
    if pd.isna(parsed):
        return None
    return parsed.date()


def prediction_registered_at(record) -> str:
    """اولین زمان ثبت پیش‌بینی در چرخه‌ی فعلی (نه آخرین ویرایش)."""
    history = (record or {}).get('history') or []
    if history:
        return history[0].get('set_at') or (record or {}).get('set_at')
    return (record or {}).get('set_at')


def compute_prediction_accuracy_points(predicted_date, actual_issue_date,
                                         registered_at=None,
                                         min_lead_days: int = PREDICTION_LEAD_DAYS,
                                         early_or_ontime_points: int = PREDICTION_ON_TIME_POINTS,
                                         late_points: int = PREDICTION_LATE_POINTS) -> int:
    """
    امتیاز دقت پیش‌بینی فقط وقتی حساب می‌شود که حداقل ۵ روز قبل از
    صدور واقعی ثبت شده باشد. در غیر این صورت صفر است.

    اگر واجد شرایط باشد:
      صدور به‌موقع یا زودتر از تاریخ پیش‌بینی → +۵
      تأخیر در صدور نسبت به تاریخ پیش‌بینی → −۵
    """
    predicted_date = _as_date(predicted_date)
    actual_issue_date = _as_date(actual_issue_date)
    if predicted_date is None or actual_issue_date is None:
        return 0

    registered_date = _as_date(registered_at)
    if registered_date is None:
        return 0
    if (actual_issue_date - registered_date).days < min_lead_days:
        return 0

    if actual_issue_date <= predicted_date:
        return early_or_ontime_points
    return late_points
