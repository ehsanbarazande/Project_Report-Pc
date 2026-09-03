"""
محاسبه‌ی due date بر اساس روزهای کاری (بدون پنج‌شنبه/جمعه و تعطیلات رسمی).

منبع تعطیلات: یک فایل اکسل ساده (Holidays.xlsx) با یک ستون Date، که
هر سال باید طبق تقویم رسمی (calendar.ir یا time.ir) آپدیت بشه — چون
بخشی از تعطیلات (تاسوعا/عاشورا، عید فطر، عید قربان و ...) بر اساس تقویم
قمری هستن و هر سال میلادی/شمسی تاریخ متفاوتی دارن.
"""
from datetime import timedelta, datetime, date
import pandas as pd


def load_holidays(holidays_path: str) -> set:
    """
    فایل اکسل تعطیلات رو می‌خونه. باید یک ستون به اسم Date داشته باشه
    (هر مقداری که pandas بتونه به تاریخ تبدیل کنه، مثل 1405/01/01 یا
    2026-03-21 قابل قبوله - فقط باید در قالب میلادی ذخیره شده باشه چون
    Action Date/Close Date هم میلادی هستن).
    """
    df = pd.read_excel(holidays_path, engine='openpyxl')
    date_col = df.columns[0]
    return set(pd.to_datetime(df[date_col]).dt.date)


def add_working_days(start, n_days: int, holidays: set = frozenset()) -> date:
    """
    n_days روز کاری بعد از start برمی‌گردونه.
    پنج‌شنبه (weekday=3)، جمعه (weekday=4) و هر تاریخی که تو holidays باشه
    رد می‌شه (حساب نمی‌شه).
    """
    if isinstance(start, datetime):
        current = start.date()
    elif isinstance(start, date):
        current = start
    else:
        current = pd.to_datetime(start).date()

    added = 0
    while added < n_days:
        current = current + timedelta(days=1)
        if current.weekday() in (3, 4):  # پنجشنبه، جمعه
            continue
        if current in holidays:
            continue
        added += 1
    return current


def compute_due_date(t0, holidays: set = frozenset(), due_days: int = 5) -> date:
    """پوسته‌ی ساده روی add_working_days برای استفاده مستقیم تو فرمول due date."""
    return add_working_days(t0, due_days, holidays)
