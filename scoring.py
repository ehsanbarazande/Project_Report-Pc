"""
موتور نهایی امتیازدهی سیستم رقابتی.

ترکیب می‌کنه:
    - revision_metrics.py  → سهم‌زمان هر نفر در هر رویژن + پاسخ به Distribute
    - due_date_engine.py   → due date بر اساس روز کاری (۵ روز، بدون پنج‌شنبه/جمعه/تعطیلات)

فرمول نهایی (به‌روزشده — با احتساب حجم کار):
    امتیاز = 0.40 × امتیاز رعایت Due Date
           + 0.25 × امتیاز کیفیت (تعداد رویژن لازم تا تایید نهایی - کمتر=بهتر)
           + 0.15 × امتیاز سرعت خالص (سهم‌زمان نسبت به میانگین هم‌رده)
           + 0.20 × امتیاز حجم کار (تعداد رویژن نسبت به پرکارترین نفرِ هم‌رده)

+ یک امتیاز جایزه‌ی جداگانه (خارج از فرمول بالا) برای پاسخ به Distribute:
    پاسخ در عرض ۳ روز یا کمتر  → +۵ امتیاز
    پاسخ بیشتر از ۳ روز        → -۳ امتیاز

مقایسه فقط داخل هم‌رده انجام می‌شه (کارشناس با کارشناس، ارشد با ارشد، مدیر با مدیر).
DCC وارد امتیازدهی نمی‌شه.

نکته‌ی تیم‌های تک‌نفره: تو دیسیپلین‌هایی که کارشناس جدا ندارن، همون ارشد هم
کار کارشناسی هم کار ارشد رو انجام می‌ده. برای این افراد، به‌جای زیرسهمیه‌ی
عادیِ ارشد (۲۰٪ = ۱ روز)، از زیرسهمیه‌ی ترکیبی (۹۰٪ = ۴.۵ روز = کارشناس+ارشد)
استفاده می‌شه، و برای مقایسه‌ی سرعت هم فقط با هم‌گروه‌های خودشون (بقیه‌ی
تیم‌های تک‌نفره) مقایسه می‌شن، نه با ارشدهایی که فقط کار بازبینی می‌کنن.
"""
import pandas as pd
import numpy as np
from revision_metrics import build_revision_groups, compute_person_durations, compute_distribute_response_times
from due_date_engine import compute_due_date

# سهمِ هر گروه از کلِ due date (جمع باید ۱ بشه؛ senior_combined = کارشناس+ارشد باهم)
PEER_GROUP_BUDGET_FRACTION = {
    'specialist': 0.70,
    'senior': 0.20,
    'senior_combined': 0.90,   # تیم تک‌نفره: هم کارشناس هم ارشد
    'manager': 0.10,
}

DISTRIBUTE_ON_TIME_DAYS = 3
DISTRIBUTE_ON_TIME_POINTS = 5
DISTRIBUTE_LATE_POINTS = -3

SCORE_WEIGHTS = {
    'on_time': 0.40,
    'quality': 0.25,
    'speed': 0.15,
    'volume': 0.20,
}


def compute_all_metrics(history_df: pd.DataFrame, holidays: set = frozenset(),
                          due_days: int = 5, return_details: bool = False,
                          resolve_name=None, date_from=None, date_to=None,
                          prediction_bonus_points: dict = None):
    """
    resolve_name: تابع اختیاری (raw_name -> نام نمایشی استاندارد) که قبل از
    هر گونه تجمیع روی نام افراد اعمال می‌شه. این باعث می‌شه اگه یک نفر با
    نگارش‌های متفاوت (بزرگ/کوچک، فاصله‌ی اضافه و ...) تو تاریخچه ثبت شده
    باشه، به‌جای چند ردیف جدا، یک نفر واحد در جدول رتبه‌بندی دیده بشه.

    date_from / date_to: بازه‌ی زمانیِ اختیاری (datetime.date یا None).
    اگه داده بشن، فقط رویژن‌هایی که «شروع‌شون» (T0) تو این بازه بوده حساب
    می‌شن — یعنی امتیازدهی «سه ماه گذشته» با «ماه گذشته» نتیجه‌ی متفاوتی
    می‌ده. توجه: گروه‌بندی رویژن‌ها همیشه رو کل تاریخچه انجام می‌شه (تا
    یکپارچگی محاسبه‌ی سهم‌زمان خراب نشه)، فقط در انتها رویژن‌های خارج از
    بازه کنار گذاشته می‌شن.

    prediction_bonus_points: دیکشنری اختیاری {person: امتیاز} از دقتِ
    پیش‌بینی تاریخ صدور (محاسبه‌شده تو app.py، چون به Redis نیاز داره).
    فقط کسانی که واقعاً پیش‌بینی ثبت کرده‌اند تو این دیکشنری هستن — کسی که
    پیش‌بینی نکرده، اصلاً وارد این محاسبه نمی‌شه (نه امتیاز مثبت، نه منفی).

    اگه return_details=True باشه، به‌جای یک دیتافریم، سه‌تایی
    (result_df, durations_df, distribute_df) برمی‌گرده.
    """
    df = build_revision_groups(history_df)
    durations = compute_person_durations(df)
    if durations.empty:
        empty = pd.DataFrame()
        return (empty, empty, empty) if return_details else empty

    # ===== فیلتر بازه‌ی زمانی (بر اساس T0 - شروع رویژن) =====
    if date_from is not None or date_to is not None:
        def _t0_date(v):
            if pd.isna(v):
                return None
            return v.date() if hasattr(v, 'date') else v

        t0_dates = durations['T0'].apply(_t0_date)
        mask = pd.Series(True, index=durations.index)
        if date_from is not None:
            mask &= t0_dates.apply(lambda d: d is not None and d >= date_from)
        if date_to is not None:
            mask &= t0_dates.apply(lambda d: d is not None and d <= date_to)
        durations = durations[mask]
        if durations.empty:
            empty = pd.DataFrame()
            return (empty, empty, empty) if return_details else empty

    if resolve_name is not None:
        durations['person'] = durations['person'].apply(resolve_name)
        # هرکس تو Role.xlsx نباشه (مثلاً از شرکت رفته)، resolve_name می‌شه
        # None و اینجا کاملاً از محاسبات حذف می‌شه — نه فقط از نمایش.
        durations = durations[durations['person'].notna()]
        if durations.empty:
            empty = pd.DataFrame()
            return (empty, empty, empty) if return_details else empty

    if 'is_combined_role' not in durations.columns:
        durations['is_combined_role'] = False

    # ===== محاسبه‌ی due date و تاخیر برای هر رویژن =====
    rev_info = durations[['Document No.', 'revision_group', 'T0', 'close_date']].drop_duplicates()
    rev_info = rev_info.dropna(subset=['T0'])
    rev_info['due_date'] = rev_info['T0'].apply(lambda t0: compute_due_date(t0, holidays, due_days))
    rev_info['delay_days'] = rev_info.apply(
        lambda r: (pd.to_datetime(r['close_date']).date() - r['due_date']).days
        if pd.notna(r['close_date']) else None, axis=1
    )
    rev_info['is_late'] = rev_info['delay_days'].apply(lambda d: (d is not None) and d > 0)

    durations = durations.merge(
        rev_info[['Document No.', 'revision_group', 'due_date', 'delay_days', 'is_late']],
        on=['Document No.', 'revision_group'], how='left'
    )

    # ===== گروهِ هم‌رده‌ی واقعی برای هر ردیف (تیم تک‌نفره جدا از ارشدهای عادی) =====
    durations['peer_group'] = durations.apply(
        lambda r: 'senior_combined' if (r['role'] == 'senior' and r['is_combined_role']) else r['role'],
        axis=1
    )

    # ===== زیرسهمیه‌ی روز هر گروه (بر اساس due_days کلی) =====
    role_budget_days = {pg: due_days * frac for pg, frac in PEER_GROUP_BUDGET_FRACTION.items()}
    durations['role_budget_days'] = durations['peer_group'].map(role_budget_days)
    durations['is_primary_delay_cause'] = durations['duration_days'] > durations['role_budget_days']

    # میانگین هر گروهِ هم‌رده (برای مقایسه‌ی سرعت)
    peer_avg = durations.groupby('peer_group')['duration_days'].mean().to_dict()
    durations['role_avg_duration'] = durations['peer_group'].map(peer_avg)

    # نسبت سرعتِ هر ردیف نسبت به میانگین هم‌گروهِ خودش (سقف ۱ = حداکثر امتیاز)
    durations['row_speed_ratio'] = durations.apply(
        lambda r: max(0, min(1, r['role_avg_duration'] / r['duration_days'])) if r['duration_days'] > 0 else 1.0,
        axis=1
    )

    # ===== کیفیت: تعداد کل رویژن‌های هر مدرک (کمتر = بهتر) =====
    total_revs_per_doc = durations.groupby('Document No.')['rev_seq'].max().to_dict()
    durations['total_revisions_for_doc'] = durations['Document No.'].map(total_revs_per_doc)

    # ===== تجمیع نهایی به ازای هر نفر/نقش =====
    results = []
    for (person, role), grp in durations.groupby(['person', 'role']):
        n_revisions = len(grp)
        n_delay = int(grp['is_primary_delay_cause'].sum()) if n_revisions else 0

        on_time_score = 1 - (n_delay / n_revisions) if n_revisions > 0 else 1.0
        speed_score = grp['row_speed_ratio'].mean() if n_revisions else 1.0

        avg_revisions_needed = float(grp['total_revisions_for_doc'].mean()) if n_revisions else 0.0
        quality_score = max(0, min(1, 1 / avg_revisions_needed)) if avg_revisions_needed > 0 else 1.0
        budget = float(grp['role_budget_days'].iloc[0]) if n_revisions else None
        peer_avg_d = float(grp['role_avg_duration'].iloc[0]) if n_revisions else None
        combined = bool(grp['is_combined_role'].any()) if 'is_combined_role' in grp.columns else False
        peer_group = grp['peer_group'].iloc[0] if n_revisions and 'peer_group' in grp.columns else role

        results.append({
            'person': person, 'role': role, 'n_revisions_touched': n_revisions,
            'delay_causes_count': n_delay,
            'on_time_score': round(on_time_score, 3),
            'quality_score': round(quality_score, 3),
            'speed_score': round(speed_score, 3),
            'avg_duration_days': round(float(grp['duration_days'].mean()), 2) if n_revisions else 0,
            'avg_revisions_needed': round(avg_revisions_needed, 2),
            'role_budget_days': round(budget, 2) if budget is not None else None,
            'peer_avg_duration': round(peer_avg_d, 2) if peer_avg_d is not None else None,
            'is_combined_role': combined,
            'peer_group': peer_group,
        })

    result_df = pd.DataFrame(results)
    if result_df.empty:
        empty = pd.DataFrame()
        return (result_df, durations, empty) if return_details else result_df

    # ===== امتیاز حجم (Volume): نسبت به پرکارترین نفرِ همون رده =====
    max_revisions_per_role = result_df.groupby('role')['n_revisions_touched'].transform('max')
    result_df['max_revisions_in_role'] = max_revisions_per_role.fillna(0).astype(int)
    result_df['volume_score'] = np.where(
        max_revisions_per_role > 0,
        (result_df['n_revisions_touched'] / max_revisions_per_role).round(3),
        0.0,
    )

    # ===== امتیاز نهایی (با احتساب حجم کار) =====
    w = SCORE_WEIGHTS
    result_df['final_score'] = (
        w['on_time'] * result_df['on_time_score']
        + w['quality'] * result_df['quality_score']
        + w['speed'] * result_df['speed_score']
        + w['volume'] * result_df['volume_score']
    ).round(4)

    # ===== امتیاز جایزه‌ی پاسخ به Distribute (جدا از فرمول بالا) =====
    distribute_df = compute_distribute_response_times(history_df)
    if not distribute_df.empty:
        if date_from is not None or date_to is not None:
            d_dates = distribute_df['distribute_date'].apply(lambda v: v.date() if hasattr(v, 'date') else v)
            mask = pd.Series(True, index=distribute_df.index)
            if date_from is not None:
                mask &= d_dates.apply(lambda d: d is not None and d >= date_from)
            if date_to is not None:
                mask &= d_dates.apply(lambda d: d is not None and d <= date_to)
            distribute_df = distribute_df[mask]
        if resolve_name is not None and not distribute_df.empty:
            distribute_df['to_discipline_person'] = distribute_df['to_discipline_person'].apply(resolve_name)
            distribute_df = distribute_df[distribute_df['to_discipline_person'].notna()]
    if not distribute_df.empty:
        distribute_df['points'] = distribute_df['response_days'].apply(
            lambda d: DISTRIBUTE_ON_TIME_POINTS if d <= DISTRIBUTE_ON_TIME_DAYS else DISTRIBUTE_LATE_POINTS
        )
        bonus_summary = distribute_df.groupby('to_discipline_person').agg(
            distribute_bonus_points=('points', 'sum'),
            distribute_responses_count=('points', 'count')
        ).reset_index().rename(columns={'to_discipline_person': 'person'})

        result_df = result_df.merge(bonus_summary, on='person', how='left')
    else:
        result_df['distribute_bonus_points'] = 0
        result_df['distribute_responses_count'] = 0

    result_df['distribute_bonus_points'] = result_df['distribute_bonus_points'].fillna(0).astype(int)
    result_df['distribute_responses_count'] = result_df['distribute_responses_count'].fillna(0).astype(int)

    # امتیاز هسته بدون جایزه‌های تیمی. Distribute و پیش‌بینی بعداً
    # یک‌بار در سطح دیسیپلین اعمال می‌شوند.
    result_df['core_score'] = (result_df['final_score'] * 100).round(1)
    result_df['prediction_bonus_points'] = 0
    result_df['total_score'] = result_df['core_score'].clip(lower=0)

    # رتبه‌بندی موقت؛ بعد از اعمال جایزه‌های مشترک دوباره محاسبه می‌شود
    result_df['rank_in_role'] = result_df.groupby('role')['total_score'] \
        .rank(ascending=False, method='min').astype(int)
    result_df = result_df.sort_values(['role', 'rank_in_role'])

    if return_details:
        return result_df, durations, distribute_df
    return result_df
