"""
محاسبه‌ی متریک‌های رقابتی از روی history.xlsx واقعی (با ستون From Name آماده).

منطق اصلی (طبق توضیحات کاربر + بررسی داده‌ی واقعی):

۱) گروه‌بندی رویژن:
   - رویژن‌های بسته‌شده: بر اساس (Document No., External Revision)
   - رویژن جاری (هنوز بسته نشده): تمام سطرهای Ongoing='Yes' یک مدرک،
     یک گروه واحد به اسم 'ONGOING' حساب می‌شن.
   - ترتیب رویژن‌ها (rev_seq) از روی قدیمی‌ترین Action Date هر گروه.

۲) نقش هر تراکنش از روی کد کنار اسم گرفته می‌شه (MDJ/MDS/EM/DCC یا CTR)،
   نه از جدول Role.xlsx (چون افراد گاهی نقش متفاوتی از نقش پایه‌شون بازی
   می‌کنن، طبق مشاهده‌ی واقعی).

۳) سهم‌زمان هر نفر داخل یک رویژن:
   - کارشناس (MDJ): start = min(Action Date | Assign → این کارشناس)
                     end   = max(Action Date | Issue  از این کارشناس)
   - ارشد (MDS):     start = min(Action Date | Issue از کارشناس‌ها به این ارشد)
                     end   = max(Action Date | Issue از این ارشد به مدیر)
   - مدیر (EM):       start = min(Action Date | Issue از ارشد به این مدیر)
                     end   = max(Action Date | Assign از این مدیر به DCC)
   - DCC: وارد امتیازدهی نمی‌شه (طبق تایید کاربر)، فقط برای تکمیل زنجیره.

۴) شروع رویژن (T0) و Due Date:
   - رویژن اول هر مدرک: T0 = اولین Action Date گروه
   - رویژن‌های بعدی: T0 = Comment Date ثبت‌شده روی سطرهای همون رویژن
     (که پاسخ کارفرما به رویژن قبلیه)
   - Due Date = T0 + آفست روز پروژه (از جدول تنظیمات، جداگانه تعریف می‌شه)

۵) پاسخ به Distribute (بین‌دیسیپلینی):
   هر سطر Distribute با اولین سطر Comment بعدی‌اش با همون Document No. و
   طرفین معکوس، جفت می‌شه؛ فاصله‌ی زمانی‌شون = زمان پاسخ (امتیاز مثبت جدا).

۶) امتیاز کیفیت (ستون Status و Comment) به‌صورت جدا محاسبه می‌شه.
"""
import re
import pandas as pd
import numpy as np


ROLE_CODES = {'MDJ': 'specialist', 'MDS': 'senior', 'EM': 'manager',
              'DCC': 'dcc', 'CTR': 'dcc'}

STATUS_TIER = {
    'IFI': 1, 'IFR': 1,
    'IFA': 2, 'AFP': 2, 'AFD': 2, 'FA': 2,
    'IFC': 3, 'AFC': 3,
}

COMMENT_TIER = {
    'APPROVED': 5,
    'APPROVED AS NOTE': 4,
    'APPROVED WITH COMMENT': 3,
    'COMMENTED': 2, 'CM': 2,
    'REJECT': 1, 'RJ': 1,
}


def extract_role_code(name_with_code):
    if not isinstance(name_with_code, str):
        return None, None
    m = re.match(r'^(.*?)\s*\(([^)]+)\)\s*$', name_with_code.strip())
    if not m:
        return name_with_code.strip(), None
    return m.group(1).strip(), m.group(2).strip().upper()


def status_score(value):
    if not isinstance(value, str):
        return None
    return STATUS_TIER.get(value.strip().upper())


def comment_score(value):
    if not isinstance(value, str):
        return None
    return COMMENT_TIER.get(value.strip().upper())


def build_revision_groups(history_df: pd.DataFrame) -> pd.DataFrame:
    """
    به history_df دو ستون اضافه می‌کنه: revision_group (شناسه‌ی گروه) و rev_seq
    (ترتیب رویژن برای اون مدرک، از ۱ شروع می‌شه).
    """
    df = history_df.copy()
    df['revision_group'] = np.where(
        df['Ongoing'] == 'Yes',
        'ONGOING',
        df['External Revision'].astype(str)
    )

    # ترتیب رویژن‌ها بر اساس قدیمی‌ترین Action Date هر گروه
    order = (
        df.groupby(['Document No.', 'revision_group'])['Action Date']
        .min()
        .reset_index()
        .sort_values(['Document No.', 'Action Date'])
    )
    order['rev_seq'] = order.groupby('Document No.').cumcount() + 1
    df = df.merge(order[['Document No.', 'revision_group', 'rev_seq']],
                   on=['Document No.', 'revision_group'], how='left')
    return df


def compute_person_durations(df_with_groups: pd.DataFrame) -> pd.DataFrame:
    """
    برای هر (Document No., revision_group)، سهم‌زمان هر نفر رو طبق نقشش
    محاسبه می‌کنه. خروجی: یک ردیف به‌ازای هر (مدرک، رویژن، نفر، نقش).
    """
    records = []

    for (doc_no, rev_group), group in df_with_groups.groupby(['Document No.', 'revision_group']):
        group = group.sort_values('Action Date')
        rev_seq = group['rev_seq'].iloc[0]

        # T0 رویژن
        if rev_seq == 1:
            t0 = group['Action Date'].min()
        else:
            comment_dates = group['Comment Date'].dropna()
            t0 = comment_dates.min() if len(comment_dates) else group['Action Date'].min()

        close_date = group['Close Date'].dropna()
        close_date = close_date.max() if len(close_date) else None

        # --- کارشناس (MDJ) ---
        specialists = {}
        for _, row in group.iterrows():
            to_name, to_code = extract_role_code(row['To Name'])
            from_name, from_code = extract_role_code(row['From Name'])

            if row['Log Status'] == 'Assign' and ROLE_CODES.get(to_code) == 'specialist':
                specialists.setdefault(to_name, {})['start'] = min(
                    row['Action Date'], specialists.get(to_name, {}).get('start', row['Action Date'])
                )
            if row['Log Status'] == 'Issue' and ROLE_CODES.get(from_code) == 'specialist':
                specialists.setdefault(from_name, {})['end'] = max(
                    row['Action Date'], specialists.get(from_name, {}).get('end', row['Action Date'])
                )

        for person, times in specialists.items():
            if 'start' in times and 'end' in times:
                records.append({
                    'Document No.': doc_no, 'revision_group': rev_group, 'rev_seq': rev_seq,
                    'person': person, 'role': 'specialist',
                    'duration_days': (times['end'] - times['start']).total_seconds() / 86400,
                    'T0': t0, 'close_date': close_date,
                    'is_combined_role': False
                })

        # --- ارشد (MDS): از دریافتِ Issue از کارشناس، تا issue به مدیر ---
        # نکته‌ی مهم: تو دیسیپلین‌های تک‌نفره، ممکنه اصلاً کارشناسی وجود
        # نداشته باشه و ارشد مستقیم از DCC/CTR مدرک رو تحویل بگیره (Assign)
        # و خودش مستقیم برای مدیر Issue کنه. این حالت رو فقط وقتی به‌عنوان
        # «شروع کار ارشد» حساب می‌کنیم که هیچ کارشناسی تو همین رویژن دخیل
        # نبوده باشه — وگرنه (تو رویژن‌های عادی که کارشناس هم داره) این
        # Assign اولیه فقط انتقال کامنت کارفرماست، نه شروع کار خودِ ارشد.
        specialist_involved = any(
            ROLE_CODES.get(extract_role_code(r['To Name'])[1]) == 'specialist'
            for _, r in group.iterrows()
        )

        seniors = {}
        for _, row in group.iterrows():
            to_name, to_code = extract_role_code(row['To Name'])
            from_name, from_code = extract_role_code(row['From Name'])

            if row['Log Status'] == 'Issue' and ROLE_CODES.get(to_code) == 'senior' \
               and ROLE_CODES.get(from_code) == 'specialist':
                seniors.setdefault(to_name, {})['start'] = min(
                    row['Action Date'], seniors.get(to_name, {}).get('start', row['Action Date'])
                )
            # ✅ حالت دیسیپلین تک‌نفره: فقط وقتی هیچ کارشناسی تو رویژن نبوده
            if not specialist_involved and row['Log Status'] == 'Assign' \
               and ROLE_CODES.get(to_code) == 'senior' and ROLE_CODES.get(from_code) == 'dcc':
                seniors.setdefault(to_name, {})['start'] = min(
                    row['Action Date'], seniors.get(to_name, {}).get('start', row['Action Date'])
                )
            if row['Log Status'] == 'Issue' and ROLE_CODES.get(from_code) == 'senior' \
               and ROLE_CODES.get(to_code) == 'manager':
                seniors.setdefault(from_name, {})['end'] = max(
                    row['Action Date'], seniors.get(from_name, {}).get('end', row['Action Date'])
                )

        for person, times in seniors.items():
            if 'start' in times and 'end' in times:
                records.append({
                    'Document No.': doc_no, 'revision_group': rev_group, 'rev_seq': rev_seq,
                    'person': person, 'role': 'senior',
                    'duration_days': (times['end'] - times['start']).total_seconds() / 86400,
                    'T0': t0, 'close_date': close_date,
                    # اگه کارشناسی تو این رویژن دخیل نبوده، یعنی این نفر خودش
                    # هم کار کارشناسی هم کار ارشد رو انجام داده (تیم تک‌نفره)
                    'is_combined_role': not specialist_involved
                })

        # --- مدیر مهندسی (EM): از دریافتِ Issue از ارشد، تا Assign به DCC ---
        managers = {}
        for _, row in group.iterrows():
            to_name, to_code = extract_role_code(row['To Name'])
            from_name, from_code = extract_role_code(row['From Name'])

            if row['Log Status'] == 'Issue' and ROLE_CODES.get(to_code) == 'manager':
                managers.setdefault(to_name, {})['start'] = min(
                    row['Action Date'], managers.get(to_name, {}).get('start', row['Action Date'])
                )
            if row['Log Status'] == 'Assign' and ROLE_CODES.get(from_code) == 'manager' \
               and ROLE_CODES.get(to_code) == 'dcc':
                managers.setdefault(from_name, {})['end'] = max(
                    row['Action Date'], managers.get(from_name, {}).get('end', row['Action Date'])
                )

        for person, times in managers.items():
            if 'start' in times and 'end' in times:
                records.append({
                    'Document No.': doc_no, 'revision_group': rev_group, 'rev_seq': rev_seq,
                    'person': person, 'role': 'manager',
                    'duration_days': (times['end'] - times['start']).total_seconds() / 86400,
                    'T0': t0, 'close_date': close_date,
                    'is_combined_role': False
                })

    return pd.DataFrame(records)


def compute_distribute_response_times(history_df: pd.DataFrame) -> pd.DataFrame:
    """
    هر سطر Distribute رو با اولین Comment بعدی‌اش (همون مدرک، طرفین معکوس) جفت می‌کنه.
    """
    records = []
    for doc_no, group in history_df.groupby('Document No.'):
        group = group.sort_values('Action Date')
        distributes = group[group['Log Status'] == 'Distribute']
        comments = group[group['Log Status'] == 'Comment']

        for _, drow in distributes.iterrows():
            d_from, _ = extract_role_code(drow['From Name'])
            d_to, _ = extract_role_code(drow['To Name'])
            match = comments[
                (comments['Action Date'] > drow['Action Date']) &
                (comments['From Name'].apply(lambda x: extract_role_code(x)[0]) == d_to) &
                (comments['To Name'].apply(lambda x: extract_role_code(x)[0]) == d_from)
            ]
            if len(match):
                crow = match.iloc[0]
                records.append({
                    'Document No.': doc_no,
                    'from_discipline_person': d_from,
                    'to_discipline_person': d_to,
                    'distribute_date': drow['Action Date'],
                    'response_date': crow['Action Date'],
                    'response_days': (crow['Action Date'] - drow['Action Date']).total_seconds() / 86400,
                    'outcome': crow.get('Comment')
                })
    return pd.DataFrame(records)
