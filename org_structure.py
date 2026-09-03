"""ساختار سازمانی امتیازدهی: دیسیپلین تجمعی و پورتفولیوی پروژه."""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Callable, Dict, List, Optional

import pandas as pd


ROLE_ALIASES = {
    'mdj': 'specialist',
    'specialist': 'specialist',
    'کارشناس': 'specialist',
    'engineer': 'specialist',
    'mds': 'senior',
    'senior': 'senior',
    'ارشد': 'senior',
    'مهندس ارشد': 'senior',
    'em': 'manager',
    'manager': 'manager',
    'مدیر': 'manager',
    'مدیر مهندسی': 'manager',
}

ROLE_ORDER = {'manager': 0, 'senior': 1, 'specialist': 2}

EXCLUDED_DISCIPLINES = {'management'}
CIVIL_CANON = 'Civil'
ARCH_CANON = 'Architecture'

PROJECT_EM_RULES = [
    (('ahvaz', 'اهواز'), ['Ghasem Javadpour', 'قاسم جواد پور', 'جوادپور']),
    (('abadan', 'آبادان', 'ابادان'), ['Alireza Aber', 'علیرضا عابر']),
    (('serajeh', 'سراجه', 'aghajari', 'آغاجاری', 'اغاجاری', 'pazanan', 'پازنان'),
     ['Mahmoud Movafagh', 'Mahmoud Salimmovafagh', 'Mahmoud salimmovafagh', 'محمود سلیم موفق', 'محمود موفق']),
]

DISCIPLINE_TEAMS = {
    CIVIL_CANON: {
        'senior': ['Ali Ordoukhani', 'علی اردوخانی'],
        'specialist': [
            'Bahareh Dadashpour', 'Behareh Dadashpour', 'بهاره داداش‌پور', 'بهاره داداش پور',
            'Maryam Tajik', 'مریم تاجیک',
        ],
    },
    ARCH_CANON: {
        'senior': ['Alireza Karegar', 'Ali Karegar', 'علیرضا کارگر'],
        'specialist': [],
    },
}


def _fold_text(text) -> str:
    value = str(text or '').strip().lower()
    for src, dst in (('آ', 'ا'), ('ي', 'ی'), ('ك', 'ک'), ('‌', ''), ('&', ' '), ('/', ' '), ('-', ' '), ('+', ' ')):
        value = value.replace(src, dst)
    return re.sub(r'\s+', ' ', value).strip()


def canonicalize_discipline(name) -> str:
    if name is None:
        return 'نامشخص'
    try:
        if pd.isna(name):
            return 'نامشخص'
    except (ValueError, TypeError):
        pass
    raw = str(name).strip()
    if not raw or raw.lower() in ('nan', 'none', 'نامشخص'):
        return 'نامشخص'
    folded = _fold_text(raw)
    compact = folded.replace(' ', '')
    if compact in EXCLUDED_DISCIPLINES or 'management' in folded or compact in ('مدیریت', 'مديريت'):
        return 'Management'
    tokens = set(folded.split())
    has_civil = bool(tokens & {'civil', 'سیویل', 'سيويل', 'عمران'})
    has_struct = bool(tokens & {'structure', 'structures', 'سازه'})
    has_arch = bool(tokens & {'architecture', 'architectural', 'معماری', 'معماري', 'معمار'})
    if has_arch and not has_civil and not has_struct:
        return ARCH_CANON
    if has_civil or has_struct:
        return CIVIL_CANON
    return raw


def is_excluded_discipline(name) -> bool:
    return canonicalize_discipline(name) == 'Management'


def project_em_aliases(project: str) -> List[str]:
    folded = _fold_text(project)
    compact = folded.replace(' ', '')
    for keys, aliases in PROJECT_EM_RULES:
        for key in keys:
            needle = _fold_text(key)
            if needle.replace(' ', '') in compact or needle in folded:
                return list(aliases)
    return []


def _norm_role(value) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (ValueError, TypeError):
        pass
    text = str(value).strip().lower()
    if not text:
        return None
    if text in ROLE_ALIASES:
        return ROLE_ALIASES[text]
    for key, role in ROLE_ALIASES.items():
        if key in text:
            return role
    return None


def load_org_roster(role_path, name_mapping, normalize_name, to_proper_case) -> List[dict]:
    """Role.xlsx → لیست افراد با دیسیپلین و نقش."""
    roster = []
    try:
        df = pd.read_excel(role_path, engine='openpyxl')
    except Exception:
        return roster

    name_col = discipline_col = role_col = project_col = None
    for col in df.columns:
        low = str(col).strip().lower()
        if low in ('person name', 'name', 'نام') and name_col is None:
            name_col = col
        if low in ('discipline', 'دیسیپلین') and discipline_col is None:
            discipline_col = col
        if low in ('role', 'position', 'title', 'نقش', 'سمت') and role_col is None:
            role_col = col
        if low in ('project', 'پروژه') and project_col is None:
            project_col = col

    if name_col is None:
        return roster

    english_to_persian = {}
    for key, persian in (name_mapping or {}).items():
        english_to_persian[normalize_name(key)] = persian

    for _, row in df.iterrows():
        raw = row.get(name_col)
        if pd.isna(raw) or not str(raw).strip():
            continue
        canon = to_proper_case(str(raw).strip())
        discipline = canonicalize_discipline(row.get(discipline_col)) if discipline_col else 'نامشخص'
        role = _norm_role(row.get(role_col)) if role_col else None
        project = str(row.get(project_col)).strip() if project_col and pd.notna(row.get(project_col)) else ''
        roster.append({
            'name': canon,
            'persian_name': english_to_persian.get(normalize_name(canon), ''),
            'discipline': discipline or 'نامشخص',
            'role': role,
            'project': project,
        })
    return roster


def roster_lookups(roster: List[dict]):
    by_name = {}
    by_persian = {}
    for item in roster:
        by_name[item['name']] = item
        if item.get('persian_name'):
            by_persian[item['persian_name']] = item
    return by_name, by_persian


def apply_shared_team_bonuses(scores: List[dict], distribute_df, prediction_by_discipline: Dict[str, dict],
                              roster: List[dict], resolve_name: Optional[Callable] = None):
    """
    امتیاز هسته هر نفر جدا می‌ماند.
    Distribute و پیش‌بینی یک‌بار برای کل دیسیپلین حساب می‌شوند و به همه
    اعضای همان دیسیپلین نمایش داده می‌شوند، ولی در جمع دیسیپلین فقط یک‌بار می‌آیند.
    """
    by_name, by_persian = roster_lookups(roster)
    person_disc = {}
    person_role = {}

    for item in roster:
        person_disc[item['name']] = canonicalize_discipline(item['discipline'])
        if item.get('role'):
            person_role[item['name']] = item['role']

    for row in scores:
        person = row.get('person')
        info = by_name.get(person) or by_persian.get(person) or {}
        disc = canonicalize_discipline(info.get('discipline') or person_disc.get(person) or row.get('discipline') or 'نامشخص')
        row['discipline'] = disc
        if not row.get('org_role'):
            row['org_role'] = info.get('role') or row.get('role')
        person_disc[person] = disc

    disc_distribute = defaultdict(int)
    if distribute_df is not None and not getattr(distribute_df, 'empty', True):
        for _, event in distribute_df.iterrows():
            person = event.get('to_discipline_person')
            if resolve_name:
                person = resolve_name(person)
            if not person:
                continue
            disc = canonicalize_discipline(
                person_disc.get(person) or (by_name.get(person) or {}).get('discipline') or 'نامشخص'
            )
            if is_excluded_discipline(disc):
                continue
            try:
                disc_distribute[disc] += int(event.get('points') or 0)
            except (TypeError, ValueError):
                continue

    for row in scores:
        disc = canonicalize_discipline(row.get('discipline') or 'نامشخص')
        row['discipline'] = disc
        core = float(row.get('core_score', row.get('final_score', 0) * 100) or 0)
        row['core_score'] = round(core, 1)
        if is_excluded_discipline(disc) or row.get('role') == 'manager':
            row['shared_distribute_points'] = 0
            row['shared_prediction_points'] = 0
            row['total_score'] = max(0, round(core, 1))
            continue
        pred_info = prediction_by_discipline.get(disc) or {}
        shared_dist = int(disc_distribute.get(disc, 0))
        shared_pred = int(pred_info.get('points', 0))
        row['shared_distribute_points'] = shared_dist
        row['shared_prediction_points'] = shared_pred
        row['total_score'] = max(0, round(core + shared_dist + shared_pred, 1))

    for role in {r.get('role') for r in scores}:
        role_rows = [r for r in scores if r.get('role') == role]
        role_rows.sort(key=lambda r: r.get('total_score', 0), reverse=True)
        for idx, row in enumerate(role_rows, start=1):
            row['rank_in_role'] = idx

    return scores, dict(disc_distribute)


def build_discipline_groups(scores: List[dict]) -> List[dict]:
    grouped = defaultdict(list)
    for row in scores:
        if row.get('role') == 'manager':
            continue
        disc = canonicalize_discipline(row.get('discipline') or 'نامشخص')
        if is_excluded_discipline(disc):
            continue
        row = dict(row)
        row['discipline'] = disc
        grouped[disc].append(row)

    result = []
    for name, members in grouped.items():
        members_sorted = sorted(
            members,
            key=lambda r: (ROLE_ORDER.get(r.get('role'), 9), -float(r.get('core_score') or 0)),
        )
        core_sum = round(sum(float(m.get('core_score') or 0) for m in members), 1)
        shared_dist = int(members[0].get('shared_distribute_points') or 0) if members else 0
        shared_pred = int(members[0].get('shared_prediction_points') or 0) if members else 0
        result.append({
            'discipline': name,
            'member_count': len(members),
            'core_score': core_sum,
            'shared_distribute_points': shared_dist,
            'shared_prediction_points': shared_pred,
            'total_score': max(0, round(core_sum + shared_dist + shared_pred, 1)),
            'members': members_sorted,
        })
    result.sort(key=lambda item: item['total_score'], reverse=True)
    for idx, item in enumerate(result, start=1):
        item['rank'] = idx
    return result


def _assign_role(code) -> Optional[str]:
    if not code:
        return None
    text = str(code).strip()
    mapped = _norm_role(text)
    if mapped:
        return mapped
    return {
        'EM': 'manager',
        'MDS': 'senior',
        'MDJ': 'specialist',
    }.get(text.upper())


def _score_lookup(score_map, *names):
    for name in names:
        if name and name in score_map:
            return score_map[name]
        folded = _fold_text(name)
        if not folded:
            continue
        for key, row in score_map.items():
            if _fold_text(key) == folded:
                return row
    return {}


def _score_card(name, role, discipline, score_map, persian=''):
    info = _score_lookup(score_map, name, persian)
    return {
        'person': name,
        'persian_name': persian or '',
        'role': role,
        'discipline': discipline,
        'total_score': info.get('total_score'),
        'core_score': info.get('core_score'),
        'shared_distribute_points': info.get('shared_distribute_points') or 0,
        'shared_prediction_points': info.get('shared_prediction_points') or 0,
        'has_score': info.get('total_score') is not None,
    }


def _person_matches(person: dict, aliases: List[str]) -> bool:
    names = [person.get('name'), person.get('persian_name'), *(person.get('aliases') or [])]
    wanted = {_fold_text(a).replace(' ', '') for a in aliases if a}
    for name in names:
        compact = _fold_text(name).replace(' ', '')
        if compact and compact in wanted:
            return True
    return False


def _find_person(people: List[dict], aliases: List[str]) -> Optional[dict]:
    for person in people or []:
        if _person_matches(person, aliases):
            return person
    return None


def _unique_cards(cards: List[dict]) -> List[dict]:
    seen = set()
    unique = []
    for item in cards:
        key = item.get('person')
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    unique.sort(key=lambda item: -(item.get('total_score') if item.get('total_score') is not None else -1))
    return unique


def _disc_payload(disc_name, bucket):
    for key in ('managers', 'seniors', 'specialists', 'others'):
        bucket[key] = _unique_cards(bucket[key])
    members = bucket['seniors'] + bucket['specialists'] + bucket['others']
    if not members and not bucket['managers']:
        return None
    scored = [m for m in members if m.get('has_score')]
    core_sum = round(sum(float(m.get('core_score') or 0) for m in scored), 1)
    shared_dist = int(scored[0].get('shared_distribute_points') or 0) if scored else 0
    shared_pred = int(scored[0].get('shared_prediction_points') or 0) if scored else 0
    return {
        'discipline': disc_name,
        'managers': [],
        'seniors': bucket['seniors'],
        'specialists': bucket['specialists'],
        'others': bucket['others'],
        'core_score': core_sum,
        'shared_distribute_points': shared_dist,
        'shared_prediction_points': shared_pred,
        'total_score': max(0, round(core_sum + shared_dist + shared_pred, 1)),
    }


def build_project_portfolios(people: List[dict], scores: List[dict], project_name: str = 'همه') -> dict:
    """
    هر پروژه یک مدیر مهندسی ثابت دارد؛ دیسیپلین Management نمایش داده نمی‌شود.
    Civil و Architecture تیم مشخص دارند. امتیاز پروژه = جمع امتیاز دیسیپلین‌های تولیدی.
    """
    score_map = {}
    for row in scores or []:
        person = row.get('person')
        if person:
            score_map[person] = row

    grouped = defaultdict(lambda: defaultdict(lambda: {
        'managers': [], 'seniors': [], 'specialists': [], 'others': [],
    }))
    projects_seen = set()

    for person in people or []:
        name = person.get('name')
        if not name:
            continue
        discipline = canonicalize_discipline(person.get('discipline'))
        persian = person.get('persian_name') or ''
        for assignment in person.get('assignments') or []:
            project = str(assignment.get('project') or '').strip()
            if not project:
                continue
            if project_name and project_name != 'همه' and project != project_name:
                continue
            projects_seen.add(project)
            role = _assign_role(assignment.get('role_code') or assignment.get('role_label') or person.get('org_role'))
            code = str(assignment.get('role_code') or '').upper()
            if code in ('DCC', 'CTR', 'EM') or role == 'manager':
                continue
            if role not in ('senior', 'specialist'):
                continue
            if is_excluded_discipline(discipline):
                continue
            card = _score_card(name, role, discipline, score_map, persian)
            bucket_name = 'seniors' if role == 'senior' else 'specialists'
            grouped[project][discipline][bucket_name].append(card)

    for project in list(projects_seen):
        for disc_name, team in DISCIPLINE_TEAMS.items():
            for role_key, aliases in team.items():
                bucket_name = 'seniors' if role_key == 'senior' else 'specialists'
                for alias in aliases:
                    match = _find_person(people, [alias] + aliases)
                    if not match:
                        continue
                    on_project = any(
                        str(a.get('project') or '').strip() == project
                        for a in (match.get('assignments') or [])
                    )
                    if not on_project and project_name not in (None, '', 'همه'):
                        continue
                    if not on_project:
                        continue
                    card = _score_card(
                        match.get('name'),
                        'senior' if role_key == 'senior' else 'specialist',
                        disc_name,
                        score_map,
                        match.get('persian_name') or '',
                    )
                    grouped[project][disc_name][bucket_name].append(card)

    result_projects = []
    for project in sorted(grouped.keys() | projects_seen):
        if project_name and project_name != 'همه' and project != project_name:
            continue
        disciplines = []
        for disc_name, bucket in grouped[project].items():
            if is_excluded_discipline(disc_name):
                continue
            payload = _disc_payload(disc_name, bucket)
            if payload:
                disciplines.append(payload)
        disciplines.sort(key=lambda item: item['total_score'], reverse=True)
        project_score = round(sum(float(d.get('total_score') or 0) for d in disciplines), 1)

        em_aliases = project_em_aliases(project)
        em_person = _find_person(people, em_aliases) if em_aliases else None
        display_name = (em_person or {}).get('name') or (em_aliases[0] if em_aliases else 'مدیر مهندسی ثبت نشده')
        em_card = _score_card(
            display_name,
            'manager',
            '',
            score_map,
            (em_person or {}).get('persian_name') or '',
        ) if em_aliases else {
            'person': 'مدیر مهندسی ثبت نشده',
            'total_score': None,
            'core_score': None,
            'has_score': False,
            'personal_score': None,
        }
        em_card['personal_score'] = em_card.get('total_score')
        em_card['total_score'] = project_score
        em_card['has_score'] = bool(disciplines)
        em_card['disciplines'] = disciplines

        result_projects.append({
            'project': project,
            'total_score': project_score,
            'managers': [em_card],
        })

    result_projects.sort(key=lambda item: -(item.get('total_score') or 0))
    return {
        'project': project_name,
        'projects': result_projects,
        'managers': result_projects[0]['managers'] if len(result_projects) == 1 else [],
    }


def build_portfolio_tree(scores: List[dict], project_name: str = 'همه') -> dict:
    """سازگاری قدیمی: اگر فقط امتیازها باشند، همان را گروه‌بندی می‌کند."""
    return build_project_portfolios([], scores, project_name=project_name)
