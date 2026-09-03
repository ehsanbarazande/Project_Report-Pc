"""
محاسبات مالی پروژه (مبلغ قرارداد/الحاقیه/صورت‌وضعیت/زمان باقی‌مانده).
تماماً قطعی و بدون مدل زبانی — Qwen فقط نتیجه‌ی این محاسبات را روایت
می‌کند، هیچ عددی اینجا حدس زده نمی‌شود.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional


def _parse_date(value):
    if not value:
        return None
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def compute_summary(project_record: dict, payments: list, today=None) -> dict:
    """
    خلاصه‌ی مالی یک پروژه رو از روی رکورد پروژه (قرارداد+الحاقیه‌ها) و
    لیست صورت‌وضعیت‌هاش می‌سازه.
    """
    today = today or datetime.now().date()
    project_record = project_record or {}
    payments = payments or []

    start_date = _parse_date(project_record.get("start_date"))
    months_elapsed = round((today - start_date).days / 30.44, 1) if start_date else None

    amendments = project_record.get("amendments") or []
    total_amendment_amount = sum(float(a.get("amount_added") or 0) for a in amendments)
    original_amount = float(project_record.get("original_amount") or 0)
    total_contract_amount = original_amount + total_amendment_amount

    total_invoiced = sum(float(p.get("amount") or 0) for p in payments)
    remaining_balance = total_contract_amount - total_invoiced
    percent_invoiced = round((total_invoiced / total_contract_amount) * 100, 1) if total_contract_amount else None

    sorted_payments = sorted(payments, key=lambda p: p.get("payment_date") or "")
    latest_payment = sorted_payments[-1] if sorted_payments else None

    latest_expiry = None
    for a in amendments:
        d = _parse_date(a.get("new_expiry_date"))
        if d and (latest_expiry is None or d > latest_expiry):
            latest_expiry = d
    if latest_expiry is None and start_date and project_record.get("duration_months"):
        try:
            duration_days = round(float(project_record["duration_months"]) * 30.44)
            latest_expiry = start_date.fromordinal(start_date.toordinal() + duration_days)
        except Exception:
            latest_expiry = None

    days_to_expiry = (latest_expiry - today).days if latest_expiry else None

    by_subcontractor = {}
    for p in payments:
        key = p.get("subcontractor_name") or "قرارداد اصلی"
        by_subcontractor[key] = by_subcontractor.get(key, 0) + float(p.get("amount") or 0)

    return {
        "project_name": project_record.get("project_name"),
        "employer": project_record.get("employer"),
        "contract_name": project_record.get("contract_name"),
        "start_date": project_record.get("start_date"),
        "months_elapsed": months_elapsed,
        "duration_months": project_record.get("duration_months"),
        "original_amount": original_amount,
        "total_amendment_amount": total_amendment_amount,
        "amendments_count": len(amendments),
        "total_contract_amount": total_contract_amount,
        "total_invoiced": round(total_invoiced, 2),
        "remaining_balance": round(remaining_balance, 2),
        "percent_invoiced": percent_invoiced,
        "payments_count": len(payments),
        "latest_payment": latest_payment,
        "latest_amendment_expiry": latest_expiry.isoformat() if latest_expiry else None,
        "days_to_amendment_expiry": days_to_expiry,
        "by_subcontractor": by_subcontractor,
        "subcontractors": project_record.get("subcontractors") or [],
    }


def render_summary_text(summary: dict) -> str:
    """روایت قالب‌محور (بدون مدل) — همیشه در دسترس، برای فallback یا نمایش مستقیم."""
    if not summary or not summary.get("project_name"):
        return "اطلاعات مالی ثبت‌شده‌ای برای این پروژه پیدا نشد."

    parts = [f"وضعیت مالی {summary['project_name']}:"]
    if summary.get("employer"):
        parts.append(f"کارفرما: {summary['employer']}.")
    if summary.get("months_elapsed") is not None:
        parts.append(f"{summary['months_elapsed']} ماه از شروع پروژه گذشته.")

    parts.append(
        f"مبلغ کل قرارداد (با احتساب {summary.get('amendments_count', 0)} الحاقیه) "
        f"{summary['total_contract_amount']:,.0f} است."
    )
    parts.append(
        f"تا الان {summary['total_invoiced']:,.0f} صورت‌وضعیت شده "
        f"({summary.get('percent_invoiced') or 0}٪) و {summary['remaining_balance']:,.0f} باقی مانده."
    )

    lp = summary.get("latest_payment")
    if lp:
        who = lp.get("subcontractor_name") or "قرارداد اصلی"
        parts.append(f"آخرین صورت‌وضعیت: {lp.get('payment_date')} به مبلغ {float(lp.get('amount') or 0):,.0f} ({who}).")

    d = summary.get("days_to_amendment_expiry")
    if d is not None:
        if d >= 0:
            parts.append(f"{d} روز تا انقضای آخرین الحاقیه/مدت قرارداد مانده.")
        else:
            parts.append(f"مدت قرارداد/آخرین الحاقیه {abs(d)} روز پیش منقضی شده.")

    return " ".join(parts)
