
from __future__ import annotations

import csv
import io
import re
from datetime import datetime
from typing import Any, Dict, List

from flask import Blueprint, Response, flash, render_template, request
from auth_guard import login_required
from db import get_db_connection # ✅ use your db.py function

bp = Blueprint("fee_reports", __name__, url_prefix="/reports")
fee_reports = bp

def _safe_filename(text: str) -> str:
    text = (text or "").strip()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^A-Za-z0-9._-]", "", text)
    return text or "file"


def _is_valid_date_yyyy_mm_dd(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except Exception:
        return False


def _fetch_classes(conn) -> List[str]:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT DISTINCT class_name
            FROM students
            WHERE COALESCE(archived,0)=0
              AND class_name IS NOT NULL
              AND class_name <> ''
            ORDER BY class_name
            """
        )
        return [r["class_name"] for r in cur.fetchall()]
    finally:
        cur.close()


def _fetch_payment_report(conn, class_name: str, date_from: str, date_to: str) -> List[Dict[str, Any]]:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT
              s.student_number,
              CONCAT(
                TRIM(s.first_name),' ',
                TRIM(COALESCE(s.middle_name,'')),' ',
                TRIM(s.last_name)
              ) AS student_name,
              s.class_name,
              s.section,

              f.year,
              f.term,
              f.payment_type,
              f.payment_item,
              f.requirement_name,

              f.amount_paid,
              f.bursary_amount,
              f.expected_amount,

              f.date_paid,
              f.receipt_no,
              f.method,
              f.recorded_by,
              f.comment
            FROM fees f
            JOIN students s ON s.id = f.student_id
            WHERE COALESCE(s.archived,0)=0
              AND s.class_name = %s
              AND f.date_paid IS NOT NULL
              AND f.date_paid >= %s
              AND f.date_paid < DATE_ADD(%s, INTERVAL 1 DAY)
              AND (f.comment IS NULL OR LOWER(f.comment) NOT LIKE '%%void%%')
            ORDER BY s.student_number, f.date_paid, f.id
            """,
            (class_name, date_from, date_to),
        )
        return cur.fetchall()
    finally:
        cur.close()


@bp.route("/payments", methods=["GET"])
@login_required
def payments_report():
    class_name = (request.args.get("class_name") or "").strip()
    date_from = (request.args.get("date_from") or "").strip()
    date_to = (request.args.get("date_to") or "").strip()

    conn = None
    classes: List[str] = []
    rows: List[Dict[str, Any]] = []

    try:
        conn = get_db_connection()
        classes = _fetch_classes(conn)

        if class_name or date_from or date_to:
            # validate
            if not (class_name and date_from and date_to):
                flash("Please select Class, From date, and To date.", "warning")
            elif not (_is_valid_date_yyyy_mm_dd(date_from) and _is_valid_date_yyyy_mm_dd(date_to)):
                flash("Dates must be in YYYY-MM-DD format.", "warning")
            else:
                rows = _fetch_payment_report(conn, class_name, date_from, date_to)

    except Exception as e:
        flash(f"Report error: {e}", "danger")

    finally:
        if conn:
            conn.close()

    return render_template(
        "reports/payments_report.html",
        classes=classes,
        class_name=class_name,
        date_from=date_from,
        date_to=date_to,
        rows=rows,
    )


@bp.route("/payments.csv", methods=["GET"])
@login_required
def payments_report_csv():
    class_name = (request.args.get("class_name") or "").strip()
    date_from = (request.args.get("date_from") or "").strip()
    date_to = (request.args.get("date_to") or "").strip()

    if not (class_name and date_from and date_to):
        return Response("Missing filters: class_name, date_from, date_to", status=400)

    if not (_is_valid_date_yyyy_mm_dd(date_from) and _is_valid_date_yyyy_mm_dd(date_to)):
        return Response("Dates must be in YYYY-MM-DD format", status=400)

    conn = None
    try:
        conn = get_db_connection()
        rows = _fetch_payment_report(conn, class_name, date_from, date_to)

        output = io.StringIO()
        writer = csv.writer(output)

        headers = [
            "student_number", "student_name", "class_name", "section",
            "year", "term", "payment_type", "payment_item", "requirement_name",
            "amount_paid", "bursary_amount", "expected_amount",
            "date_paid", "receipt_no", "method", "recorded_by", "comment",
        ]
        writer.writerow(headers)

        for r in rows:
            writer.writerow([r.get(h, "") for h in headers])

        csv_data = output.getvalue()

        filename = f"payments_{_safe_filename(class_name)}_{date_from}_to_{date_to}.csv"
        return Response(
            csv_data,
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    except Exception as e:
        return Response(f"CSV generation failed: {e}", status=500)

    finally:
        if conn:
            conn.close()
