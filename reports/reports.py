from flask import Blueprint, render_template, request, Response, flash
from auth_guard import login_required
import csv
import io
from db import get_db_connection

bp = Blueprint("fee_reports", __name__, url_prefix="/reports")
fee_reports = bp

def _fetch_classes(conn):
    cur = conn.cursor(dictionary=True)
    # If you have a classes table use it; else use distinct from students
    cur.execute("""
        SELECT DISTINCT class_name
        FROM students
        WHERE COALESCE(archived,0)=0 AND class_name IS NOT NULL AND class_name <> ''
        ORDER BY class_name
    """)
    return [r["class_name"] for r in cur.fetchall()]

def _fetch_payment_report(conn, class_name, date_from, date_to):
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT
          s.student_number,
          CONCAT(TRIM(s.first_name),' ',TRIM(COALESCE(s.middle_name,'')),' ',TRIM(s.last_name)) AS student_name,
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
    """, (class_name, date_from, date_to))
    return cur.fetchall()

@bp.route("/payments", methods=["GET"])
@login_required
def payments_report():
    conn = get_conn()
    classes = _fetch_classes(conn)

    class_name = request.args.get("class_name", "")
    date_from = request.args.get("date_from", "")
    date_to = request.args.get("date_to", "")

    rows = []
    if class_name and date_from and date_to:
        rows = _fetch_payment_report(conn, class_name, date_from, date_to)
    elif any([class_name, date_from, date_to]):
        flash("Please select Class, From date, and To date.", "warning")

    return render_template(
        "reports/payments_report.html",
        classes=classes,
        class_name=class_name,
        date_from=date_from,
        date_to=date_to,
        rows=rows
    )

@bp.route("/payments.csv", methods=["GET"])
@login_required
def payments_report_csv():
    conn = get_conn()
    class_name = request.args.get("class_name", "")
    date_from = request.args.get("date_from", "")
    date_to = request.args.get("date_to", "")

    if not (class_name and date_from and date_to):
        return Response("Missing filters: class_name, date_from, date_to", status=400)

    rows = _fetch_payment_report(conn, class_name, date_from, date_to)

    output = io.StringIO()
    writer = csv.writer(output)

    headers = [
        "student_number","student_name","class_name","section",
        "year","term","payment_type","payment_item","requirement_name",
        "amount_paid","bursary_amount","expected_amount",
        "date_paid","receipt_no","method","recorded_by","comment"
    ]
    writer.writerow(headers)

    for r in rows:
        writer.writerow([r.get(h, "") for h in headers])

    csv_data = output.getvalue()
    output.close()

    filename = f"payments_{class_name}_{date_from}_to_{date_to}.csv"
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )