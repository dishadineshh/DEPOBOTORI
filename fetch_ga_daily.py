# fetch_ga_daily.py
import os
import csv
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from google.analytics.data_v1beta import BetaAnalyticsDataClient, DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account

BASE = Path(__file__).parent
ENV = BASE / ".env"
load_dotenv(ENV)

GA_PROPERTY_ID = os.getenv("GA_PROPERTY_ID")  # e.g. 456260050 (GA4 property)
if not GA_PROPERTY_ID:
    raise RuntimeError("GA_PROPERTY_ID missing in .env")

CREDS_JSON = os.getenv("GA_KEYS_JSON") or str(BASE / "keys" / "service_account.json")
if not Path(CREDS_JSON).exists():
    raise RuntimeError(f"GA service account key not found at {CREDS_JSON}. Put your JSON in back/keys and set GA_KEYS_JSON in .env")

# Output CSV the server reads
OUT = BASE / "data" / "ga_metrics.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]

def run():
    creds = service_account.Credentials.from_service_account_file(CREDS_JSON, scopes=SCOPES)
    client = BetaAnalyticsDataClient(credentials=creds)

    # Pull last 30 days, with the exact columns our server fallback expects
    request = RunReportRequest(
        property=f"properties/{GA_PROPERTY_ID}",
        date_ranges=[DateRange(start_date="30daysAgo", end_date="today")],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="country"),
            Dimension(name="pagePath"),  # can be empty for some rows
        ],
        metrics=[
            Metric(name="activeUsers"),
            Metric(name="eventCount"),
        ],
        limit=100000,
    )
    resp = client.run_report(request)

    # Normalize rows to: date, country, page, activeUsers, eventCount
    rows_out = []
    for r in resp.rows:
        # GA v1beta returns .value for both dimension and metric values
        dvals = [d.value for d in r.dimension_values]
        mvals = [m.value for m in r.metric_values]

        dt_raw   = (dvals[0] or "").strip() if len(dvals) > 0 else ""
        country  = (dvals[1] or "").strip() if len(dvals) > 1 else ""
        page_raw = (dvals[2] or "").strip() if len(dvals) > 2 else ""

        # date is YYYYMMDD → keep as is; server parses it
        users  = (mvals[0] or "0").strip() if len(mvals) > 0 else "0"
        events = (mvals[1] or "0").strip() if len(mvals) > 1 else "0"

        rows_out.append({
            "date": dt_raw,
            "country": country,
            "page": page_raw,
            "activeUsers": users,
            "eventCount": events,
        })

    # Write CSV
    with OUT.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["date","country","page","activeUsers","eventCount"])
        w.writeheader()
        for row in rows_out:
            w.writerow(row)

    print(f"[ga] wrote {OUT} rows={len(rows_out)}")

if __name__ == "__main__":
    run()
