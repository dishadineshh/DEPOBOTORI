# fetch_ga_test.py
import os
from dotenv import load_dotenv

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account

load_dotenv()

# ----- Config -----
# Put your numeric GA4 property id in .env, e.g. GA_PROPERTY_ID=456260050
GA_PROPERTY_ID = (os.getenv("GA_PROPERTY_ID") or "").strip()
# Path to your JSON key (you already saved it under back/keys/...)
GA_KEY_PATH = (os.getenv("GA_KEY_PATH") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()

if not GA_PROPERTY_ID:
    raise RuntimeError("GA_PROPERTY_ID is not set in .env")
if not GA_KEY_PATH or not os.path.exists(GA_KEY_PATH):
    raise RuntimeError("GA_KEY_PATH (or GOOGLE_APPLICATION_CREDENTIALS) is missing/invalid")

# Accept both "456260050" and "properties/456260050"
if not GA_PROPERTY_ID.startswith("properties/"):
    PROPERTY = f"properties/{GA_PROPERTY_ID}"
else:
    PROPERTY = GA_PROPERTY_ID


def _val(x):
    """Safely read GA API Value objects across versions."""
    # DimensionValue / MetricValue typically expose .value
    return getattr(x, "value", getattr(x, "string_value", ""))


def run_report():
    creds = service_account.Credentials.from_service_account_file(GA_KEY_PATH)
    client = BetaAnalyticsDataClient(credentials=creds)

    request = RunReportRequest(
        property=PROPERTY,
        dimensions=[Dimension(name="date"), Dimension(name="country")],
        metrics=[Metric(name="activeUsers"), Metric(name="eventCount")],
        date_ranges=[DateRange(start_date="7daysAgo", end_date="today")],
        limit=10,
    )

    print(f"\nRunning GA4 report for {PROPERTY} ...")
    response = client.run_report(request)

    # Print header
    dim_headers = [d.name for d in response.dimension_headers]
    met_headers = [m.name for m in response.metric_headers]
    print("Dimensions:", dim_headers)
    print("Metrics   :", met_headers)
    print("Rows:")
    for row in response.rows:
        dims = [_val(v) for v in row.dimension_values]
        mets = [_val(v) for v in row.metric_values]
        print("  ", dims, mets)

    print("\nOK: Report fetched successfully.")


if __name__ == "__main__":
    try:
        run_report()
    except Exception as e:
        # Friendlier error hints
        msg = str(e)
        if "PERMISSION_DENIED" in msg or "PermissionDenied" in msg or "403" in msg:
            print("\nERROR: Permission denied.")
            print("• Make sure the *service account email* from your JSON key is added in GA4 → Admin → "
                  "Property Access Management → + Add users → paste the service account email → Role: Viewer/Analyst.")
            print("• Confirm the GA_PROPERTY_ID matches the GA4 property (the 'p' number in the GA URL).")
        else:
            print("\nERROR:", e)
        raise
