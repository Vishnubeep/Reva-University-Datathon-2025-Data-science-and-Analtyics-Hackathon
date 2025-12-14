import pandas as pd
import numpy as np
from datetime import datetime

# ---------------------------
# CONFIG (fixed per problem)
# ---------------------------
CURRENT_WINDOW_START = pd.Timestamp("2025-11-23 00:00:00")
CURRENT_WINDOW_END   = pd.Timestamp("2025-11-30 23:59:59")
PREV_WINDOW_START    = pd.Timestamp("2025-11-16 00:00:00")
PREV_WINDOW_END      = pd.Timestamp("2025-11-22 23:59:59")

OUTPUT_CSV = "Final_Risk_Report.csv"

# ---------------------------
# HELPERS
# ---------------------------
def pick_column(cols, keywords):
    """
    Return the actual column name in cols that matches any keyword in keywords (case-insensitive).
    If none found, return None.
    """
    lower_map = {c.lower().strip(): c for c in cols}
    for kw in keywords:
        for low, orig in lower_map.items():
            if kw.lower() in low:
                return orig
    return None

def find_col_by_keywords(cols, keywords):
    for c in cols:
        low = c.lower().strip()
        for kw in keywords:
            if kw in low:
                return c
    return None

# ---------------------------
# 1) LOAD CSVs (expect files in same folder)
# ---------------------------
print("Loading CSV files...")
profiles = pd.read_csv("USER_PROFILES.csv")
usage    = pd.read_csv("USAGE_LOGS.csv")
tickets  = pd.read_csv("SUPPORT_TICKETS.csv")
billing  = pd.read_csv("BILLING_STATUS.csv")
print("CSV load complete.")

# ---------------------------
# 2) NORMALIZE/RENAME IMPORTANT COLUMNS (defensive)
# ---------------------------
# Profiles
uid_col = pick_column(profiles.columns, ["user_id","userid","id"])
if uid_col is None:
    raise SystemExit("USER_PROFILES.csv must contain a user id column.")
profiles = profiles.rename(columns={uid_col: "User_ID"})

monthly_col = pick_column(profiles.columns, ["monthly","monthly_spend","monthly_charge","price","amount","fee"])
if monthly_col:
    profiles = profiles.rename(columns={monthly_col: "Monthly_Charge"})
else:
    profiles["Monthly_Charge"] = 0.0

sub_col = pick_column(profiles.columns, ["subscription","tier","plan","account_type"])
if sub_col:
    profiles = profiles.rename(columns={sub_col: "Subscription_Type"})
else:
    profiles["Subscription_Type"] = "unknown"

pay_col = pick_column(profiles.columns, ["is_paying","paying","ispaid","customer_type"])
if pay_col:
    profiles = profiles.rename(columns={pay_col: "Paying_Flag"})
else:
    profiles["Paying_Flag"] = True

# Usage
usage_uid = pick_column(usage.columns, ["user_id","userid"])
usage_time = pick_column(usage.columns, ["timestamp","time","log_timestamp","event_time","datetime"])
usage_dur = pick_column(usage.columns, ["duration","session_duration","session_duration_min","minutes","length"])

if usage_uid is None or usage_time is None:
    raise SystemExit("USAGE_LOGS.csv must contain a user id and a timestamp-like column.")

usage = usage.rename(columns={usage_uid: "User_ID", usage_time: "Log_Timestamp"})
if usage_dur:
    usage = usage.rename(columns={usage_dur: "Session_Duration_Min"})
else:
    usage["Session_Duration_Min"] = 1.0

usage["Log_Timestamp"] = pd.to_datetime(usage["Log_Timestamp"], errors="coerce")

# Tickets
tik_uid = pick_column(tickets.columns, ["user_id","userid"])
tik_time = pick_column(tickets.columns, ["date_opened","opened","timestamp","created","date"])
tik_status = pick_column(tickets.columns, ["status","ticket_status"])
tik_sev = pick_column(tickets.columns, ["severity","priority","sev","priority_level"])

if tik_uid is None or tik_time is None:
    raise SystemExit("SUPPORT_TICKETS.csv must contain a user id and a date-opened-like column.")

tickets = tickets.rename(columns={tik_uid: "User_ID", tik_time: "Date_Opened"})
if tik_status:
    tickets = tickets.rename(columns={tik_status: "Status"})
else:
    tickets["Status"] = "Unknown"
if tik_sev:
    tickets = tickets.rename(columns={tik_sev: "Severity"})
else:
    tickets["Severity"] = "Low"

tickets["Date_Opened"] = pd.to_datetime(tickets["Date_Opened"], errors="coerce")

# Billing
bill_uid = pick_column(billing.columns, ["user_id","userid"])
if bill_uid is None:
    raise SystemExit("BILLING_STATUS.csv must contain a user id column.")
billing = billing.rename(columns={bill_uid: "User_ID"})

# detection for cancellation / payment issues / last login / monthly charge inside billing
cancel_col = find_col_by_keywords(billing.columns, ["cancel","cancellation","cancel_request","churn"])
if cancel_col:
    billing = billing.rename(columns={cancel_col: "Cancellation_Requested"})
    billing["Cancellation_Requested"] = billing["Cancellation_Requested"].astype(str).str.lower().isin(["true","1","yes","y","requested"])
else:
    billing["Cancellation_Requested"] = False

payment_col = find_col_by_keywords(billing.columns, ["payment_issue","payment_flag","payment_problem","payment", "issue", "flag"])
if payment_col:
    billing = billing.rename(columns={payment_col: "Payment_Issue_Flag"})
    billing["Payment_Issue"] = billing["Payment_Issue_Flag"].astype(str).str.lower().isin(["true","1","yes","y","issue","problem"])
else:
    billing["Payment_Issue"] = False

last_login_col = find_col_by_keywords(billing.columns, ["last_login","last_login_date","last_activity"])
if last_login_col:
    billing = billing.rename(columns={last_login_col: "Last_Login_Date"})
    billing["Last_Login_Date"] = pd.to_datetime(billing["Last_Login_Date"], errors="coerce")
    billing["days_since_last_login"] = (pd.Timestamp(CURRENT_WINDOW_END) - billing["Last_Login_Date"]).dt.days.fillna(9999).astype(int)
else:
    billing["days_since_last_login"] = 9999

bill_month_col = find_col_by_keywords(billing.columns, ["monthly","monthly_charge","amount","price","fee","spend","charge"])
if bill_month_col:
    billing = billing.rename(columns={bill_month_col: "Monthly_Charge"})
    billing["Monthly_Charge"] = pd.to_numeric(billing["Monthly_Charge"].astype(str).str.replace(r'[^\d\.-]', '', regex=True), errors="coerce").fillna(0.0)
else:
    if "Monthly_Charge" not in billing.columns:
        billing["Monthly_Charge"] = 0.0

print("Billing columns detected:", list(billing.columns))

# If profiles monthly charge is missing/zero, later we will fallback to billing["Monthly_Charge"]

# ---------------------------
# 3) MASTER VIEW (left join from profiles)
# ---------------------------
profiles = profiles.copy()
master = profiles[["User_ID", "Subscription_Type", "Monthly_Charge", "Paying_Flag"]].drop_duplicates(subset=["User_ID"]).set_index("User_ID")

# ---------------------------
# 4) USAGE FEATURES (current vs previous window)
# ---------------------------
usage_curr = usage[(usage["Log_Timestamp"] >= CURRENT_WINDOW_START) & (usage["Log_Timestamp"] <= CURRENT_WINDOW_END)]
usage_prev = usage[(usage["Log_Timestamp"] >= PREV_WINDOW_START) & (usage["Log_Timestamp"] <= PREV_WINDOW_END)]

def usage_agg(df):
    if df.empty:
        return pd.DataFrame(columns=["User_ID","total_minutes","session_count","active_days"]).set_index("User_ID")
    g = df.groupby("User_ID").agg(
        total_minutes = ("Session_Duration_Min", "sum"),
        session_count  = ("Session_Duration_Min", "count"),
        active_days    = ("Log_Timestamp", lambda x: x.dt.date.nunique() if pd.api.types.is_datetime64_any_dtype(x) else 0)
    )
    return g

usage_curr_agg = usage_agg(usage_curr)
usage_prev_agg = usage_agg(usage_prev)

# join and fill zeros
master = master.join(usage_curr_agg, how="left").join(usage_prev_agg, how="left", rsuffix="_prev")

for col in ["total_minutes","session_count","active_days","total_minutes_prev","session_count_prev","active_days_prev"]:
    if col not in master.columns:
        master[col] = 0
master[["total_minutes","session_count","active_days","total_minutes_prev","session_count_prev","active_days_prev"]] = \
    master[["total_minutes","session_count","active_days","total_minutes_prev","session_count_prev","active_days_prev"]].fillna(0)

# ---------------------------
# 5) TICKET FEATURES (safe copy to avoid SettingWithCopyWarning)
# ---------------------------
tik_recent = tickets[(tickets["Date_Opened"] >= CURRENT_WINDOW_START) & (tickets["Date_Opened"] <= CURRENT_WINDOW_END)].copy()

if "Status" in tik_recent.columns:
    tik_recent.loc[:, "Unresolved"] = ~tik_recent["Status"].astype(str).str.lower().isin(["resolved","closed","done","fixed"])
else:
    tik_recent.loc[:, "Unresolved"] = True

if "Severity" in tik_recent.columns:
    tik_recent.loc[:, "High_Severity"] = tik_recent["Severity"].astype(str).str.lower().isin(["high","critical","sev 1","p1"])
else:
    tik_recent.loc[:, "High_Severity"] = False

ticket_agg = tik_recent.groupby("User_ID").agg(
    unresolved_count = ("Unresolved", "sum"),
    total_tickets = ("User_ID", "count"),
    high_sev_count = ("High_Severity", "sum")
)

master = master.join(ticket_agg, how="left")
for c in ["unresolved_count","total_tickets","high_sev_count"]:
    if c not in master.columns:
        master[c] = 0
master[["unresolved_count","total_tickets","high_sev_count"]] = master[["unresolved_count","total_tickets","high_sev_count"]].fillna(0)

# ---------------------------
# 6) BILLING FEATURES (safe aggregation)
# ---------------------------
billing_agg = billing.groupby("User_ID").agg(
    cancellation_requested_any = ("Cancellation_Requested", "max"),
    payment_issues_count = ("Payment_Issue", "sum"),
    avg_days_since_login = ("days_since_last_login", "mean"),
    monthly_charge_billing = ("Monthly_Charge", "mean")
).reset_index().set_index("User_ID")

master = master.join(billing_agg, how="left")

# fill defaults
master["cancellation_requested_any"] = master["cancellation_requested_any"].fillna(False).astype(bool)
master["payment_issues_count"] = master["payment_issues_count"].fillna(0).astype(int)
master["avg_days_since_login"] = master["avg_days_since_login"].fillna(9999).astype(int)
master["monthly_charge_billing"] = master["monthly_charge_billing"].fillna(0.0)

# pick final Monthly_Charge (profiles preferred)
master["Monthly_Charge"] = master["Monthly_Charge"].fillna(0.0)
master["Monthly_Charge"] = master["Monthly_Charge"].replace("", 0.0).fillna(0.0)
try:
    master["Monthly_Charge"] = master["Monthly_Charge"].astype(float)
except Exception:
    # if parse fails, coerce with numeric conversion
    master["Monthly_Charge"] = pd.to_numeric(master["Monthly_Charge"].astype(str).str.replace(r'[^\d\.-]', '', regex=True), errors="coerce").fillna(0.0)

master["Monthly_Charge"] = np.where(master["Monthly_Charge"] <= 0, master["monthly_charge_billing"], master["Monthly_Charge"])

# ---------------------------
# 7) ACTIVITY DROP METRIC
# ---------------------------
prev_activity = master["total_minutes_prev"] + 0.2 * master["session_count_prev"]
curr_activity = master["total_minutes"] + 0.2 * master["session_count"]

rel_drop = (prev_activity - curr_activity) / np.maximum(prev_activity, 1.0)
rel_drop = rel_drop.clip(lower=0.0)
master["activity_relative_drop"] = rel_drop
master["zero_current_activity"] = (curr_activity <= 0).astype(int)

# ---------------------------
# 8) RISK SCORE (explainable formula)
# ---------------------------
w = {
    "activity": 0.40,
    "unresolved": 0.20,
    "high_sev": 0.15,
    "payment_issues": 0.10,
    "cancellation": 0.90,
    "inactive_days": 0.05
}

comp_activity = master["activity_relative_drop"].clip(0,1)
comp_unresolved = (master["unresolved_count"].clip(0,5) / 5.0).fillna(0)
comp_highsev = (master["high_sev_count"] > 0).astype(int)
comp_pay_issues = (master["payment_issues_count"].clip(0,3) / 3.0).fillna(0)
comp_cancellation = master["cancellation_requested_any"].astype(int)
comp_inactive = (master["avg_days_since_login"].clip(0,365) - 7) / (90 - 7)
comp_inactive = comp_inactive.clip(0,1).fillna(1.0)

master["score_raw"] = (
    comp_activity * w["activity"] +
    comp_unresolved * w["unresolved"] +
    comp_highsev * w["high_sev"] +
    comp_pay_issues * w["payment_issues"] +
    comp_cancellation * w["cancellation"] +
    comp_inactive * w["inactive_days"]
)

minv = master["score_raw"].min()
maxv = master["score_raw"].max()
if pd.isna(minv) or pd.isna(maxv) or maxv == minv:
    master["risk_score"] = 0.0
else:
    master["risk_score"] = ((master["score_raw"] - minv) / (maxv - minv)).clip(0,1)

# ---------------------------
# 9) PRIMARY REASON
# ---------------------------
contribs = pd.DataFrame({
    "activity": comp_activity * w["activity"],
    "unresolved": comp_unresolved * w["unresolved"],
    "high_sev": comp_highsev * w["high_sev"],
    "payment_issues": comp_pay_issues * w["payment_issues"],
    "cancellation": comp_cancellation * w["cancellation"],
    "inactive_days": comp_inactive * w["inactive_days"]
}, index=master.index)

master["Primary_Reason"] = contribs.idxmax(axis=1)
reason_map = {
    "activity": "Activity drop (7d vs prev)",
    "unresolved": "Unresolved recent support tickets",
    "high_sev": "High-severity open ticket",
    "payment_issues": "Recent payment issues",
    "cancellation": "Cancellation requested",
    "inactive_days": "Long time since last login"
}
master["Primary_Reason"] = master["Primary_Reason"].map(reason_map).fillna("Other")

# ---------------------------
# 10) RISK TIERS
# ---------------------------
def tier_from_score(s):
    if s >= 0.95:
        return "Critical"
    if s >= 0.75:
        return "High"
    if s >= 0.40:
        return "Medium"
    return "Low"

master = master.reset_index()
master = master.sort_values(by=["risk_score","Monthly_Charge"], ascending=[False, False])
master["Risk_Tier"] = master["risk_score"].apply(tier_from_score)

# ---------------------------
# 11) FINAL OUTPUT & SAVE
# ---------------------------
final = master[[
    "User_ID",
    "risk_score",
    "Risk_Tier",
    "Primary_Reason",
    "Monthly_Charge",
    "activity_relative_drop",
    "unresolved_count",
    "high_sev_count",
    "payment_issues_count",
    "cancellation_requested_any",
    "avg_days_since_login"
]].copy()

final = final.rename(columns={"risk_score": "Risk_Score"})
final["Risk_Score"] = final["Risk_Score"].round(4)

final.to_csv(OUTPUT_CSV, index=False)
final.head(100).to_csv("Top100_HighRisk.csv", index=False)

print(f"WROTE {OUTPUT_CSV} and Top100_HighRisk.csv")
print("Top 20 high-risk users:")
print(final.head(20).to_string(index=False))
