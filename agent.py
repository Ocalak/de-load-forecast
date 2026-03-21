import os
import psycopg2
import pandas as pd
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

client = OpenAI(
    api_key=os.getenv("GEMINI_API_KEY"),
    base_url="https://generativelanguage.googleapis.com/v1beta/openai/"
)

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD")
)

# 1. Fetch last 4 hours of actual demand
query_actuals = """
SELECT timestamp, demand_mw 
FROM hourly_demand_data 
ORDER BY timestamp DESC 
LIMIT 4
"""
df_actuals = pd.read_sql(query_actuals, conn)
df_actuals = df_actuals.sort_values("timestamp")
actuals_text = "\n".join([f"- {row['timestamp']}: {round(row['demand_mw'], 1)} MW" for _, row in df_actuals.iterrows()])

if df_actuals.empty:
    actuals_text = "No recent actual demand data available."

# 2. Fetch next 24 hours of predicted demand
query_forecasts = """
SELECT forecast_target_time, predicted_demand 
FROM fct_demand_forecast 
WHERE forecast_target_time >= NOW()
ORDER BY forecast_target_time ASC 
LIMIT 24
"""
df_forecasts = pd.read_sql(query_forecasts, conn)
forecasts_text = "\n".join([f"- {row['forecast_target_time']}: {round(row['predicted_demand'], 1)} MW" for _, row in df_forecasts.iterrows()])

if df_forecasts.empty:
    forecasts_text = "No upcoming 24-hour predictions available."

# 3. Check for latest anomaly alert (from last 24h)
query_alerts = """
SELECT alert_id, timestampx, region, deviation, severity
FROM fct_alerts
WHERE created_at >= NOW() - INTERVAL '24 HOURS'
ORDER BY alert_id DESC
LIMIT 1
"""
df_alerts = pd.read_sql(query_alerts, conn)
conn.close()

alert_text = "No critical anomalies detected in the last 24 hours. Grid is stable."
if not df_alerts.empty:
    row = df_alerts.iloc[0]
    alert_text = (f"ALERT #{row['alert_id']} DETECTED:\n"
                  f"Region: {row['region']} | Time: {row['timestampx']}\n"
                  f"Deviation: {round(float(row['deviation']) * 100, 2)}% | Severity: {row['severity']}")

# 4. Construct AI Prompt
prompt = f"""
You are an expert energy grid monitoring AI.

Please write a 'Daily Grid Operations & Forecast Report' to be emailed to the grid operators at 10:00 AM. 

Here is the telemetry data:

**1. Last 4 Hours of Actual Demand:**
{actuals_text}

**2. Next 24 Hours of Forecasted Demand:**
{forecasts_text}

**3. System Alert Status:**
{alert_text}

Requirements:
- Format it as a professional email.
- Briefly summarize the actuals (e.g. trends or peaks).
- Briefly summarize the forecast (e.g. expected load bounds).
- Mention the alert status emphatically.
- Do not invent missing facts.
- Keep it concise but highly readable.
"""

# 5. Generate AI Report
try:
    response = client.chat.completions.create(
        model="gemini-1.5-flash", 
        messages=[
            {"role": "system", "content": "You are a professional energy grid operational assistant."}, 
            {"role": "user", "content": prompt}
        ]
    )
    email_body = response.choices[0].message.content
except Exception as e:
    email_body = f"[AI Generation Failed: {e}]\n\n--- AUTO-GENERATED FALLBACK REPORT ---\n\nACTUALS:\n{actuals_text}\n\nFORECASTS (Next 24h):\n{forecasts_text}\n\nSTATUS:\n{alert_text}"

print("----- GENERATED EMAIL BODY -----")
print(email_body)
print("--------------------------------")

# 6. Send Email
SMTP_SERVER = os.getenv("SMTP_SERVER", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", 1025))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASSWORD", "")
TARGET_EMAIL = "email@ocalkaptan.de"
FROM_EMAIL = "alerts@gridmonitor.local"

msg = EmailMessage()
msg.set_content(email_body)
msg["Subject"] = "Daily Grid Operations & Forecast Report"
msg["From"] = FROM_EMAIL
msg["To"] = TARGET_EMAIL

try:
    if SMTP_SERVER == "localhost" or not SMTP_SERVER:
        print(f"Simulating email dispatch to {TARGET_EMAIL} (Configure SMTP in .env for real delivery)")
    else:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        if SMTP_USER and SMTP_PASS:
            server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)
        server.quit()
        print(f"Successfully sent Daily Report email to {TARGET_EMAIL}")
except Exception as e:
    print(f"Could not send email over SMTP. Error: {e}")
