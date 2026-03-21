import os
import psycopg2
import pandas as pd
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
client = OpenAI(api_key=os.getenv("GEMINI_API_KEY"), base_url="https://generativelanguage.googleapis.com/v1beta/openai/")

conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"), port=os.getenv("DB_PORT", "5432"),
    database=os.getenv("DB_NAME", "postgres"), user=os.getenv("DB_USER", "postgres"), password=os.getenv("DB_PASSWORD", "")
)

# Only get anomalies from the LAST HOUR
query = "SELECT alert_id, timestampx, region, deviation, severity FROM fct_alerts WHERE created_at >= NOW() - INTERVAL '1 HOUR' ORDER BY alert_id DESC LIMIT 1"
df = pd.read_sql(query, conn)
conn.close()

if df.empty:
    print("No new hourly alerts. Exiting smoothly without sending email.")
    raise SystemExit

row = df.iloc[0]
prompt = f"Write a highly professional, 2 sentence urgent alert email for grid operator. Anomaly: {round(float(row['deviation'])*100, 2)}% deviation at {row['timestampx']}. Severity: {row['severity']}."

# Send Email Logic
try:
    response = client.chat.completions.create(
        model="gemini-1.5-flash", 
        messages=[{"role": "system", "content": "You are a grid alert auto-responder."}, {"role": "user", "content": prompt}]
    )
    email_body = response.choices[0].message.content
except Exception as e:
    email_body = f"[AI Error]\n\nURGENT: Alert #{row['alert_id']} triggered for region {row['region']} at {row['timestampx']} with {round(float(row['deviation']) * 100, 2)}% deviation."

SMTP_SERVER = os.getenv("SMTP_SERVER", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", 1025))
TARGET_EMAIL = "email@ocalkaptan.de"

msg = EmailMessage()
msg.set_content(email_body)
msg["Subject"] = f"[{row['severity'].upper()}] HOURLY Grid Alert #{row['alert_id']}"
msg["From"] = "alerts@gridmonitor.local"
msg["To"] = TARGET_EMAIL

try:
    if SMTP_SERVER == "localhost" or not SMTP_SERVER:
        print(f"Simulated HOURLY Alert email dispatch to {TARGET_EMAIL}")
    else:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        if os.getenv("SMTP_USER") and os.getenv("SMTP_PASSWORD"):
            server.starttls()
            server.login(os.getenv("SMTP_USER"), os.getenv("SMTP_PASSWORD"))
        server.send_message(msg)
        server.quit()
        print(f"Successfully sent HOURLY Alert to {TARGET_EMAIL}")
except Exception as e:
    print(f"Could not send email. Error: {e}")
