import os
import numpy as np
import pandas as pd
import psycopg2
import holidays

from dotenv import load_dotenv
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error

# -------------------------------------------------
# 1. LOAD DATA
# -------------------------------------------------
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
)

query = """
SELECT *
FROM merged_demand_data
ORDER BY timestamp
"""
df = pd.read_sql(query, conn)
conn.close()

df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
df = df.sort_values("timestamp").set_index("timestamp")

# -------------------------------------------------
# 2. BASIC FEATURE HELPERS
# -------------------------------------------------
de_holidays = holidays.Germany()

def add_calendar_features_for_timestamp(ts):
    local_ts = ts.tz_convert("Europe/Berlin")
    return {
        "month": local_ts.month,
        "weekday": local_ts.weekday(),
        "hour": local_ts.hour,
        "holiday": int(local_ts.date() in de_holidays),
    }

def make_feature_row(history_df, forecast_time):
    """
    history_df: data available up to forecast origin
    forecast_time: timestamp to predict
    """
    row = add_calendar_features_for_timestamp(forecast_time)

    # demand lags relative to forecast origin
    # at forecast creation time, these are known
    for lag in [1, 2, 3, 6, 12, 24, 48, 72, 168]:
        lag_time = history_df.index.max() - pd.Timedelta(hours=lag - 1)
        if lag_time in history_df.index:
            row[f"demand_lag_{lag}"] = history_df.loc[lag_time, "demand_mw"]
        else:
            row[f"demand_lag_{lag}"] = np.nan

    return pd.DataFrame([row], index=[forecast_time])

def one_hot_encode(df_in):
    return pd.get_dummies(df_in, columns=["month", "weekday", "hour"], drop_first=False)

# -------------------------------------------------
# 3. BUILD TRAINING DATA FOR 24 HORIZONS
# -------------------------------------------------
def build_direct_training_sets(df, run_hour=10, horizons=24):
    """
    Build one training set per horizon.
    Uses each day at run_hour as forecast origin.
    """
    training_sets = {h: [] for h in range(1, horizons + 1)}

    local_index = df.index.tz_convert("Europe/Berlin")

    # candidate forecast origins: each day at 10:00 local time
    forecast_origins = df.index[local_index.hour == run_hour]

    for origin in forecast_origins:
        history = df.loc[:origin].copy()

        # must have enough history
        if len(history) < 200:
            continue

        for h in range(1, horizons + 1):
            target_time = origin + pd.Timedelta(hours=h)

            if target_time not in df.index:
                continue

            X_row = make_feature_row(history, target_time)
            y_val = df.loc[target_time, "demand_mw"]

            X_row["target"] = y_val
            training_sets[h].append(X_row)

    # combine per horizon
    final_sets = {}
    for h, rows in training_sets.items():
        if rows:
            ds = pd.concat(rows)
            ds = ds.dropna()
            final_sets[h] = ds

    return final_sets

training_sets = build_direct_training_sets(df, run_hour=10, horizons=24)

# -------------------------------------------------
# 4. TRAIN 24 MODELS
# -------------------------------------------------
models = {}
feature_columns_by_horizon = {}

for h, ds in training_sets.items():
    X = ds.drop(columns=["target"])
    y = ds["target"]

    X_enc = one_hot_encode(X)

    feature_columns_by_horizon[h] = X_enc.columns.tolist()

    model = XGBRegressor(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.05,
        min_child_weight=5,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=1.0,
        reg_lambda=1.0,
        objective="reg:squarederror",
        eval_metric="rmse",
        random_state=42,
        n_jobs=-1,
    )

    model.fit(X_enc, y)
    models[h] = model

print(f"Trained {len(models)} horizon models.")

# -------------------------------------------------
# 5. DAILY FORECAST FUNCTION
# -------------------------------------------------
def predict_next_24_hours(df, models, feature_columns_by_horizon, forecast_origin):
    """
    forecast_origin should be the daily run time, e.g. 10:00 Europe/Berlin converted to UTC.
    """
    history = df.loc[:forecast_origin].copy()

    predictions = []

    for h in range(1, 25):
        forecast_time = forecast_origin + pd.Timedelta(hours=h)

        X_future = make_feature_row(history, forecast_time)
        X_future = one_hot_encode(X_future)

        # align columns with training
        cols = feature_columns_by_horizon[h]
        X_future = X_future.reindex(columns=cols, fill_value=0)

        y_pred = models[h].predict(X_future)[0]

        predictions.append({
            "forecast_origin": forecast_origin,
            "forecast_time": forecast_time,
            "horizon": h,
            "predicted_demand_mw": y_pred
        })

    return pd.DataFrame(predictions)

# -------------------------------------------------
# 6. EXAMPLE: RUN TODAY AT 10:00 EUROPE/BERLIN
# -------------------------------------------------
today_10_local = pd.Timestamp.now(tz="Europe/Berlin").normalize() + pd.Timedelta(hours=10)
forecast_origin = today_10_local.tz_convert("UTC")

forecast_df = predict_next_24_hours(
    df=df,
    models=models,
    feature_columns_by_horizon=feature_columns_by_horizon,
    forecast_origin=forecast_origin
)

print(forecast_df)