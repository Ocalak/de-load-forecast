import os
import numpy as np
import pandas as pd
import psycopg2
import holidays
import matplotlib.pyplot as plt

from dotenv import load_dotenv

from sklearn.preprocessing import StandardScaler, OneHotEncoder, FunctionTransformer
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import ParameterGrid
from sklearn.metrics import mean_squared_error

from xgboost import XGBRegressor, DMatrix
from xgboost import cv as xgb_cv

import warnings
warnings.filterwarnings("ignore", category=UserWarning)

# =========================================================
# 1. LOAD DATA
# =========================================================
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
)

# Use weather table to drive the timestamps up to tomorrow
query = """
SELECT w.ts_utc as timestamp, d.demand_mw, w.avg_temperature_c as temp
FROM weather_temperature_hourly w
LEFT JOIN hourly_demand_data d ON w.ts_utc = d.timestamp
ORDER BY w.ts_utc
"""
df = pd.read_sql(query, conn)
conn.close()

# =========================================================
# 2. FILTER STUDY WINDOW
# =========================================================
STUDY_START_DATE = pd.Timestamp("2023-03-17 00:00", tz="UTC")

de_load = df.copy()
de_load["timestamp"] = pd.to_datetime(de_load["timestamp"], utc=True)
de_load = de_load.set_index("timestamp")
de_load.index.name = "time"

de_load = de_load.loc[de_load.index >= STUDY_START_DATE].copy()

# =========================================================
# 3. FEATURE ENGINEERING
# =========================================================
def add_time_features(df_in):
    df_out = df_in.copy()
    local_index = df_out.index.tz_convert("Europe/Berlin")
    df_out["month"] = local_index.month
    df_out["weekday"] = local_index.weekday
    df_out["hour"] = local_index.hour
    return df_out

def add_holiday_features(df_in):
    df_out = df_in.copy()
    de_holidays = holidays.Germany()
    local_dates = pd.Series(df_out.index.tz_convert("Europe/Berlin"), index=df_out.index)
    df_out["holiday"] = local_dates.apply(lambda d: d.date() in de_holidays).astype(int)
    return df_out

def add_lag_features(df_in, col="demand_mw", lag_hours=range(48, 73)):
    df_out = df_in.copy()
    for n_hours in lag_hours:
        df_out[f"{col}_lag_{n_hours}"] = df_out[col].shift(n_hours)
    return df_out

def add_all_features(df_in):
    df_out = df_in.copy()
    df_out = add_time_features(df_out)
    df_out = add_holiday_features(df_out)
    # 48 to 72 hour lags for demand so we can predict up to tomorrow safely
    df_out = add_lag_features(df_out, col="demand_mw", lag_hours=range(48, 73))
    # We also add past temperature lags 
    df_out = add_lag_features(df_out, col="temp", lag_hours=[48, 72, 96])
    return df_out

print("Engineering features...")
de_load_feat = add_all_features(de_load)

# Drop rows at the beginning where lags are NaN
lag_cols = [c for c in de_load_feat.columns if "lag_" in c]
de_load_feat = de_load_feat.dropna(subset=lag_cols).copy()

# =========================================================
# 4. TRAIN / TEST / PREDICT SPLIT
# =========================================================
now_berlin = pd.Timestamp.now(tz="Europe/Berlin")
tomorrow_10am = (now_berlin + pd.Timedelta(days=1)).replace(hour=10, minute=0, second=0, microsecond=0)
tomorrow_10am_utc = tomorrow_10am.tz_convert("UTC")

# Training data (where we have both demand and temp)
df_historical = de_load_feat[de_load_feat["demand_mw"].notnull()].copy()

# Prediction data (where demand is null, up to tomorrow 10am)
df_predict = de_load_feat[(de_load_feat["demand_mw"].isnull()) & (de_load_feat.index <= tomorrow_10am_utc)].copy()

# Further split historical into train/test for model evaluation (last 15 days as test)
split_time = df_historical.index.max() - pd.Timedelta(days=15)
df_train = df_historical.loc[df_historical.index < split_time].copy()
df_test = df_historical.loc[df_historical.index >= split_time].copy()

# =========================================================
# 5. DEFINE X / y
# =========================================================
target_col = "demand_mw"

X_train = df_train.drop(columns=[target_col])
y_train = df_train[target_col]

X_test = df_test.drop(columns=[target_col])
y_test = df_test[target_col]

X_predict = df_predict.drop(columns=[target_col])

# =========================================================
# 6. PREPROCESSING
# =========================================================
def fit_prep_pipeline(df_in):
    cat_features = ["month", "weekday", "hour"]
    bool_features = ["holiday"]
    num_features = [c for c in df_in.columns if c not in cat_features + bool_features and c != target_col]

    prep_pipeline = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_features),
            ("bool", FunctionTransformer(validate=False), bool_features),
            ("num", StandardScaler(), num_features),
        ],
        remainder="drop",
    )

    prep_pipeline.fit(df_in)

    feature_names = []
    one_hot_tf = prep_pipeline.named_transformers_["cat"]
    for i, cat_feature in enumerate(cat_features):
        categories = one_hot_tf.categories_[i]
        feature_names.extend([f"{cat_feature}_{c}" for c in categories])

    feature_names.extend(bool_features)
    feature_names.extend(num_features)

    return feature_names, prep_pipeline

feature_names, prep_pipeline = fit_prep_pipeline(X_train)

def prep_data(X):
    X_prep = prep_pipeline.transform(X)
    return pd.DataFrame(X_prep, columns=feature_names, index=X.index)

X_train_prep = prep_data(X_train)
X_test_prep = prep_data(X_test)
X_predict_prep = prep_data(X_predict)

# =========================================================
# 7. MODEL EVALUATION & SELECTION
# =========================================================
from sklearn.linear_model import SGDRegressor
from sklearn.ensemble import RandomForestRegressor

models = {
    "Linear_SGD": SGDRegressor(penalty="elasticnet", max_iter=1000, tol=1e-3, random_state=42),
    "Random_Forest": RandomForestRegressor(n_estimators=100, min_samples_leaf=3, random_state=42, n_jobs=-1),
    "XGBoost": XGBRegressor(
        n_estimators=300, max_depth=4, learning_rate=0.05, 
        subsample=0.8, colsample_bytree=0.8, min_child_weight=5, random_state=42, n_jobs=-1
    )
}

best_model_name = None
best_model = None
best_rmse = float('inf')

print("Evaluating Models on the test set (last 15 days)...")
for name, model in models.items():
    model.fit(X_train_prep, y_train)
    test_pred = model.predict(X_test_prep)
    test_rmse = np.sqrt(mean_squared_error(y_test, test_pred))
    print(f"{name} Test RMSE: {test_rmse:.4f}")
    
    if test_rmse < best_rmse:
        best_rmse = test_rmse
        best_model = model
        best_model_name = name

print(f"\nSelecting {best_model_name} as the final model with Test RMSE: {best_rmse:.4f}")
final_model = best_model
test_pred = final_model.predict(X_test_prep)
test_rmse = best_rmse

# =========================================================
# 9. GENERATE PREDICTIONS
# =========================================================
print(f"\nGenerating predictions from {X_predict_prep.index.min()} to {X_predict_prep.index.max()}...")
predictions = final_model.predict(X_predict_prep)
pred_df = pd.DataFrame({
    "predicted_demand_mw": predictions
}, index=X_predict_prep.index)

print("\n--- UPCOMING FORECAST ---")
print(pred_df)

# --- SAVE TO DATABASE ---
conn_save = psycopg2.connect(
    host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD")
)
cursor = conn_save.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS fct_demand_forecast (
        forecast_target_time TIMESTAMPTZ PRIMARY KEY,
        region TEXT NOT NULL,
        predicted_demand DOUBLE PRECISION NOT NULL,
        inserted_at TIMESTAMPTZ DEFAULT NOW()
    );
''')
from psycopg2.extras import execute_values
insert_query = '''
INSERT INTO fct_demand_forecast (forecast_target_time, region, predicted_demand)
VALUES %s
ON CONFLICT (forecast_target_time) DO UPDATE 
SET predicted_demand = EXCLUDED.predicted_demand, inserted_at = NOW();
'''
rows = [(idx.to_pydatetime(), 'DE', float(val)) for idx, val in zip(pred_df.index, pred_df["predicted_demand_mw"])]
execute_values(cursor, insert_query, rows, page_size=1000)
conn_save.commit()
cursor.close()
conn_save.close()
print(f"Saved {len(rows)} predictions to fct_demand_forecast table.")
# ------------------------

# Plot actual vs predicted for the last 5 days + future
last_5_days = df_historical.loc[df_historical.index >= df_historical.index.max() - pd.Timedelta(days=5)]

plt.figure(figsize=(14, 5))
plt.plot(last_5_days.index, last_5_days["demand_mw"], label="Actual Demand", color="black")
plt.plot(pred_df.index, pred_df["predicted_demand_mw"], label="Predicted Demand (Future)", color="red", linestyle="--")

# Also plot the test set predictions to show how well it fits
test_pred_df = pd.DataFrame({"test_pred": test_pred}, index=y_test.index)
test_plot = test_pred_df.loc[test_pred_df.index >= df_historical.index.max() - pd.Timedelta(days=5)]
plt.plot(test_plot.index, test_plot["test_pred"], label="Test Set Prediction", color="tab:blue", alpha=0.7)

plt.axvline(x=df_historical.index.max(), color='green', linestyle=':', label="Current Time (End of Data)")
plt.title("Energy Demand Forecast (including Tomorrow 10:00 AM)")
plt.ylabel("Demand (MW)")
plt.legend()
plt.grid(True)
plt.savefig("forecast_plot.png")
print("Saved forecast plot to forecast_plot.png")