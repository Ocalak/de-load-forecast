import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import (
    StandardScaler, OneHotEncoder, FunctionTransformer
)
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import (
    train_test_split, KFold, GridSearchCV, ParameterGrid,learning_curve, TimeSeriesSplit
)
from sklearn.metrics import mean_squared_error

from sklearn.linear_model import SGDRegressor
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor, DMatrix
from xgboost import cv as xgb_cv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

query="""
SELECT *
FROM fct_energy_features
ORDER BY timestamp
"""
df = pd.read_sql(query, conn)
conn.close()

df_dummies = pd.get_dummies(df, columns=["hour", "season", "dow"], drop_first=True)
df = df_dummies.set_index("timestamp")
X = df.drop(columns=["demand_mw"])
y = df["demand_mw"]

# Scale X for SGDRegressor
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)
X_scaled_df = pd.DataFrame(X_scaled, index=X.index, columns=X.columns)

split_idx = int(len(X) * 0.8)

# Unscaled data (Tree models)
X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

# Scaled data (Linear models)
X_train_scaled, X_test_scaled = X_scaled_df.iloc[:split_idx], X_scaled_df.iloc[split_idx:]

models = {
    "Linear_SGD": SGDRegressor(penalty="elasticnet", max_iter=1000, tol=1e-3, random_state=42),
    "Random_Forest": RandomForestRegressor(n_estimators=100, min_samples_leaf=3, random_state=42, n_jobs=-1),
    "XGBoost": XGBRegressor(
        n_estimators=300, max_depth=4, learning_rate=0.05, 
        subsample=0.8, colsample_bytree=0.8, min_child_weight=5, random_state=42
    )
}

print("Evaluating Models...")
results = {}

for name, model in models.items():
    print(f"Training {name}...")
    if name == "Linear_SGD":
        model.fit(X_train_scaled, y_train)
        preds = model.predict(X_test_scaled)
    else:
        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        
    mse = mean_squared_error(y_test, preds)
    rmse = np.sqrt(mse)
    mae = np.mean(np.abs(y_test - preds))
    mape = np.mean(np.abs((y_test - preds) / y_test)) * 100
    
    results[name] = {"RMSE": rmse, "MAE": mae, "MAPE": mape}

print("\n--- Model Comparison ---")
results_df = pd.DataFrame(results).T
print(results_df)

best_model = results_df["RMSE"].idxmin()
print(f"\nBest Model based on RMSE: {best_model} -> {results_df.loc[best_model, 'RMSE']:.2f}")
