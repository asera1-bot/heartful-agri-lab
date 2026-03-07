import pandas as pd

env = pd.read_csv("datasets/env_daily.csv")
<<<<<<< HEAD
print(env.head())
print(env.columns)
print(env.dtypes)
=======
env["date"] = pd.to_datetime(env["date"]).dt.date

df = pd.read_excel("datesets/test/イチゴ/59期収量（実験棟）.xlsx")
df["date"] = pd.to_datetime(df["日付"]).dt.date

joined = df.merge(env, on="date", how="left")

print("env hit rate:",
    joined.drop(colmns=["date"]).notna().any(axis=1).mean())

print("env range:", env["date"].min(), "->", env["date"].max())
>>>>>>> 6f82dd6 (env x test_harvest joined)
