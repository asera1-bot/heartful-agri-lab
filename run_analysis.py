from __future__ import annotations

from pathlib import Path
import re
import math
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent
DATASETS = ROOT / "datasets"
REPORTS = ROOT / "reports"
FIG_DIR = REPORTS / "figures"

HARVEST_PATH = DATASETS / "planting_conditions.csv"
ENV_PATH = DATASETS / "env_daily.csv"

REPORTS.mkdir(parents=True, exist_ok=True)
FIG_DIR.mkdir(parents=True, exist_ok=True)

# 59期 実験棟（Z棟）の株数 （暫定、枯死数は無視）
PLANT_COUNTS_59 = {
    ("よつぼし", "Z", "上"): 34,
    ("よつぼし", "Z", "中"): 140,
    ("よつぼし", "Z", "下"): 22,
    ("やよいひめ", "Z", "上"): 72,
    ("やよいひめ", "Z", "中"): 210,
    ("やよいひめ", "Z", "下"): 20,
    ("紅ほっぺ", "Z", "上"): 72,
    ("紅ほっぺ", "Z", "中"): 210,
    ("紅ほっぺ", "Z", "下"): 144,
    ("かおり野", "Z", "上"): 32,
    ("かおり野", "Z", "中"): 140,
    ("かおり野", "Z", "下"): 34,
}

COMPANY_MAP = {
    "ｚ": "Z",
    "z": "Z",
    "Ｚ": "Z",
    "アドビ": "Adobe",
    "Naito": "NaITO",
    "Ｎａｉｔｏ": "NaITO",
}

HOUSE_MAP = {
    "ｚ": "Z",
    "z": "Z",
    "Ｚ": "Z",
}

def normalize_text(x: object) -> object:
    if pd.isna(x):
        return x
    s = str(x).strip()
    s = s.replace("\u3000", " ")
    return s

def clean_harvest(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [normalize_text(c) for c in out.columns]

    required = ["日付", "期", "企業名", "ハウスNo", "段", "品種", "処理", "収量", "パック"]
    missing = [c for c in required if c not in out.columns]
    if missing:
        raise ValueError(f"planting_conditions.csv に必要列がありません: {missing}")

    out["日付"] = pd.to_datetime(out["日付"], errors="coerce")
    out["期"] = pd.to_numeric(out["期"], errors="coerce")
    for c in ["企業名", "ハウスNo", "段", "品種", "処理"]:
        out[c] = out[c].map(normalize_text)

    out["企業名"] = out["企業名"].replace(COMPANY_MAP)
    out["ハウスNo"] = out["ハウスNo"].replace(HOUSE_MAP)
    out["段"] = out["段"].replace({"上段": "上", "中段": "中", "下段": "下"})

    out["収量"] = pd.to_numeric(out["収量"], errors="coerce")
    out["パック"] = pd.to_numeric(out["パック"], errors="coerce")

    # 品種のベース名（処理は別名で保持）
    out["品種_base"] = out["品種"].str.replace(r"（.*?）", "", regex=True)
    out["品種_base"] = out["品種_base"].str.replace(r"\(.*?\)", "", regex=True)
    out["品種_base"] = out["品種_base"].map(normalize_text)

    out["処理"] = out["処理"].fillna("通常")
    out["パック推定"] = np.where(out["収量"].notna(), out["収量"] / 100.0, np.nan)
    out["パック最終"] = out["パック"].fillna(out["パック推定"])

    # 基本分析対象>0, 段あり, 日付あり　を使う
    out = out[out["日付"].notna()].copy()
    out = out[out["収量"].fillna(0) > 0].copy()
    out = out[out["段"].notna()].copy()

    out["date"] = out["日付"].dt.normalize()
    return out

def load_env(path: Path) -> pd.DataFrame:
    env = pd.read_csv(path)
    env["date"] = pd.to_datetime(env["date"], errors="coerce").dt.normalize()
    env = env.dropna(subset=["date"]).copy()
    return env

def attach_env_same_day(harvest: pd.DataFrame, env: pd.DataFrame) -> pd.DataFrame:
    h = harvest.copy()
    e = env.copy()
    h["date"] = pd.to_datetime(h["date"], errors="coerce")
    joined = h.merge(e, on="date", how="left")
    env_cols = [c for c in e.columns if c != "date"]
    joined["env_hit"] = joined[env_cols].notna().any(axis=1)
    return joined

def add_plant_counts(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    def lookup(row: pd.Series) -> float:
        key = (row["品種_base"], row["ハウスNo"], row["段"])
        if pd.notna(row["期"]) and int(row["期"]) == 59 and key in PLANT_COUNTS_59:
            return PLANT_COUNTS_59[key]
        return np.nan

    out["株数"] = out.apply(lookup, axis=1)
    out["株当たり収量"] = out["収量"] / out["株数"]
    return out

def summary_by_variety_level(df: pd.DataFrame) -> pd.DataFrame:
    work = add_plant_counts(df).copy()
    return (
        work.groupby(["期", "ハウスNo", "段", "品種_base"], dropna=False)
        .agg(
            n_records=("日付", "count"),
            total_yield_g=("収量", "sum"),
            total_packs=("パック最終", "sum"),
            mean_yield_g=("収量", "mean"),
            plants=("株数", "max"),
            yield_per_plant_g=("株当たり収量", "sum"),
        )
        .reset_index()
        .sort_values(["期", "品種_base", "段"])
    )

def summary_env_hit(df: pd.DataFrame) -> pd.DataFrame:
    work = add_plant_counts(df).copy()
    env_cols = [
        c for c in work.columns
        if c.startswith(("temp_c_", "rh_pct_", "vpd_kpa_", "sand_temp_c_", "lux_", "vwc_"))
    ]
    hit = work[work[env_cols].notna().any(axis=1)].copy() if env_cols else work.iloc[0:0].copy()
    if hit.empty:
        return pd.DataFrame()

    cols = {
        "temp_c_mean": "temp_mean",
        "rh_pct_mean": "rh_mean",
        "vpd_kpa_mean": "vpd_mean",
        "sand_temp_c_mean": "sand_temp_mean",
        "lux_mean": "lux_mean",
    }

    agg_map = {
        "日付": ("日付", "count"),
        "収量": ("収量", "sum"),
        "パック最終": ("パック最終", "sum"),
        "株当たり収量": ("株当たり収量", "sum"),
    }

    for src in cols:
        if src in hit.columns:
            agg_map[src] = (src, "mean")

    out = (
        hit.groupby(["期", "ハウスNo", "段", "品種_base"], dropna=False)
        .agg(**{k: v for k, v in agg_map.items()})
        .reset_index()
        .rename(columns={
            "日付": "n_env_records",
            "収量": "env_hit_total_yield_g",
            "パック最終": "env_hit_total_packs",
            "株当たり収量": "env_hit_yield_per_plant_g",
            **cols,
        })
    )
    return out.sort_values(["期", "品種_base", "段"])

def cumulative_table(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    daily = (
        work.groupby(["期", "ハウスNo", "段", "品種_base", "日付"], dropna=False)["収量"]
        .sum()
        .reset_index()
        .sort_values(["期", "ハウスNo", "段", "品種_base", "日付"])
    )
    daily["累積収量_g"] = daily.groupby(["期", "ハウスNo", "段", "品種_base"])["収量"].cumsum()
    return daily

def company_house_consistency(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    grp = (
        work.groupby(["期", "企業名", "品種_base"], dropna=False)["ハウスNo"]
        .agg(lambda s: sorted(set(x for x in s if pd.notna(x))))
        .reset_index(name="houses")
    )
    grp["house_count"] = grp["houses"].apply(len)
    return grp.sort_values(
        ["house_count", "期", "企業名", "品種_base"],
        ascending=[False, True, True, True]
    )

def save_excel(outputs: dict[str, pd.DataFrame], path: Path) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet, df in outputs.items():
            df.to_excel(writer, sheet_name=sheet[:31], index=False)

def main() -> None:
    if not HARVEST_PATH.exists():
        raise FileNotFoundError(f"not found: {HARVEST_PATH}")
    if not ENV_PATH.exists():
        raise FileNotFoundError(f"not found: {ENV_PATH}")

    raw = pd.read_csv(HARVEST_PATH)
    harvest = clean_harvest(raw)
    env = load_env(ENV_PATH)
    joined = attach_env_same_day(harvest, env)

    summary_all = summary_by_variety_level(harvest)
    summary_joined = summary_env_hit(joined)
    cumulative = cumulative_table(harvest)
    consistency = company_house_consistency(harvest)

    cleaned_path = REPORTS / "harvest_master_cleaned.csv"
    joined_path = REPORTS / "harvest_env_joined_same_day.csv"
    xlsx_path = REPORTS / "analysis_summary.xlsx"

    harvest.to_csv(cleaned_path, index=False)
    joined.to_csv(joined_path, index=False)

    save_excel(
        {
            "harvest_master_cleaned": harvest,
            "summary_all": summary_all,
            "summary_env_hit": summary_joined,
            "cumulative": cumulative,
            "consistency_check": consistency,
        },
        xlsx_path,
    )

    hit_rate = joined["env_hit"].mean() if ("env_hit" in joined.columns and len(joined)) else float("nan")

    print(f"saved: {cleaned_path}")
    print(f"saved: {joined_path}")
    print(f"saved: {xlsx_path}")
    print(f"rows_harvest={len(harvest)} rows_joined={len(joined)} env_hit_rate={hit_rate:.3f}")

def plot_bar_yield(summary_all: pd.DataFrame) -> None:
    import matplotlib.pyplot as plt
    df = summary_all.copy()
    if df.empty:
        return

    target = df[df["期"] == 59].copy()
    if target.empty:
        return

    # 先に品種×段で集計して1セル１値にする
    plot_df = (
        target.groupby(["品種_base", "段"], dropna=False)["total_yield_g"]
        .sum()
        .reset_index()
    )

    levels = ["上", "中", "下"]
    varieties = list(plot_df["品種_base"].dropna().unique())
    fig, ax = plt.subplots(figsize=(12, 7))
    width = 0.22
    x = np.arange(len(varieties))

    for i, level in enumerate(levels):
        sub = plot_df[plot_df["段"] == level].set_index("品種_base")
        y = [float(sub.loc[v, "total_yield_g"]) if v in sub.index else 0.0 for v in varieties]
        ax.bar(x + (i - 1) * width, y, width=width, label=level)

    ax.set_title("59期 実験棟 品種×段 総収量")
    ax.set_xlabel("品種")
    ax.set_ylabel("総収量(g)")
    ax.set_xticks(x)
    ax.set_xticklabels(varieties, rotation=0)
    ax.legend(title="段")
    fig.tight_layout()
    fig.savefig(FIG_DIR / "01 bar_total_yield_59ki.png", dpi=200)
    plt.close(fig)

def plot_bar_yield_per_plant(summary_all: pd.DataFrame) -> None:
    import matplotlib.pyplot as plt

    df = summary_all.copy()
    if df.empty:
        return

    target = df[(df["期"] == 59) & (df["yield_per_plant_g"].notna())].copy()
    if target.empty:
        return
    
    plot_df = (
        target.groupby(["品種_base", "段"], dropna=False)["yield_per_plant_g"]
        .sum()
        .reset_index()
    )

    levels = ["上", "中", "下"]
    varieties = list(target["品種_base"].dropna().unique())

    fig, ax = plt.subplots(figsize=(12, 7))
    width = 0.22
    x = np.arange(len(varieties))

    for i, level in enumerate(levels):
        sub = plot_df[plot_df["段"] == level].set_index("品種_base")
        y = [float(sub.loc[v, "yield_per_plant_g"]) if v in sub.index else 0.0 for v in varieties]
        ax.bar(x + (i - 1) * width, y, width=width, label=level)
    
    ax.axhline(250, linestyle="--", linewidth=1)
    ax.set_title("59期 実験棟 品種×段 株当り収量")
    ax.set_xlabel("品種")
    ax.set_ylabel("株当り収量(g/株)")
    ax.set_xticks(x)
    ax.set_xticklabels(varieties, rotation=0)
    ax.legend(title="段")
    fig.tight_layout()
    fig.savefig(FIG_DIR / "02_bar_yield_per_plant_59ki.png", dpi=200)
    plt.close(fig)

def plot_cumulative(cumulative: pd.DataFrame) -> None:
    import matplotlib.pyplot as plt

    df = cumulative.copy()
    if df.empty:
        return

    target = df[df["期"] == 59].copy()
    if target.empty:
        return

    target["series"] = target["品種_base"].astype(str) + "_" + target["段"].astype(str)

    fig, ax = plt.subplots(figsize=(13, 7))
    for key, sub in target.groupby("series"):
        sub = sub.sort_values("日付")
        ax.plot(sub["日付"], sub["累積収量_g"], label=key)

    ax.set_title("59期 実験棟 累積収量曲線")
    ax.set_xlabel("日付")
    ax.set_ylabel("累積収量(g)")
    ax.legend(ncol=2, fontsize=8)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "03_cumulative_yield_59ki.png", dpi=200)
    plt.close(fig)

def plot_vpd_scatter(joined: pd.DataFrame) -> None:
    import matplotlib.pyplot as plt

    df = joined.copy()
    if df.empty:
        return

    target = df[(df["期"] == 59) & (df["env_hit"])].copy()
    if target.empty or "vpd_kpa_mean" not in target.columns:
        return

    fig, ax = plt.subplots(figsize=(13, 7))
    for variety, sub in target.groupby("品種_base"):
        ax.scatter(sub["vpd_kpa_mean"], sub["収量"], label=variety)

    valid = target[["vpd_kpa_mean", "収量"]].dropna()
    if len(valid) >= 2:
        z = np.polyfit(valid["vpd_kpa_mean"], valid["収量"], 1)
        xs = np.linspace(valid["vpd_kpa_mean"].min(), valid["vpd_kpa_mean"].max(), 100)
        ys = z[0] * xs + z[1]
        ax.plot(xs, ys, linewidth=1)

    ax.set_title("59期 実験棟 VPD平均 × 収量")
    ax.set_xlabel("VPD平均(kpa)")
    ax.set_ylabel("収量(g)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(FIG_DIR / "04_scatter_vpd_vs_yield_59ki.png", dpi=200)
    plt.close(fig)

def plot_temp_scatter(joined: pd.DataFrame) -> None:
    import matplotlib.pyplot as plt

    df = joined.copy()
    if df.empty:
        return

    target = df[(df["期"] == 59) & (df["env_hit"])].copy()
    if target.empty or "temp_c_mean" not in target.columns:
        return

    fig, ax = plt.subplots(figsize=(10, 7))
    for variety, sub in target.groupby("品種_base"):
        ax.scatter(sub["temp_c_mean"], sub["収量"], label=variety)

    valid = target[["temp_c_mean", "収量"]].dropna()
    if len(valid) >= 2:
        z = np.polyfit(valid["temp_c_mean"], valid["収量"], 1)
        xs = np.linspace(valid["temp_c_mean"].min(), valid["temp_c_mean"].max(), 100)
        ys = z[0] * xs * z[1]
        ax.plot(xs, ys, linewidth=1)

    ax.set_title("59期 実験棟 温度平均 × 収量")
    ax.set_xlabel("温度平均(℃ )")
    ax.set_ylabel("収量(g)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(FIG_DIR / "05_scatter_temp vs_yield_59ki.png", dpi=200)
    plt.close(fig)

def main() -> None:
    if not HARVEST_PATH.exists():
        raise FileNotFoundError(f"not found: {HARVEST_PATH}")
    if not ENV_PATH.exists():
        raise FileNotFoundError(f"not found: {ENV_PATH}")

    raw = pd.read_csv(HARVEST_PATH)
    harvest = clean_harvest(raw)
    env = load_env(ENV_PATH)
    joined = attach_env_same_day(harvest, env)

    summary_all = summary_by_variety_level(harvest)
    summary_joined = summary_env_hit(joined)
    cumulative = cumulative_table(harvest)
    consistency = company_house_consistency(harvest)

    cleaned_path = REPORTS / "harvest_master_cleaned.csv"
    joined_path = REPORTS / "harvest_env_joined_same_day.csv"
    xlsx_path = REPORTS / "analytics_summary.xlsx"

    harvest.to_csv(cleaned_path, index=False)
    joined.to_csv(joined_path, index=False)

    save_excel(
        {
            "harvest_master_cleaned": harvest,
            "summary_all": summary_all,
            "summary_env_hit": summary_joined,
            "cumulative": cumulative,
            "consistency_chech": consistency,
        },
        xlsx_path,
    )

    plot_bar_yield(summary_all)
    plot_bar_yield_per_plant(summary_all)
    plot_cumulative(cumulative)
    plot_vpd_scatter(joined)
    plot_temp_scatter(joined)

    hit_rate = joined["env_hit"].mean() if ("env_hit" in joined.columns and len(joined)) else float("nan")

    print(f"saved: {cleaned_path}")
    print(f"saved: {joined_path}")
    print(f"saved: {xlsx_path}")
    print(f"saved figures: {FIG_DIR}")
    print(f"rows_harvest={len(harvest)} rows_joined={len(joined)} ecn_hit_rate={hit_rate:.3f}")

if __name__ == "__main__":
    main()
