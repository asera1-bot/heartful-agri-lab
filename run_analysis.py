
from  __future__ import annotations

from pathlib import Path
from typing import Iterable
import re

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm


# =========================================================
# paths / config
# =========================================================
ROOT = Path(__file__).resolve().parent
DATASETS = ROOT / "datasets"
REPORTS = ROOT / "reports"
FIG_DIR = REPORTS / "figures"
EXT_DIR = REPORTS / "extended"
EXT_FIG_DIR = EXT_DIR / "figures_ext"

HARVEST_PATH = DATASETS / "planting_conditions.csv"
ENV_PATH = DATASETS / "env_daily.csv"

SALES_XLSX = DATASETS / "野菜販売　集計表59期.xlsx"

# 必要に応じて実ファイルへ変更
WORK_XLSX = DATASETS / "test" / "イチゴ" / "59期生データ.xlsx"
KALE_GROWTH_XLSX = DATASETS / "test" / "ケール_ハーブ" / "ケール　収量まとめ.xlsx"
KALE_WORK_XLSX = DATASETS / "test" / "ケール_ハーブ" / "生データ　ケール.xlsx"
HERB_GROWTH_XLSX = DATASETS / "test" / "ケール_ハーブ" / "生データ　ハーブ.xlsx"
HERB_WORK_XLSX = DATASETS / "test" / "ケール_ハーブ" / "生データ　ハーブ.xlsx"

PACK_G = 100
PACK_PRICE_YEN = 300

REPORTS.mkdir(parents=True, exist_ok=True)
FIG_DIR.mkdir(parents=True, exist_ok=True)
EXT_DIR.mkdir(parents=True, exist_ok=True)
EXT_FIG_DIR.mkdir(parents=True, exist_ok=True)


# =========================================================
# reference
# =========================================================
FIELD_MASTER_58 = [
    # 企業棟（中）
    ("58", "B1", "中", "岡部",   "紅ほっぺ", 130),
    ("58", "B1", "中", "Adobe", "かおり野", 260),
    ("58", "B1", "中", "バリュ", "紅ほっぺ", 130),

    ("58", "B2", "中", "ソリトン", "紅ほっぺ", 130),
    ("58", "B2", "中", "富士機械", "やよいひめ", 260),
    ("58", "B2", "中", "QB",     "紅ほっぺ", 130),

    # B3は注記対象
    ("58", "B3", "中", "慈誠会", "紅ほっぺ", 130),
    ("58", "B3", "中", "ケイアイ", "紅ほっぺ", 260),

    ("58", "B4", "中", "昭和",   "紅ほっぺ", 130),
    ("58", "B4", "中", "富士機材", "やよいひめ", 260),
    ("58", "B4", "中", "三和",   "紅ほっぺ", 130),

    ("58", "B5", "中", "QB",    "紅ほっぺ", 130),
    ("58", "B5", "中", "東レ",   "よつぼし", 260),
    ("58", "B5", "中", "日建",   "紅ほっぺ", 130),

    # その他中段
    ("58", "A4", "中", "東レ",   "紅ほっぺ", 520),
    ("58", "E2", "中", "マルテー", "紅ほっぺ", 520),
    ("58", "Z",  "中", "マルテー", "紅ほっぺ", 372),

    # Z上段（すべてマルテー）
    ("58", "Z", "上", "マルテー", "かおり野", 40),
    ("58", "Z", "上", "マルテー", "かおり野", 56),
    ("58", "Z", "上", "マルテー", "よつぼし", 56),
    ("58", "Z", "上", "マルテー", "やよいひめ", 56),
    ("58", "Z", "上", "マルテー", "やよいひめ", 38),
]

FIELD_MASTER_59 = [
    # 企業棟（中）
    ("59", "B1", "中", "岡部",   "紅ほっぺ", 200),
    ("59", "B1", "中", "Adobe", "かおり野", 400),
    ("59", "B1", "中", "バリュ", "紅ほっぺ", 200),

    ("59", "B2", "中", "ソリトン", "紅ほっぺ", 200),
    ("59", "B2", "中", "富士機械", "やよいひめ", 400),
    ("59", "B2", "中", "NaITO",  "紅ほっぺ", 200),

    # B3は注記対象
    ("59", "B3", "中", "慈誠会", "紅ほっぺ", 200),
    ("59", "B3", "中", "ケイアイ", "紅ほっぺ", 400),

    ("59", "B4", "中", "昭和",   "紅ほっぺ", 200),
    ("59", "B4", "中", "富士機材", "やよいひめ", 400),
    ("59", "B4", "中", "三和",   "紅ほっぺ", 200),

    ("59", "B5", "中", "QB",    "紅ほっぺ", 200),
    ("59", "B5", "中", "東レ",   "よつぼし", 400),
    ("59", "B5", "中", "バリュ", "紅ほっぺ", 200),

    # Z中（マルテー）
    ("59", "Z", "中", "マルテー", "よつぼし", 140),
    ("59", "Z", "中", "マルテー", "やよいひめ", 210),
    ("59", "Z", "中", "マルテー", "紅ほっぺ", 210),
    ("59", "Z", "中", "マルテー", "かおり野", 140),

    # Z上段（マルテー）
    ("59", "Z", "上", "マルテー", "よつぼし", 34),
    ("59", "Z", "上", "マルテー", "やよいひめ", 72),
    ("59", "Z", "上", "マルテー", "紅ほっぺ", 72),
    ("59", "Z", "上", "マルテー", "かおり野", 32),

    # Z下段（マルテー）
    ("59", "Z", "下", "マルテー", "よつぼし", 22),
    ("59", "Z", "下", "マルテー", "やよいひめ", 20),
    ("59", "Z", "下", "マルテー", "紅ほっぺ", 144),
    ("59", "Z", "下", "マルテー", "かおり野", 34),
]

PLANTS_58_ALL = {
    "紅ほっぺ": 2842,
    "かおり野": 356,
    "よつぼし": 316,
    "やよいひめ": 614,
}

PLANTS_58_COMPANY = {
    "紅ほっぺ": 2470,
    "かおり野": 260,
    "よつぼし": 260,
    "やよいひめ": 520,
}

PLANTS_58_Z = {
    "紅ほっぺ": 372,
    "かおり野": 96,
    "よつぼし": 56,
    "やよいひめ": 94,
}


PLANTS_59_ALL = {
    "紅ほっぺ": 2426,
    "かおり野": 606,
    "よつぼし": 596,
    "やよいひめ": 1102,
}

PLANTS_59_COMPARE = {
    "紅ほっぺ": 2026,   # 企業1600 + Z426
    "かおり野": 606,
    "よつぼし": 596,
    "やよいひめ": 1102,
}



# 58期は発表資料から比較用に固定値で参照
REFERENCE_58_VARIETY = [
    {"期": 58, "品種_base": "かおり野", "株数": 360, "総収量_g": 61090, "株当たり収量_g": 170, "パック数": 323},
    {"期": 58, "品種_base": "よつぼし", "株数": 360, "総収量_g": 85880, "株当たり収量_g": 239, "パック数": 306},
    {"期": 58, "品種_base": "やよいひめ", "株数": 700, "総収量_g": 112040, "株当たり収量_g": 160, "パック数": 510},
    {"期": 58, "品種_base": "紅ほっぺ", "株数": 1830, "総収量_g": 292670, "株当たり収量_g": 160, "パック数": 1231},
]

ALLOWED_VARIETIES = {"紅ほっぺ", "かおり野", "やよいひめ", "よつぼし"}


# =========================================================
# utility
# =========================================================
def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def normalize_text(x: object) -> object:
    if pd.isna(x):
        return x
    return str(x).strip().replace("\u3000", " ")


def setup_matplotlib_font() -> None:
    candidates = [
        "Noto Sans CJK JP",
        "Noto Sans JP",
        "IPAexGothic",
        "IPAGothic",
        "Yu Gothic",
        "MS Gothic",
    ]
    available = {f.name for f in fm.fontManager.ttflist}
    selected = next((name for name in candidates if name in available), None)
    if selected:
        plt.rcParams["font.family"] = selected
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["figure.autolayout"] = True


def first_existing_sheet(xls: pd.ExcelFile, candidates: Iterable[str]) -> str:
    normalized = {str(s).strip(): s for s in xls.sheet_names}
    for c in candidates:
        if c in normalized:
            return normalized[c]
    raise ValueError(f"候補シートが見つかりません: {list(candidates)} / actual={xls.sheet_names}")


def time_like_to_minutes(x: object) -> float:
    if pd.isna(x):
        return np.nan

    if isinstance(x, pd.Timedelta):
        return x.total_seconds() / 60

    if hasattr(x, "hour") and hasattr(x, "minute"):
        return x.hour * 60 + x.minute + getattr(x, "second", 0) / 60

    parsed = pd.to_timedelta(str(x), errors="coerce")
    if pd.notna(parsed):
        return parsed.total_seconds() / 60

    return pd.to_numeric(x, errors="coerce")


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [normalize_text(c) for c in out.columns]
    return out


def coerce_numeric_if_exists(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def validate_varieties(df: pd.DataFrame) -> None:
    if "品種_base" in df.columns:
        bad = sorted(set(df["品種_base"].dropna()) - ALLOWED_VARIETIES)
        if bad:
            print("[unknown varieties]", bad)


def load_harvest_cleaned(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig")
    df = clean_columns(df)

    if "日付" in df.columns:
        df["日付"] = pd.to_datetime(df["日付"], errors="coerce")
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    for c in ["期", "収量", "パック", "パック推定", "パック最終", "株数", "株当たり収量"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

# =========================================================
# strawberry / core
# =========================================================
def clean_harvest(df: pd.DataFrame) -> pd.DataFrame:
    out = clean_columns(df)

    required = ["日付", "期", "企業名", "ハウスNo", "段", "品種", "処理", "収量", "パック"]
    missing = [c for c in required if c not in out.columns]
    if missing:
        raise ValueError(f"planting_conditions.csv に必要列がありません: {missing}")

    out["日付"] = pd.to_datetime(out["日付"], errors="coerce")
    out["期"] = pd.to_numeric(out["期"], errors="coerce")

    for c in ["企業名", "ハウスNo", "段", "品種", "処理"]:
        out[c] = out[c].map(normalize_text)

    out["企業名"] = out["企業名"].replace({
        "ｚ": "Z",
        "z": "Z",
        "Ｚ": "Z",
        "アドビ": "Adobe",
        "Naito": "NaITO",
        "Ｎａｉｔｏ": "NaITO",
    })
    out["ハウスNo"] = out["ハウスNo"].replace({
        "ｚ": "Z",
        "z": "Z",
        "Ｚ": "Z",
    })
    out["段"] = out["段"].replace({
        "上段": "上",
        "中段": "中",
        "下段": "下",
    })

    out["品種"] = out["品種"].replace({
        "かおりの": "かおり野",
        "やよい姫": "やよいひめ",
        "弥生姫": "やよいひめ",
    })

    out["収量"] = pd.to_numeric(out["収量"], errors="coerce")
    out["パック"] = pd.to_numeric(out["パック"], errors="coerce")

    out["品種_base"] = out["品種"].str.replace(r"（.*?）", "", regex=True)
    out["品種_base"] = out["品種_base"].str.replace(r"\(.*?\)", "", regex=True)
    out["品種_base"] = out["品種_base"].map(normalize_text)

    out["パック推定"] = np.where(out["収量"].notna(), out["収量"] / PACK_G, np.nan)
    out["パック最終"] = out["パック"].fillna(out["パック推定"])

    out = out[out["日付"].notna()].copy()
    out = out[out["収量"].fillna(0) > 0].copy()
    out = out[out["段"].notna()].copy()

    out["date"] = out["日付"].dt.normalize()

    validate_varieties(out)
    return out


def load_env(path: Path) -> pd.DataFrame:
    env = pd.read_csv(path)
    env = clean_columns(env)
    env["date"] = pd.to_datetime(env["date"], errors="coerce").dt.normalize()
    env = env.dropna(subset=["date"]).copy()
    return env


def attach_env_same_day(harvest: pd.DataFrame, env: pd.DataFrame) -> pd.DataFrame:
    h = harvest.copy()
    e = env.copy()
    joined = h.merge(e, on="date", how="left")
    env_cols = [c for c in e.columns if c != "date"]
    joined["env_hit"] = joined[env_cols].notna().any(axis=1) if env_cols else False
    return joined

def add_plant_counts(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    master = build_field_master_df()

    if "株数" not in out.columns:
        out["株数"] = np.nan

    def norm_house(v):
        if pd.isna(v):
            return v
        return str(v).strip()

    def norm_stage(v):
        if pd.isna(v):
            return v
        s = str(v).strip()
        if s in {"中段", "中"}:
            return "中"
        if s in {"上段", "上"}:
            return "上"
        if s in {"下段", "下"}:
            return "下"
        return s

    def norm_variety(v):
        if pd.isna(v):
            return v
        s = str(v).strip()
        s = s.replace("（断無）", "")
        return s

    out["ハウスNo"] = out["ハウスNo"].map(norm_house)
    out["段"] = out["段"].map(norm_stage)
    out["品種_base"] = out["品種_base"].map(norm_variety)

    master["ハウスNo"] = master["ハウスNo"].map(norm_house)
    master["段"] = master["段"].map(norm_stage)
    master["品種_base"] = master["品種_base"].map(norm_variety)

    # まずキー単位の総株数を作る
    plant_map = (
        master.groupby(["期", "ハウスNo", "段", "品種_base"], dropna=False)["株数_master"]
        .sum()
        .to_dict()
    )

    keys = list(zip(out["期"], out["ハウスNo"], out["段"], out["品種_base"]))
    out["株数"] = [plant_map.get(k, np.nan) for k in keys]

    return out

def summary_by_variety_level(df: pd.DataFrame) -> pd.DataFrame:
    work = add_plant_counts(df)

    agg = (
        work.groupby(["期", "ハウスNo", "段", "品種_base"], dropna=False)
        .agg(
            n_records=("日付", "count"),
            total_yield_g=("収量", "sum"),
            total_packs=("パック最終", "sum"),
            mean_yield_g=("収量", "mean"),
            plants=("株数", "max"),
        )
        .reset_index()
    )

    agg["yield_per_plant_g"] = agg["total_yield_g"] / agg["plants"]

    return agg.sort_values(["期", "品種_base", "段"])

def summary_env_hit(df: pd.DataFrame) -> pd.DataFrame:
    work = add_plant_counts(df)

    env_cols = [
        c for c in work.columns
        if c.startswith(("temp_c_", "rh_pct_", "vpd_kpa_", "sand_temp_c_", "lux_", "vwc_"))
    ]
    if not env_cols:
        return pd.DataFrame()

    hit = work[work[env_cols].notna().any(axis=1)].copy()
    if hit.empty:
        return pd.DataFrame()

    rename_map = {
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
        "株数": ("株数", "max"),
    }
    for src in rename_map:
        if src in hit.columns:
            agg_map[src] = (src, "mean")

    out = (
        hit.groupby(["期", "ハウスNo", "段", "品種_base"], dropna=False)
        .agg(**agg_map)
        .reset_index()
        .rename(columns={
            "日付": "n_env_records",
            "収量": "env_hit_total_yield_g",
            "パック最終": "env_hit_total_packs",
            "株数": "plants",
            **rename_map,
        })
    )

    out["env_hit_yield_per_plant_g"] = out["env_hit_total_yield_g"] / out["plants"]

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
    grp = (
        df.groupby(["期", "企業名", "品種_base"], dropna=False)["ハウスNo"]
        .agg(lambda s: sorted(set(x for x in s if pd.notna(x))))
        .reset_index(name="houses")
    )
    grp["house_count"] = grp["houses"].apply(len)
    return grp.sort_values(
        ["house_count", "期", "企業名", "品種_base"],
        ascending=[False, True, True, True],
    )


def save_excel(outputs: dict[str, pd.DataFrame], path: Path) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, df in outputs.items():
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)


# =========================================================
# presentation figures / strawberry
# =========================================================
def plot_yield_per_plant_59ki(df: pd.DataFrame, out_png: Path) -> None:
    d = add_plant_counts(df)
    d = d[d["期"] == 59].copy()

    agg = (
        d.groupby(["品種_base", "段"], dropna=False)
        .agg(
            総収量_g=("収量", "sum"),
            株数=("株数", "max"),
        )
        .reset_index()
    )

    agg["株当たり収量"] = agg["総収量_g"] / agg["株数"]

    order_variety = ["かおり野", "よつぼし", "やよいひめ", "紅ほっぺ"]
    order_stage = ["上段", "中", "下段", "1B", "2B", "3B", "4B", "1", "2", "3", "4"]

    agg["品種_base"] = pd.Categorical(agg["品種_base"], categories=order_variety, ordered=True)
    agg["段"] = pd.Categorical(agg["段"], categories=order_stage, ordered=True)
    agg = agg.sort_values(["品種_base", "段"])

    plt.figure(figsize=(10, 6))
    x = np.arange(len(agg))
    plt.bar(x, agg["株当たり収量"])
    plt.xticks(x, [f"{v}\n{s}" for v, s in zip(agg["品種_base"], agg["段"])], rotation=45, ha="right")
    plt.ylabel("株当たり収量(g/株)")
    plt.title("59期 品種×段別 株当たり収量")
    plt.tight_layout()
    plt.savefig(out_png, dpi=200, bbox_inches="tight")
    plt.close()

def plot_cumulative_by_variety(df: pd.DataFrame, variety: str, out_path: Path) -> None:
    d = df.copy()
    d = d[d["期"].astype("Int64") == 59].copy()
    d = d[d["品種_base"].astype(str) == str(variety)].copy()
    if d.empty:
        return

    d["日付"] = pd.to_datetime(d["日付"], errors="coerce")
    d = d.dropna(subset=["日付"]).copy()

    daily = (
        d.groupby(["日付", "段"], dropna=False)["収量"]
        .sum()
        .reset_index()
        .sort_values(["日付", "段"])
    )

    stage_order = ["上", "中", "下"]
    plt.figure(figsize=(12, 6))
    for stage in stage_order:
        s = daily[daily["段"] == stage].copy()
        if s.empty:
            continue
        s["累積収量_g"] = s["収量"].cumsum()
        plt.plot(s["日付"], s["累積収量_g"], marker="o", label=stage)

    plt.title(f"59期 累積収量推移（{variety}）")
    plt.xlabel("収穫日")
    plt.ylabel("累積収量 (g)")
    plt.legend(title="段")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close()


def plot_box_yield_distribution(df: pd.DataFrame, out_path: Path) -> None:
    d = df.copy()
    d = d[d["期"].astype("Int64") == 59].copy()
    if d.empty:
        return

    d["品種_段"] = d["品種_base"].astype(str) + "\n" + d["段"].astype(str)
    stage_rank = {"上": 0, "中": 1, "下": 2}

    order_df = (
        d[["品種_base", "段", "品種_段"]]
        .drop_duplicates()
        .assign(_rank=lambda x: x["段"].map(stage_rank).fillna(99))
        .sort_values(["品種_base", "_rank"])
    )
    order = order_df["品種_段"].tolist()

    data = [d.loc[d["品種_段"] == key, "収量"].dropna().values for key in order]
    labels = [key for key in order if len(d.loc[d["品種_段"] == key, "収量"].dropna()) > 0]
    data = [x for x in data if len(x) > 0]

    if not data:
        return

    plt.figure(figsize=(14, 6))
    plt.boxplot(data, tick_labels=labels, patch_artist=False)
    plt.title("59期 収量分布（品種×段）")
    plt.xlabel("品種 × 段")
    plt.ylabel("収量 (g)")
    plt.xticks(rotation=0)
    plt.grid(axis="y", linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close()


def generate_presentation_figures(df: pd.DataFrame, figures_dir: Path) -> None:
    ensure_dir(figures_dir)
    setup_matplotlib_font()

    plot_yield_per_plant_59ki(df, figures_dir / "01_bar_yield_per_plant_59ki.png")

    for variety in ["よつぼし", "紅ほっぺ", "かおり野", "やよいひめ"]:
        plot_cumulative_by_variety(df, variety, figures_dir / f"02_cumulative_yield_59ki_{variety}.png")

    plot_box_yield_distribution(df, figures_dir / "03_box_yield_distribution_59ki.png")


# =========================================================
# extended analysis / strawberry
# =========================================================
def build_field_master_df() -> pd.DataFrame:
    rows = FIELD_MASTER_58 + FIELD_MASTER_59
    out = pd.DataFrame(
        rows,
        columns=["期", "ハウスNo", "段", "企業名_master", "品種_base", "株数_master"]
    )
    out["期"] = out["期"].astype(int)
    return out

def build_58_reference_table() -> pd.DataFrame:
    return pd.DataFrame(REFERENCE_58_VARIETY)


def build_59_variety_summary(df: pd.DataFrame) -> pd.DataFrame:
    d = df[df["期"] == 59].copy()

    agg = (
        d.groupby(["期", "品種_base"], dropna=False)
        .agg(
            総収量_g=("収量", "sum"),
            パック数=("パック最終", "sum"),
        )
        .reset_index()
    )

    plants_59 = PLANTS_59_COMPARE.copy()   # B3除外で比較するならこっち
    # plants_59 = PLANTS_59_ALL.copy()     # 全棟計で出したいならこっち

    agg["株数"] = agg["品種_base"].map(plants_59)
    agg["株当たり収量_g"] = agg["総収量_g"] / agg["株数"]

    return (
        agg[["期", "品種_base", "株数", "総収量_g", "株当たり収量_g", "パック数"]]
        .sort_values(["品種_base"])
        .reset_index(drop=True)
    )

def build_58_variety_summary(df: pd.DataFrame) -> pd.DataFrame:
    d = df[df["期"] == 58].copy()

    agg = (
        d.groupby(["期", "品種_base"], dropna=False)
        .agg(
            総収量_g=("収量", "sum"),
            パック数=("パック最終", "sum"),
        )
        .reset_index()
    )

    plants_58 = PLANTS_58_ALL.copy()
    # plants_58 = PLANTS_58_COMPANY.copy()  # 企業担当のみ比較したいならこっち

    agg["株数"] = agg["品種_base"].map(plants_58)
    agg["株当たり収量_g"] = agg["総収量_g"] / agg["株数"]

    return (
        agg[["期", "品種_base", "株数", "総収量_g", "株当たり収量_g", "パック数"]]
        .sort_values(["品種_base"])
        .reset_index(drop=True)
    )

def add_house_group(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["棟区分"] = np.where(out["ハウスNo"].astype(str).eq("Z"), "Z棟", "企業棟ほか")
    return out


def add_per_plant(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "株当たり収量" not in out.columns:
        out["株当たり収量"] = out["収量"] / out["株数"]
    return out


def summarize_stage_z_only(df: pd.DataFrame) -> pd.DataFrame:
    d = add_per_plant(add_plant_counts(df))
    d = d[d["ハウスNo"].astype(str) == "Z"].copy()
    return (
        d.groupby(["期", "品種_base", "段"], dropna=False)
        .agg(
            records=("日付", "count"),
            total_yield_g=("収量", "sum"),
            plants=("株数", "max"),
            yield_per_plant_g=("株当たり収量", "sum"),
            mean_yield_g=("収量", "mean"),
        )
        .reset_index()
        .sort_values(["期", "品種_base", "段"])
    )


def summarize_house_group(df: pd.DataFrame) -> pd.DataFrame:
    d = add_house_group(add_per_plant(add_plant_counts(df)))
    return (
        d.groupby(["期", "棟区分", "品種_base"], dropna=False)
        .agg(
            total_yield_g=("収量", "sum"),
            mean_yield_g=("収量", "mean"),
            plants=("株数", "sum"),
            yield_per_plant_g=("株当たり収量", "mean"),
        )
        .reset_index()
        .sort_values(["期", "棟区分", "品種_base"])
    )



def build_harvest_timing_table(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()

    if "日付" not in d.columns:
        raise ValueError("harvest に '日付' 列がありません")

    d["日付"] = pd.to_datetime(d["日付"], errors="coerce")
    d = d.dropna(subset=["日付"]).copy()
    d = d[d["収量"] > 0].copy()

    daily = (
        d.groupby(["期", "品種_base", "日付"], dropna=False)["収量"]
        .sum()
        .reset_index()
        .sort_values(["期", "品種_base", "日付"])
    )

    rows = []
    for (ki, variety), sub in daily.groupby(["期", "品種_base"], dropna=False):
        first_day = sub["日付"].min()
        last_day = sub["日付"].max()
        peak_idx = sub["収量"].idxmax()

        rows.append({
            "期": ki,
            "品種_base": variety,
            "初収穫日": first_day,
            "最終収穫日": last_day,
            "収穫期間日数": (last_day - first_day).days + 1,
            "ピーク日": sub.loc[peak_idx, "日付"],
            "ピーク日収量_g": sub.loc[peak_idx, "収量"],
            "総収量_g": sub["収量"].sum(),
            "平均日収量_g": sub["収量"].sum() / ((last_day - first_day).days + 1),
        })

    return pd.DataFrame(rows).sort_values(["期", "品種_base"]).reset_index(drop=True)

def build_monthly_speed_table(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["年月"] = d["日付"].dt.to_period("M").astype(str)
    out = (
        d.groupby(["期", "品種_base", "年月"], dropna=False)
        .agg(
            total_yield_g=("収量", "sum"),
            harvest_days=("日付", "nunique"),
        )
        .reset_index()
    )
    out["収穫速度_g_per_day"] = out["total_yield_g"] / out["harvest_days"]
    return out.sort_values(["期", "品種_base", "年月"])


def build_daily_yield(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()

    d["日付"] = pd.to_datetime(d["日付"], errors="coerce")
    d = d.dropna(subset=["日付"])

    out = (
        d.groupby(["期", "品種_base", "日付"], dropna=False)["収量"]
        .sum()
        .reset_index()
        .sort_values(["期", "品種_base", "日付"])
    )

    return out

def load_sales_summary_sheet(xlsx_path: Path) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, sheet_name="販売数まとめ")
    df.columns = [str(c).strip() for c in df.columns]
    return df

def load_sales_excel(path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(path)
    frames = []

    for sheet in xls.sheet_names:
        if str(sheet).strip() == "販売数まとめ":
            continue

        tmp = pd.read_excel(path, sheet_name=sheet)
        tmp = clean_columns(tmp)

        left = pd.DataFrame()
        if {"品目", "容量", "数量"}.issubset(tmp.columns):
            left = tmp[["品目", "容量", "数量"]].copy()

        right = pd.DataFrame()
        if {"品目.1", "数量.1"}.issubset(tmp.columns):
            right = tmp[["品目.1", "数量.1"]].copy()
            right = right.rename(columns={"品目.1": "品目", "数量.1": "数量"})
            right["容量"] = np.nan

        one = pd.concat([left, right], ignore_index=True)
        if one.empty:
            continue

        one["sheet_name"] = str(sheet).strip()
        frames.append(one)

    if not frames:
        return pd.DataFrame(columns=["品目", "容量", "数量", "sheet_name"])

    df = pd.concat(frames, ignore_index=True)
    df["品目"] = df["品目"].astype(str).str.strip()
    df["容量"] = pd.to_numeric(df["容量"], errors="coerce")
    df["数量"] = pd.to_numeric(df["数量"], errors="coerce")
    df = df[df["品目"].notna() & (df["品目"] != "") & (df["品目"] != "nan")].copy()
    return df

def summarize_strawberry_sales(df_sales: pd.DataFrame) -> pd.DataFrame:
    d = df_sales.copy()
    d = d[(d["品目"] == "イチゴ") & (d["容量"] == PACK_G)].copy()
    out = (
        d.groupby(["sheet_name"], dropna=False)
        .agg(actual_packs=("数量", "sum"))
        .reset_index()
        .sort_values(["sheet_name"])
    )
    out["actual_sales_yen"] = out["actual_packs"] * PACK_PRICE_YEN
    return out


def estimate_sales_from_harvest(df_harvest: pd.DataFrame) -> pd.DataFrame:
    d = df_harvest.copy()
    d["推定パック数"] = d["収量"] / PACK_G
    out = (
        d.groupby(["期", "品種_base"], dropna=False)
        .agg(
            total_yield_g=("収量", "sum"),
            est_packs=("推定パック数", "sum"),
        )
        .reset_index()
        .sort_values(["期", "品種_base"])
    )
    out["est_sales_yen"] = out["est_packs"] * PACK_PRICE_YEN
    return out


def load_worktime_excel(path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(path)
    sheet = first_existing_sheet(xls, ["作業時間"])
    df = pd.read_excel(path, sheet_name=sheet)
    df = clean_columns(df)

    rename_map = {}
    for c in df.columns:
        if c == "品種１":
            rename_map[c] = "品種1"
        if c == "担 当者5":
            rename_map[c] = "担当者5"
    df = df.rename(columns=rename_map)

    if "日付" in df.columns:
        df["日付"] = pd.to_datetime(df["日付"], errors="coerce")

    # 時間列
    if "所要時間（分）" in df.columns:
        df["作業分"] = df["所要時間（分）"].map(time_like_to_minutes)
    elif "作業分" in df.columns:
        df["作業分"] = df["作業分"].map(time_like_to_minutes)
    else:
        df["作業分"] = np.nan

    if "日付" in df.columns:
        df["年月"] = df["日付"].dt.strftime("%Y-%m")

    # 品種列を1列に寄せる
    variety_cols = [c for c in ["品種", "品種1", "品種2", "品種3"] if c in df.columns]
    if variety_cols:
        df["品種"] = df[variety_cols].bfill(axis=1).iloc[:, 0]
        df["品種"] = df["品種"].astype(str).str.strip()
        df = df[df["品種"].notna() & (df["品種"] != "") & (df["品種"] != "nan")]

    # 作業内容列を1列に寄せる
    task_cols = [c for c in ["作業内容", "作業内容1", "作業内容2", "作業内容3", "作業内容4"] if c in df.columns]
    if task_cols:
        df["作業内容"] = df[task_cols].bfill(axis=1).iloc[:, 0]
        df["作業内容"] = df["作業内容"].astype(str).str.strip()
        df = df[df["作業内容"].notna() & (df["作業内容"] != "") & (df["作業内容"] != "nan")]

    return df

def summarize_worktime(df_work: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    by_task = (
        df_work.groupby("作業内容", dropna=False)
        .agg(
            total_minutes=("作業分", "sum"),
            records=("日付", "count"),
        )
        .reset_index()
        .sort_values("total_minutes", ascending=False)
    )

    by_month_task = (
        df_work.groupby(["年月", "作業内容"], dropna=False)
        .agg(total_minutes=("作業分", "sum"))
        .reset_index()
        .sort_values(["年月", "total_minutes"], ascending=[True, False])
    )
    return by_task, by_month_task


# =========================================================
# kale / herb
# =========================================================
def load_kale_growth_excel(path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(path)

    sheet = None
    for s in ["元データ", "生育観察"]:
        if s in xls.sheet_names:
            sheet = s
            break

    if sheet is None:
        print(f"[WARN] load_kale_growth_excel: sheet not found -> {xls.sheet_names}")
        return pd.DataFrame()

    df = pd.read_excel(path, sheet_name=sheet)
    df = clean_columns(df)

    if "日付" in df.columns:
        df["日付"] = pd.to_datetime(df["日付"], errors="coerce")

    for c in [
        "収穫重量 (g)",
        "葉長/葉鞘無し（㎝）",
        "葉長/葉鞘あり（㎝）",
        "葉色（5段階）",
        "縮れ度（5段階）",
        "硬さ（5段階）",
        "株高（㎝）",
        "葉数",
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def load_kale_worktime_excel(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="作業時間", header=1)
    df = clean_columns(df)

    if "日付" in df.columns:
        df["日付"] = pd.to_datetime(df["日付"], errors="coerce")

    if "作業内容1" in df.columns:
        df["作業内容"] = df["作業内容1"].astype(str).str.strip()

    if "所要時間（分）" in df.columns:
        df["所要時間（分）"] = df["所要時間（分）"].map(time_like_to_minutes)
    else:
        df["所要時間（分）"] = np.nan

    return df

def load_herb_growth_excel(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="生育観察")
    df = clean_columns(df)
    df["日付"] = pd.to_datetime(df["日付"], errors="coerce")
    for c in ["株番号", "葉数", "株高（㎝）", "葉色（基準値１-５）"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def load_herb_worktime_excel(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="作業時間")
    df = clean_columns(df)
    df["日付"] = pd.to_datetime(df["日付"], errors="coerce")

    variety_cols = [c for c in ["品種1", "品種2", "品種3", "品種4", "品種5"] if c in df.columns]
    base_cols = [c for c in df.columns if c not in variety_cols]

    parts = []
    for vc in variety_cols:
        tmp = df[base_cols + [vc]].copy()
        tmp = tmp.rename(columns={vc: "品種"})
        tmp["品種"] = tmp["品種"].astype(str).str.strip()
        tmp = tmp[tmp["品種"].notna() & (tmp["品種"] != "") & (tmp["品種"] != "nan")]
        parts.append(tmp)

    out = pd.concat(parts, ignore_index=True) if parts else df.copy()
    out["作業内容"] = out["作業内容"].astype(str).str.strip()
    out["作業分"] = out["作業分"].map(time_like_to_minutes)
    return out

def summarize_kale_growth(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    if "収穫重量 (g)" in d.columns:
        d["収穫重量 (g)"] = pd.to_numeric(d["収穫重量 (g)"], errors="coerce")
        if "葉色（5段階）" in d.columns:
            d["葉色（5段階）"] = pd.to_numeric(d["葉色（5段階）"], errors="coerce")
        return (
            d.groupby("品種", dropna=False)
            .agg(
                records=("日付", "count"),
                total_yield_g=("収穫重量 (g)", "sum"),
                mean_yield_g=("収穫重量 (g)", "mean"),
                mean_leaf_color=("葉色（5段階）", "mean"),
            )
            .reset_index()
        )
    return d


def summarize_kale_worktime(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    return (
        d.groupby(["品種", "作業内容"], dropna=False)
        .agg(total_minutes=("所要時間（分）", "sum"))
        .reset_index()
        .sort_values("total_minutes", ascending=False)
    )


def summarize_herb_growth(df: pd.DataFrame) -> pd.DataFrame:
    return df.copy()


def summarize_herb_worktime(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    return (
        d.groupby(["品種", "作業内容"], dropna=False)
        .agg(total_minutes=("所要時間（分）", "sum"))
        .reset_index()
        .sort_values("total_minutes", ascending=False)
    )


# =========================================================
# extended figures
# =========================================================
def plot_58_59_per_plant(df_59: pd.DataFrame, out_path: Path) -> None:
    ref58 = build_58_reference_table()[["期", "品種_base", "株当たり収量_g"]]
    s59 = build_59_variety_summary(df_59)[["期", "品種_base", "株当たり収量_g"]]
    comp = pd.concat([ref58, s59], ignore_index=True)

    pivot = comp.pivot(index="品種_base", columns="期", values="株当たり収量_g")
    ax = pivot.plot(kind="bar", figsize=(10, 6), rot=0)
    ax.set_title("58期 vs 59期 株当たり収量")
    ax.set_xlabel("品種")
    ax.set_ylabel("株当たり収量 (g/株)")
    ax.legend(title="期")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close()


def plot_z_vs_other(df: pd.DataFrame, out_png: Path) -> None:
    d = add_plant_counts(df)
    d = d[d["期"] == 59].copy()

    # Z / 上段 / 下段 を実験区、それ以外を企業棟に寄せる
    def to_group(house_no):
        s = str(house_no).strip()
        if s in {"Z", "実験棟", "Z棟", "上段", "下段"}:
            return s if s in {"上段", "下段"} else "Z"
        return "企業棟"

    d["区分"] = d["ハウスNo"].map(to_group)

    agg = (
        d.groupby(["品種_base", "区分"], dropna=False)
        .agg(
            総収量_g=("収量", "sum"),
            株数=("株数", "max"),
        )
        .reset_index()
    )

    agg["株当たり収量_g"] = agg["総収量_g"] / agg["株数"]
    agg = agg.dropna(subset=["株当たり収量_g"]).copy()

    if agg.empty:
        return

    pivot = agg.pivot(index="品種_base", columns="区分", values="株当たり収量_g")

    if pivot.empty:
        return

    pivot = pivot.apply(pd.to_numeric, errors="coerce")
    pivot = pivot.dropna(how="all")

    if pivot.empty:
        return

    ax = pivot.plot(kind="bar", figsize=(12, 6))
    ax.set_title("59期 品種別 株当たり収量（企業棟 vs Z/上段/下段）")
    ax.set_ylabel("株当たり収量 (g/株)")
    ax.set_xlabel("")
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(out_png, dpi=200, bbox_inches="tight")
    plt.close()

def plot_peak_yield(timing_df: pd.DataFrame, out_path: Path) -> None:
    d = timing_df.copy()
    d = d[d["期"].isin([58, 59])].copy()
    if d.empty:
        return

    pivot = d.pivot(index="品種_base", columns="期", values="ピーク日収量_g")
    ax = pivot.plot(kind="bar", figsize=(10, 6), rot=0)
    ax.set_title("ピーク日収量比較")
    ax.set_xlabel("品種")
    ax.set_ylabel("ピーク日収量 (g)")
    ax.legend(title="期")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close()


def plot_first_harvest_day(timing_df: pd.DataFrame, out_path: Path) -> None:
    d = timing_df.copy()
    d = d[d["期"].isin([58, 59])].copy()
    if d.empty:
        return

    base = d[["期", "品種_base", "初収穫日"]].copy()
    base["初収穫日_ordinal"] = base["初収穫日"].map(pd.Timestamp.toordinal)

    pivot = base.pivot(index="品種_base", columns="期", values="初収穫日_ordinal")
    ax = pivot.plot(kind="bar", figsize=(10, 6), rot=0)
    ax.set_title("初収穫日比較")
    ax.set_xlabel("品種")
    ax.set_ylabel("初収穫日（相対比較）")
    ax.legend(title="期")
    plt.tight_layout()
    plt.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close()


def plot_worktime_top(df: pd.DataFrame, path: Path) -> None:
    work = df.copy()
    work.columns = [str(c).strip() for c in work.columns]

    if "作業内容" not in work.columns or "total_minutes" not in work.columns:
        print(f"[WARN] plot_worktime_top: columns not found -> {work.columns.tolist()}")
        return

    work = work[["作業内容", "total_minutes"]].copy()
    work["total_minutes"] = pd.to_numeric(work["total_minutes"], errors="coerce")
    work = work.dropna(subset=["作業内容", "total_minutes"])

    if work.empty:
        return

    top = (
        work.groupby("作業内容", dropna=False)["total_minutes"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .sort_values(ascending=True)
    )

    plt.figure(figsize=(8, 5))
    plt.barh(top.index.astype(str), top.values)
    plt.title("作業時間 上位10")
    plt.xlabel("分")
    plt.tight_layout()
    plt.savefig(path, dpi=200, bbox_inches="tight")
    plt.close()

def plot_kale_worktime(df: pd.DataFrame, path: Path) -> None:
    if "作業内容" not in df.columns or "所要時間（分）" not in df.columns:
        print(f"[WARN] plot_kale_worktime: columns not found -> {df.columns.tolist()}")
        return

    work = df.copy()
    work["所要時間（分）"] = pd.to_numeric(work["所要時間（分）"], errors="coerce")
    work = work.dropna(subset=["作業内容", "所要時間（分）"])

    if work.empty:
        return

    agg = (
        work.groupby("作業内容", dropna=False)["所要時間（分）"]
        .sum()
        .sort_values(ascending=False)
    )

    plt.figure(figsize=(8, 5))
    plt.bar(agg.index.astype(str), agg.values)
    plt.title("ケール 作業時間まとめ")
    plt.ylabel("分")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(path, dpi=200, bbox_inches="tight")
    plt.close()

def plot_daily_yield(df: pd.DataFrame, outdir: Path):

    daily = build_daily_yield(df)

    for ki in daily["期"].unique():

        sub = daily[daily["期"] == ki]

        plt.figure(figsize=(10,6))

        for v in sub["品種_base"].unique():
            s = sub[sub["品種_base"] == v]
            plt.plot(s["日付"], s["収量"], label=v)

        plt.title(f"Daily Yield {ki}期")
        plt.xlabel("Date")
        plt.ylabel("Yield g")
        plt.legend()
        plt.tight_layout()

        plt.savefig(outdir / f"daily_yield_{ki}.png")
        plt.close()



def plot_daily_yield_z(df: pd.DataFrame, outdir: Path):

    d = df[df["ハウスNo"] == "Z"].copy()

    d["日付"] = pd.to_datetime(d["日付"], errors="coerce")

    daily = (
        d.groupby(["期","品種_base","日付"])["収量"]
        .sum()
        .reset_index()
    )

    for ki in daily["期"].unique():

        sub = daily[daily["期"] == ki]

        plt.figure(figsize=(10,6))

        for v in sub["品種_base"].unique():
            s = sub[sub["品種_base"] == v]
            plt.plot(s["日付"], s["収量"], label=v)

        plt.title(f"Z house Daily Yield {ki}")
        plt.legend()
        plt.tight_layout()

        plt.savefig(outdir / f"daily_yield_Z_{ki}.png")
        plt.close()

def plot_kale_growth(df: pd.DataFrame, path: Path) -> None:
    work = df.copy()
    work.columns = [str(c).strip() for c in work.columns]

    if "品種" not in work.columns:
        print(f"[WARN] plot_kale_growth: 品種列なし -> {work.columns.tolist()}")
        return

    value_col = None
    for c in ["mean_yield_g", "収穫重量 (g)", "株高（㎝）"]:
        if c in work.columns:
            value_col = c
            break

    if value_col is None:
        print(f"[WARN] plot_kale_growth: value column not found -> {work.columns.tolist()}")
        return

    work[value_col] = pd.to_numeric(work[value_col], errors="coerce")
    work = work.dropna(subset=["品種", value_col])

    if work.empty:
        return

    plt.figure(figsize=(8, 5))
    plt.bar(work["品種"].astype(str), work[value_col])
    plt.title(f"ケール 品種別平均{value_col}")
    plt.ylabel(value_col)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(path, dpi=200, bbox_inches="tight")
    plt.close()


def plot_herb_growth(df, path):

    plt.figure(figsize=(8,5))

    plt.bar(df["品種"], df["平均草丈"])

    plt.title("Herb Growth")
    plt.ylabel("Height cm")

    plt.tight_layout()
    plt.savefig(path)
    plt.close()

def plot_worktime(df, path):

    top = df.sort_values("時間", ascending=False).head(10)

    plt.figure(figsize=(8,5))

    plt.barh(top["作業"], top["時間"])

    plt.title("Work Time Top Tasks")

    plt.tight_layout()
    plt.savefig(path)
    plt.close()

def plot_sales(df: pd.DataFrame, path: Path) -> None:
    work = df.copy()
    work.columns = [str(c).strip() for c in work.columns]

    item_col = None
    qty_col = None

    for c in ["品目", "品種"]:
        if c in work.columns:
            item_col = c
            break

    for c in ["数量", "販売数", "販売個数（袋・P）"]:
        if c in work.columns:
            qty_col = c
            break

    if item_col is None or qty_col is None:
        print(f"[WARN] plot_sales: columns not found -> {work.columns.tolist()}")
        return

    work = work[[item_col, qty_col]].copy()
    work = work.dropna(subset=[item_col, qty_col])
    work[qty_col] = pd.to_numeric(work[qty_col], errors="coerce")
    work = work.dropna(subset=[qty_col])

    if work.empty:
        return

    agg = (
        work.groupby(item_col, dropna=False)[qty_col]
        .sum()
        .sort_values(ascending=False)
    )

    plt.figure(figsize=(8, 5))
    plt.bar(agg.index.astype(str), agg.values)
    plt.title("販売数量まとめ")
    plt.ylabel("数量")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(path, dpi=200, bbox_inches="tight")
    plt.close()

# =========================================================
# orchestration
# =========================================================
def run_extended_analysis(
    harvest_csv: Path,
    sales_xlsx: Path,
    work_xlsx: Path,
    kale_growth_xlsx: Path,
    kale_work_xlsx: Path,
    herb_growth_xlsx: Path,
    herb_work_xlsx: Path,
    out_dir: Path,
) -> None:
    ensure_dir(out_dir)
    fig_dir = out_dir / "figures_ext"
    ensure_dir(fig_dir)

    print("[DEBUG] work_xlsx =", work_xlsx)
    print("[DEBUG] kale_growth_xlsx =", kale_growth_xlsx)
    print("[DEBUG] kale_work_xlsx =", kale_work_xlsx)

    harvest = load_harvest_cleaned(harvest_csv)

    z_stage = summarize_stage_z_only(harvest)
    house_group = summarize_house_group(harvest)
    timing = build_harvest_timing_table(harvest)
    speed = build_monthly_speed_table(harvest)
    sales_est = estimate_sales_from_harvest(harvest)
    ref58 = build_58_reference_table()
    s59 = build_59_variety_summary(harvest)

    sales_actual = pd.DataFrame()
    if sales_xlsx.exists():
        sales_actual = load_sales_summary_sheet(sales_xlsx) 

    work_by_task = pd.DataFrame()
    work_by_month_task = pd.DataFrame()
    if work_xlsx.exists():
        work_raw = load_worktime_excel(work_xlsx)
        work_by_task, work_by_month_task = summarize_worktime(work_raw)

    if work_xlsx.exists():
        work_raw = load_worktime_excel(work_xlsx)
        print("[DEBUG] work_raw columns =", work_raw.columns.tolist())
        work_by_task, work_by_month_task = summarize_worktime(work_raw)
        print("[DEBUG] work_by_task columns =", work_by_task.columns.tolist(), "rows=", len(work_by_task))

    kale_growth_summary = pd.DataFrame()
    kale_work_summary = pd.DataFrame()
    if kale_growth_xlsx.exists():
        kale_growth = load_kale_growth_excel(kale_growth_xlsx)
        kale_growth_summary = summarize_kale_growth(kale_growth)
    if kale_work_xlsx.exists():
        kale_work = load_kale_worktime_excel(kale_work_xlsx)
        kale_work_summary = summarize_kale_worktime(kale_work)

    herb_growth_summary = pd.DataFrame()
    herb_work_summary = pd.DataFrame()
    if herb_growth_xlsx.exists():
        herb_growth = load_herb_growth_excel(herb_growth_xlsx)
        herb_growth_summary = summarize_herb_growth(herb_growth)
    #if herb_work_xlsx.exists():
    #    herb_work = load_herb_worktime_excel(herb_work_xlsx)
    #    herb_work_summary = summarize_herb_worktime(herb_work)

    print("[DEBUG] sales_actual", sales_actual.shape, sales_actual.columns.tolist())
    print("[DEBUG] work_by_task", work_by_task.shape, work_by_task.columns.tolist())
    print("[DEBUG] kale_growth_summary", kale_growth_summary.shape, kale_growth_summary.columns.tolist())
    print("[DEBUG] kale_work_summary", kale_work_summary.shape, kale_work_summary.columns.tolist())

    with pd.ExcelWriter(out_dir / "extended_analysis.xlsx", engine="openpyxl") as writer:
        ref58.to_excel(writer, sheet_name="ref_58_variety", index=False)
        s59.to_excel(writer, sheet_name="summary_59_variety", index=False)
        z_stage.to_excel(writer, sheet_name="z_stage_summary", index=False)
        house_group.to_excel(writer, sheet_name="house_group_summary", index=False)
        timing.to_excel(writer, sheet_name="harvest_timing", index=False)
        speed.to_excel(writer, sheet_name="harvest_speed", index=False)
        sales_est.to_excel(writer, sheet_name="sales_estimate", index=False)
        sales_actual.to_excel(writer, sheet_name="sales_actual", index=False)
        work_by_task.to_excel(writer, sheet_name="work_by_task", index=False)
        work_by_month_task.to_excel(writer, sheet_name="work_by_month_task", index=False)
        kale_growth_summary.to_excel(writer, sheet_name="kale_growth_summary", index=False)
        kale_work_summary.to_excel(writer, sheet_name="kale_work_summary", index=False)
        herb_growth_summary.to_excel(writer, sheet_name="herb_growth_summary", index=False)
        herb_work_summary.to_excel(writer, sheet_name="herb_work_summary", index=False)

    plot_daily_yield_z(harvest, fig_dir)
    plot_daily_yield(harvest, fig_dir)
    plot_58_59_per_plant(harvest, fig_dir / "01_58vs59_per_plant.png")
    plot_z_vs_other(harvest, fig_dir / "02_z_vs_other_per_plant.png")
    plot_first_harvest_day(timing, fig_dir / "03_first_harvest_day.png")
    plot_peak_yield(timing, fig_dir / "04_peak_yield.png")
    if not work_by_task.empty:
        plot_worktime_top(work_by_task, fig_dir / "05_worktime_top.png")
    
    if not kale_growth_summary.empty:
        plot_kale_growth(kale_growth_summary, fig_dir / "06_kale_growth.png")

    if not kale_work_summary.empty:
        plot_kale_worktime(kale_work_summary, fig_dir / "07_kale_worktime.png")

    # herb はデータ少ないため今回は図化しない

    if not work_by_task.empty:
        plot_worktime_top(work_by_task, fig_dir / "08_worktime.png")

    if not sales_actual.empty:
        plot_sales(sales_actual, fig_dir / "09_sales.png")


    print(f"saved: {out_dir / 'extended_analysis.xlsx'}")
    print(f"saved figures: {fig_dir}")


def main() -> None:
    setup_matplotlib_font()

    if not HARVEST_PATH.exists():
        raise FileNotFoundError(f"not found: {HARVEST_PATH}")
    if not ENV_PATH.exists():
        raise FileNotFoundError(f"not found: {ENV_PATH}")

    try:
        raw = pd.read_csv(HARVEST_PATH, encoding="utf-8-sig")
    except UnicodeDecodeError:
        raw = pd.read_csv(HARVEST_PATH, encoding="cp932")

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

    harvest.to_csv(cleaned_path, index=False, encoding="utf-8-sig")
    joined.to_csv(joined_path, index=False, encoding="utf-8-sig")

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

    generate_presentation_figures(harvest, FIG_DIR)

    hit_rate = joined["env_hit"].mean() if ("env_hit" in joined.columns and len(joined)) else float("nan")
    print(f"saved: {cleaned_path}")
    print(f"saved: {joined_path}")
    print(f"saved: {xlsx_path}")
    print(f"saved figures: {FIG_DIR}")
    print(f"rows_harvest={len(harvest)} rows_joined={len(joined)} env_hit_rate={hit_rate:.3f}")


if __name__ == "__main__":
    main()

    run_extended_analysis(
        harvest_csv=REPORTS / "harvest_master_cleaned.csv",
        sales_xlsx=SALES_XLSX,
        work_xlsx=WORK_XLSX,
        kale_growth_xlsx=KALE_GROWTH_XLSX,
        kale_work_xlsx=KALE_WORK_XLSX,
        herb_growth_xlsx=HERB_GROWTH_XLSX,
        herb_work_xlsx=HERB_WORK_XLSX,
        out_dir=EXT_DIR,
    )
