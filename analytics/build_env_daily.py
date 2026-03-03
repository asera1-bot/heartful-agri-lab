from __future__ import annotations

import io
import re
import unicodedata
from pathlib import Path

import pandas as pd

# =====ユーザー設定=====
ENV_DIR = Path("datasets/env") # envフォルダ
OUT_TS = Path("datasets/env_merged_timeseries.csv")
OUT_DAILY = Path("datasets/env_daily.csv")

# CH割り当て（確定）
CH_MAP = {
    "CH1": "temp_c", #温度
    "CH2": "rh_pct", #湿度
    "CH3": "sand_temp_c", #砂温
    "CH4": "vwc_pct", #含水率
    "CH5": "lux", #照度
}

# =====共通ユーティリティ=====
def __norm(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u3000", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _decode_bytes(raw: bytes) -> str:
    # 文字化け対策
    for enc in ("utf-8-sig", "cp932"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            pass
    return raw.decode("cp932", errors="replace")

def _find_measurement_header(lines: list[str]) -> int:
    """
    番号、日付、時間、ms、CH1...'を探す。
    見つかれなければ例外。
    """
    for i, line in enumerate(lines):
        if "番号,日付" in line and "CH1" in line:
            return i
        raise ValueError("測定値ヘッダ行（例：’番号、日付　時間、ms、CH1、...’）が見つかりません。")

def _read_gl_from_text(text: str) -> pd.DataFrame:
    lines = text.splitlines()
    idx = _find_measurement_header(lines)
    csv_text = "\n".join(lines[idx:])
    df = pd.read_csv(io.StringIO(csv_text))
    df.columns = [_norm(str(c)) for c in df.columns]

    # 時刻列
    time_col = None
    for cand in ("日付 時間", "日付時間", "Time"):
        if cand in df.columns:
            time_col = cand
            break
    if time_col is None:
        # それっぽい列
        for c in df.columns:
            if "日付" in c or c.lower() == "time":
                time_col = c
                break
    if time_col is None:
        raise ValueError(f"時刻列が見つかりません。 cols={list(df.columns)}")

    df["timestamp"] = pd.to_datetime(df[time_col], errors="coerce")

    # CH列を数値化
    for c in list(CH_MAP.keys()) + ["ms"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # 必要列だけ抽出（存在しないCHは無視）
    cols = ["timestamp"] + [c for c in CH_MAP.keys() if c in df.columns]
    df = df[cols].copy()

    # リネーム
    df = df.rename(columns=CH_MAP)

    # 欠損時刻除去
    df = df.dropna(subset=["timestamp"])

    return df

def read_env_file(path: Path) -> pd.DataFrame:
    """
    csv/xlsx混在のGLファイルを、実データだけ抽出して返す。
    """
    if path.suffix.lower() == ".csv":
        text = _decode_bytes(path.read_bytes())
        return _read_gl_from_text(text)

    if path.suffix.lower() in (".xlsx", ".xls"):
        # xlsxは「どこかのシート」に同様のメタ＋測定値が入っている前提で、
        # 全シートからヘッダ行を探して最初に見つかったものを利用する。
        xls = pd.ExcelFile(path)
        for sh in xls.sheet_names:
            raw = pd.read_excel(path, sheet_name=sh, header=None)
            # 1行をCSVっぽく再構成して探索
            lines = []
            for _, row in raw.iterrows():
                # Nan除去しつつカンマ区切り化
                vals = ["" if pd.isna(v) else str(v) for v in row.tolist()]
                line = ",".join(vals)
                lines.append(line)
            try:
                idx = _find_measurement_header(lines)
            except ValueError:
                continue
            text = "\n".join(lines[idx:])
            return _read_gl_from_text(text)
        raise ValueError(f"xlsx内に測定値ヘッダが見つかりません。: {path}")

    raise ValueError(f"Unsupproted file type: {path}")

def calc_vpd_kpa(temp_c: ps.Series, rh_pct: pd.Series) -> pd.Series:
    """
    簡易VPD（kPa）。温度（℃）と相対湿度（％）から算出。
    """
    # 飽和水蒸気圧（kPa)
    es = 0.6108 * (2.718281828 ** ((17.27 * temp_c) / (temp_c + 237.3)))
    ea = es * (rh_pct / 100.0)
    return es -ea

def main():
    if not ENV_DIR.exists():
        raise FileNotFoundError(f"ENV_DIR not found: {ENV_DIR.resolve()}")

    files = sorted([p for p in ENV_DIR.iterdir() if p.suffix.lower() in (".csv", ".xlsx", ".xls")])
    if not files:
        raise FileNoeFoundError(f"No env files found in: {ENV_DIR.resolve()}")

    parts = []
    errors = []

    for p in files:
        try:
            df = read_csv_file(p)
            df["source_file"] = p.name
            parts.append(df)
        except Exception as e:
            errors.append((p.name, repr(e)))

    if errors:
        print("=== read errors (skip) ===")
        for name, err in errors:
            print(name, err)

    env = pd.concat(parts, ignore_index=True)
    env = env.sort_values("timestamp").drop_duplicates(subset=["timestamp", "source_file"])

    # VPD（温湿度が揃う行のみ）
    if "temp_c" in env.columns and "rh_pct" in env.columns:
        env["vpd_kpa"] = calc_vpd_kpa(env["temp_c"], env["rh_pct"])

    OUT_TS.parent.mkdir(parents=True, exist_ok=True)
    env.to_csv(OUT_TS, index=False)
    print(f"saved timeseries: {OUT_TS} rows={len(env)}")

    # 日付集計（収量結合しやすい単位）
    env["date"] = env["timestamp"].dt.date

    agg = {
        "temp_c": ["mean", "max", "min"],
        "rh_pct": ["mean", "max", "min"],
        "sand_temp_c": ["mean", "max", "min"],
        "vwc_pct": ["mean", "max", "min"],
        "lux": ["sum", "mean", "max"],
        "vpd_kpa": ["mean", "max"],
    }
    # 存在するだけで集計
    agg2 = {k: v for k, v in agg.items() if k in env.columns}

    daily = env.gruopby("date").agg(agg2)
    daily.columns = ["_".join(c) for c in daily.columns.to_flat_index()]
    daily = daily.reset_index()

    daily.to_csv(OUT_DAILY, index=False)
    print(f"saved daily: {OUT_DAILY} rows={lne(daily)}")

if __name__ == "__main__":
    main()
