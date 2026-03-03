from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Optional

import pandas as pd


# =====ユーザー設定=====
ENV_DIR = Path("datasets/env")  # envフォルダ
OUT_TS = Path("datasets/env_merged_timeseries.csv")
OUT_DAILY = Path("datasets/env_daily.csv")

# CH割り当て（確定）
CH_MAP = {
    "CH1": "temp_c",       # 温度
    "CH2": "rh_pct",       # 湿度
    "CH3": "sand_temp_c",  # 砂温
    "CH4": "vwc_pct",      # 含水率
    "CH5": "lux",          # 照度（元は W/m2 の可能性あり。ここではそのまま lux列名として扱う）
}


# =====共通ユーティリティ=====
def _norm(s: str) -> str:
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
    CSVテキストから測定値ヘッダ行（番号,日付 時間,ms,CH1,...）を探す。
    見つからなければ例外。
    """
    for i, line in enumerate(lines):
        s = _norm(line)
        # カンマ区切り想定（Converted.csv）
        if "番号" in s and "日付" in s and "CH1" in s:
            return i
        if "NO." in s and "Time" in s and "CH1" in s:
            return i
    raise ValueError("測定値ヘッダ行が見つかりません（CSV）")


def _read_gl_from_text(text: str) -> pd.DataFrame:
    """
    GL240 Converted.csv のようなテキストから測定テーブルを抽出してDataFrame化。
    """
    lines = text.splitlines()
    idx = _find_measurement_header(lines)

    # ヘッダ行
    header = lines[idx]
    cols = [c.strip() for c in header.split(",")]

    # 単位行（NO./Time/ﾟC/% ...）が直後にあるので、次行が単位っぽいならスキップ
    start = idx + 1
    if start < len(lines):
        unit = _norm(lines[start]).lower()
        if ("no." in unit) or ("time" in unit) or ("ﾟc" in unit) or ("w/m2" in unit) or ("%" in unit):
            start = idx + 2

    data = "\n".join(lines[start:])
    df = pd.read_csv(
        pd.io.common.StringIO(data),
        names=cols,
        header=None,
        engine="python",
    )

    # 日付時刻列の統一
    if "日付 時間" in df.columns:
        df = df.rename(columns={"日付 時間": "datetime"})
    elif "Time" in df.columns:
        df = df.rename(columns={"Time": "datetime"})
    else:
        raise ValueError("datetime列（'日付 時間' または 'Time'）が見つかりません（CSV）")

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])

    # CH列の取り出し（CH1..CH5）
    keep = ["datetime"] + [ch for ch in CH_MAP.keys() if ch in df.columns]
    df = df[keep].copy()

    return df


def _read_xlsx_env(path: Path) -> pd.DataFrame:
    """
    GL240 Converted.xlsx を読む。
    - Sheet1 があれば優先（列: 日付 時間, 温度, 湿度, 砂温, 含水率, 照度）
    - なければ先頭シート（Converted）でヘッダ行（番号/日付 時間/CH1..）を探索
    """
    xl = pd.ExcelFile(path)
    sheets = xl.sheet_names

    # 1) Sheet1があるなら優先（列が整列済み）
    if "Sheet1" in sheets:
        df = pd.read_excel(path, sheet_name="Sheet1")
        df.columns = [str(c).strip() for c in df.columns]

        need = {"日付 時間", "温度", "湿度", "砂温", "含水率", "照度"}
        if need.issubset(set(df.columns)):
            out = df.rename(
                columns={
                    "日付 時間": "datetime",
                    "温度": "CH1",
                    "湿度": "CH2",
                    "砂温": "CH3",
                    "含水率": "CH4",
                    "照度": "CH5",
                }
            ).copy()
            out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
            out = out.dropna(subset=["datetime"])
            return out[["datetime", "CH1", "CH2", "CH3", "CH4", "CH5"]]

    # 2) Convertedシートを生読みしてヘッダ検出
    target = sheets[0]
    raw = pd.read_excel(path, sheet_name=target, header=None)

    def row_text(i: int) -> str:
        r = raw.iloc[i].astype(str).fillna("")
        return "|".join([x for x in r.tolist() if x and x != "nan"])

    header_i: Optional[int] = None
    for i in range(min(len(raw), 500)):
        t = row_text(i)
        if ("日付 時間" in t) and ("CH1" in t) and ("番号" in t):
            header_i = i
            break

    if header_i is None:
        raise ValueError(f"xlsx内に測定値ヘッダが見つかりません。: {path}")

    # 単位行をスキップ（L29: NO.|Time|ms|ﾟC|%... みたいな行）
    unit_i = header_i + 1
    data_start = header_i + 1
    if unit_i < len(raw):
        unit_line = row_text(unit_i).lower()
        if ("no." in unit_line) or ("time" in unit_line) or ("ﾟc" in unit_line) or ("w/m2" in unit_line) or ("%" in unit_line):
            data_start = header_i + 2

    # ヘッダ行を列名として再読込し、先頭のメタ領域を飛ばす
    df = pd.read_excel(path, sheet_name=target, header=header_i)
    df.columns = [str(c).strip() for c in df.columns]

    # メタ領域が混ざっている場合があるので data_start より上は落とす
    if data_start > header_i + 1:
        # header指定で読んだ場合、行番号がずれるので、datetimeがNaNの先頭部分を落とす方式にする
        pass

    # 必要列抽出
    if "日付 時間" in df.columns:
        out = df.rename(columns={"日付 時間": "datetime"}).copy()
    elif "Time" in df.columns:
        out = df.rename(columns={"Time": "datetime"}).copy()
    else:
        raise ValueError(f"datetime列がありません: {path} cols={list(df.columns)}")

    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    out = out.dropna(subset=["datetime"])

    keep = ["datetime", "CH1", "CH2", "CH3", "CH4", "CH5"]
    for k in keep[1:]:
        if k not in out.columns:
            raise ValueError(f"必須列 {k} がありません: {path} cols={list(out.columns)}")

    return out[keep].copy()


def read_env_file(path: Path) -> pd.DataFrame:
    """
    envファイル（csv/xlsx混在）を読み、datetime+CH1..CH5 のDataFrameを返す。
    """
    suf = path.suffix.lower()

    if suf in (".xlsx", ".xls", ".xlsm"):
        return _read_xlsx_env(path)

    if suf == ".csv":
        raw = path.read_bytes()
        text = _decode_bytes(raw)
        return _read_gl_from_text(text)

    raise ValueError(f"unsupported file: {path}")


def calc_vpd_kpa(temp_c: pd.Series, rh_pct: pd.Series) -> pd.Series:
    """
    簡易VPD（kPa）。温度（℃）と相対湿度（％）から算出。
    """
    # 飽和水蒸気圧（kPa）
    es = 0.6108 * (2.718281828 ** ((17.27 * temp_c) / (temp_c + 237.3)))
    ea = es * (rh_pct / 100.0)
    return es - ea


def main() -> None:
    if not ENV_DIR.exists():
        raise FileNotFoundError(f"ENV_DIR not found: {ENV_DIR.resolve()}")

    # _Converted.* を優先して拾う（混在時に安全）
    files = sorted(
        set(ENV_DIR.glob("*_Converted.csv")) |
        set(ENV_DIR.glob("*_Converted.xlsx")) |
        set(ENV_DIR.glob("*.csv")) |
        set(ENV_DIR.glob("*.xlsx"))
    )

    if not files:
        raise FileNotFoundError(f"No env files found in: {ENV_DIR.resolve()}")

    parts: list[pd.DataFrame] = []
    errors: list[tuple[str, str]] = []

    for p in files:
        try:
            df = read_env_file(p)
            df["source_file"] = p.name
            parts.append(df)
            print("[OK]", p.name, "rows=", len(df))
        except Exception as e:
            errors.append((p.name, repr(e)))
            print("[NG]", p.name, repr(e))

    if not parts:
        raise ValueError("No objects to concatenate (all files failed). See errors above.")

    env = pd.concat(parts, ignore_index=True)

    # CH列を統一名へ
    for ch, new in CH_MAP.items():
        if ch in env.columns:
            env[new] = pd.to_numeric(env[ch], errors="coerce")

    env = env.rename(columns={"datetime": "timestamp"})
    env["timestamp"] = pd.to_datetime(env["timestamp"], errors="coerce")
    env = env.dropna(subset=["timestamp"])

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

    # 存在する列だけで集計
    agg2 = {k: v for k, v in agg.items() if k in env.columns}

    daily = env.groupby("date").agg(agg2)
    daily.columns = ["_".join(c) for c in daily.columns.to_flat_index()]
    daily = daily.reset_index()

    daily.to_csv(OUT_DAILY, index=False)
    print(f"saved daily: {OUT_DAILY} rows={len(daily)}")


if __name__ == "__main__":
    main()
