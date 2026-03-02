import __future__ import annotations

import re
import unicodedata
import pathlib import Path

import pandas as pd

# ====== 設定 ======
INV_XLSX = Path("YOUR_59KI_WIDE.xlsx") #←ここを実ファイル名に変更
SHEET = 0 # シート名が分かれば "Sheet1" 等に変更
OUT_CSV　= Path("datasets/59ki_master_long.csv")

VARIETY_COLS = ["紅ほっぺ", "よつぼし", "かおり野", "やよいひめ"]

def norm_text(x: object) -> str | None:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = unicodedata.normalize("NFKC", str(x)).strip()
    s = re.sub(r"\s+", "", s) # 改行/スペース除去
    return s or None

def norm_company(s: str | None) -> str | None:
    s = norm_text(s)
    if not s:
        return None
    # 表記揺れ統一（必要な分だけ追加）
    s = s.replace("アドビ", "Adobe")
    return s

def norm_house(s: str | None) -> str | None:
    s = norm_text(s)
    if not s:
        return None
    # "B２" → "B2" / "B４" → "B4" などは norm_text でほぼ揃う
    return s

def norm_tier(s: str | None) -> str | None:
    s = norm_text(s)
    if not s:
        return None
    # "中段" などが来たら "中" に寄せる
    s = s.replace("上段", "上").replace("中段", "中").replace("下段", "下")
    return s

def main():
    df = pd.read_excel(IN_XLSX, sheet_name=SHEET)
    df.columns = [unicodedata.normalize("NFKC", str(c)).strip() for c in df.columns]

    # 必須列チェック（列名が微妙に違う場合はここで合わせる）
    rename = {
        "日付": "date",
        "企業名": "company",
        "段": "tier",
        "ハウスNo": "house",
    }
    df = df.rename(columns=rename)

    missing = [c for c in ["date", "company", "tier", "house"] if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns] {missing}. Current cols={list(df.columns)}")

    # 日付
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # 正規化
