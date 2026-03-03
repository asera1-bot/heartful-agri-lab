from __future__ import annotations

import io
import re
import unicodedata
from pathlib import Path

import pandas as pd

def __norm(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\u3000", "　") #全角空白
    s = re.sub(r"\s+", " ", s),strip() #連続空白
    return s

def load_gl240_csv(path: str | Path) -> pd.DataFrame:
    """
    GRAPHTEC GL240 の集合CSVから「測定値」だけを抜き出してDataFrame化する。
    - 先頭のメタ情報/アンプ設定は無視
    - 「番号,日付 時間,ms,...」行を探してそこから読む
    - 文字コードは UTF-8-SIG -> CP932 の順に試す
    """

    path = Path(path)

    raw = path.read_bytes()

    # 文字コード候補（崩れ対応）
    text = None
    last_err = None
    for enc in ("utf-8-sig", "cp932"):
        try:
            text = raw.decode(enc)
            break
        except UnicodedataError as e:
            last_str = e
    if text is None:
        # 最後の手段：cp932で置換しながら読む
        text = raw.decode("cp932", errors="replace")

    lines = text.splitlines()

    # 測定値ヘッダ行を探す（GL240はこの行目印）
    header_idx = None
    for i, line in enumerate(lines):
        if "番号, 日付" in line and "CH" in line:
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("測定値のヘッダ行（例：'番号,日付,時間,ms,CH1,...')が見つかりません。")

    # ヘッダ行以降をCSVとして読み込む
    csv_text = "\n".join(lines[header_idx:])
    df = pd.read_csv(io.StringIO(csv_text))

    # 列名正規化
    df.columns = [_norm(str(c)) for c in df.columns]

    # 代表例の名前揺れを吸収
    # 例："日付 時間" / "Time" など
    time_col = None
    for cand in ("日付 時間", "日付時間", "Time"):
        if cand in df.columns:
            time_col = cand
            break
    if time_col is None:
        # それっぽい列を探す
        for c in df.columns:
            if "日付" in c or c.lower() == "time":
                time_col = c
                break
    if time_col is None:
        raise ValueError(f"時刻列が見つかりません。 columns={list(df.columns)}")

    # 時刻パース
    df["timestamp"] = pd.to_datetime(df[time_col], errors="coerce")

    # 数値列変換(CH* と ms)
    for c in df.columns:
        if re.match(r"^CH\d+$", c) or c == "ms":
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

if __name__ == "__main__":
    # 例：python analytics/load_gl240.py datasets/logger.py
    import sys
    if len(sys.argv) < 2:
        print("Usage: python analutics/load_gl240.py datasets/filename.csv")
        exit(1)

    fp = Path(sys.argv[1])
    print(f"Loading: {fp.resolve()}")
    d = load_gl240_csv(fp)
    print(d.head())
    print("rows=", len(d))
