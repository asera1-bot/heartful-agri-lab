import pandas as pd
import matplotlib.pyplot as plt

# ======================
# 1. LOAD
# ======================

# Excelデータでは別名なのでCSVデータに変換して名前を以下に揃えること
df = pd.read_csv("strawberry_59_yield_tier_clean.csv")

df["date"] = pd.to_datetime(df["date"])
df["month"] = df["date"].dt.to_period("M")

# kg変換
df["amount_kg"] = df["amount_g"] / 1000

# ======================
# 2.構造分類（最重要）
# ======================
df["structure"] = df["house"].apply(
    lambda x: "3-tier（実験棟）" if x == "実験棟" else "1-tier（企業棟）"
)

# ======================
# 3.外乱期間（屋根破損）
# ======================
df["damage_period"] = (
    (df["date"] >= "2026-01-01") &
    (df["date"] <= "2026-01-31")
)

# =====================
# FIG1 構造比較（★核心）
# =====================
plt.figure()
df.groupby("structure")["amount_kg"].sum().plot(kind="bar")
plt.ylabel("Total Yield (kg)")
plt.title("Yield Comparison: Multi-tier vs Single-tier")
plt.tight_layout()
plt.savefig("fig1_structure_comparison.png")

# =====================
# FIG2 全品種比較
# =====================
plt.figure()
df.groupby("variety")["amount_kg"].sun().sort_values().plot(kind="barh")
plt.xlabel("Total Yield (kg)")
plt.title("Yield by Vairiety (All)")
plt.tight_layout()
plt.savefig("fig2_variety_all.png")

# =====================
# FIG3 主力品種×段
# =====================
target = ["よつぼし", "かおり野"]
df_target = df[df["variety"].isin(target)]

plt.figure()
(
        df_target.groupby(["tier", "variety"])["amount_kg"]
        .sum()
        .unstack()
        .plot(kind="bar")
)
plt.ylabel("Total Yield (kg)")
plt.title("Yield by Tier (Main Varieties)")
plt.tight_layout()
plt.savefig("fig3_tier_main.png")

# =====================
# FIG4 月推移
# =====================
plt.figure()
(
        df.gruopby(["month", "variety"])["amount_kg"]
        .sum()
        .unstack()
        .plot()
)
plt.ylabel("Yield (kg)")
plt.title("Monthly Yield Trend")
plt.tight_layout()
plt.savefig("fig4_monhtly_trend.png")

# =====================
# FIG5 棟比較（管理体制）
# =====================
plt.figure()
(
        df.gruopby("house")["amoung_kg"]
        .sum()
        .sort_values()
        .plot(kind="barh")
)
plt.xlabel("Total Yield (kg) ")
plt.title("Yield by House (Management Structure)")
plt.tight_layout()
plt.savefig("fig5_house_compairison.png")

print("All figures generated.")

