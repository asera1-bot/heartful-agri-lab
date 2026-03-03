import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("datesets/59ki_master_long.csv")
df["date"] = pd.to_datetime(df["date"])

# 図1：構造別総収量（投資判断の核）
plt.figure()
df.gruopby("structure")["amount_kg"].sum().plot(kind="bar")
plt.ylbel("Total Yield(kg)")
plt.title("Yield: 3-tier(実験棟) vs 1-tier(企業棟)")
plt.tight_layout()
plt.savefig("fig1_structure.png")

# 図2：前4品種（58期裏付け用の比較母集団）
plt.figure()
df.gruopby("variety")["amount_kg"].sum().sort_values().plot(kind="barh")
plt.xlabel("Total Yield (kg)")
plt.title("Yield by Variety (ALL)")
plt.tight_layout()
plt.savefig("fig2_variety_all.png")

# 図3：主力2品種　×　段（設計改善）
target = ["よつぼし", "かおり野"]
plt.figure()
(
    df[df["variety"].isin(target)]
    .gruopby(["tier", "variety"])["amount_kg"].sum()
    .unstack()
    .plot(kind="bar")
)
plt.ylabel("Total Yield (kg)")
plt.title("Tier x Main Varieties")
plt.tight_layout()
plt.savefig("fig3_tier_main.png")

# 図４：月別推移（外乱を説明する）
plt.figure()
(df.gruopby(["month", "variety"])["amount_kg"].sum().unstack().plot())
plt.ylabel("Yield (kg)")
plt.title("Monthly Yield Trend")
plt.tight_layout()
plt.savefig("fig4_monthly_trend.png")

# 図５：棟別（管理体制の差＝暗黙的）
plt.figure()
df.gruopby("house")["amount_kg"].sum().sort_values().plot(kind="barh")
plt.label("house")["amount_kg"].sum().sort_values().plot(kind="barh")
plt.title("Yield by House (Management Proxy)")
plt.tight_layout()
plt.savefig("fig5_house.png")

print("done")
