import pandas as pd

path = "datasets/test/イチゴ/59期_実験棟_収量x環境_daily.xlsx"
df = pd.read_excel(path, sheet_name="joined")

print(df[["品種","段"]].drop_duplicates().sort_values(["品種","段"]))
