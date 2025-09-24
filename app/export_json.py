import os, json, pandas as pd
from dotenv import load_dotenv

load_dotenv()
STATION_ID = os.getenv("STATION_ID")
if not STATION_ID:
    raise RuntimeError("請在 .env 設定 STATION_ID")

# 參數：輸出幾天資料（預設 30 天）
DAYS = int(os.getenv("EXPORT_DAYS", "30"))

cache_path = f"data/{STATION_ID}_hourly.csv"
out_dir = "web/data"; os.makedirs(out_dir, exist_ok=True)
out_path = f"{out_dir}/{STATION_ID}.json"

if not os.path.exists(cache_path):
    raise FileNotFoundError(f"找不到快取檔：{cache_path}（先跑 fetch_7d.py 累積一下）")

df = pd.read_csv(cache_path, parse_dates=["DateTime"]).sort_values("DateTime")
end = df["DateTime"].max()
start = end - pd.Timedelta(days=DAYS)
df = df[df["DateTime"].between(start, end)].copy()

records = []
for _, row in df.iterrows():
    records.append({
        "t": pd.to_datetime(row["DateTime"]).isoformat(),
        "temp": None if pd.isna(row["Temperature"]) else float(row["Temperature"]),
        "rh":   None if pd.isna(row["RH"])         else float(row["RH"]),
        "rain": 0.0  if pd.isna(row["Precip"])     else float(row["Precip"]),
    })

payload = {
    "station": STATION_ID,
    "generated_at": pd.Timestamp.utcnow().isoformat() + "Z",
    "series": records
}

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False)

print(f"已輸出 {len(records)} 筆 → {out_path}")
