import os, requests, pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import matplotlib.pyplot as plt
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("CWA_TOKEN")
STATION_ID = os.getenv("STATION_ID")
if not TOKEN or not STATION_ID:
    raise RuntimeError("請在 .env 設定 CWA_TOKEN 與 STATION_ID")

# 建立可重用連線，開 gzip 減少流量
session = requests.Session()
session.headers.update({"Accept": "application/json", "Accept-Encoding": "gzip"})

# 1) 直接組「過去 168 小時」清單（不依賴 metadata）
end_ts = pd.Timestamp.now(tz="Asia/Taipei").floor("h")
last_hours = list(pd.date_range(end=end_ts, periods=168, freq="h"))

def fetch_one(dt):
    """下載單一小時資料，回傳該測站的三欄位；不存在就回 None。"""
    url = (
    f"https://opendata.cwa.gov.tw/historyapi/v1/getData/O-A0001-001/"
    f"{dt:%Y/%m/%d/%H/00/00}?Authorization={TOKEN}&downloadType=WEB&format=JSON"
    )
    try:
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
    except requests.HTTPError as e:
        # 該小時尚未產出 → 404 就跳過
        if getattr(e.response, "status_code", None) == 404:
            return None
        raise

    # 優先 JSON，不行就 XML
    try:
        j = resp.json()
    except Exception:
        import xmltodict
        j = xmltodict.parse(resp.text)

    stations = (j.get("records") or {}).get("Station")
    if stations is None:
        stations = (j.get("cwaopendata") or {}).get("dataset", {}).get("Station", [])
    if isinstance(stations, dict):
        stations = [stations]

    for s in stations:
        if s.get("StationId") == STATION_ID:
            we = s.get("WeatherElement", {}) or {}
            now = we.get("Now") if isinstance(we.get("Now"), dict) else {}
            return {
                "DateTime": dt.isoformat(),
                "Temperature": pd.to_numeric(we.get("AirTemperature"), errors="coerce"),
                "RH": pd.to_numeric(we.get("RelativeHumidity"), errors="coerce"),
                "Precip": pd.to_numeric((now or {}).get("Precipitation"), errors="coerce"),
            }
    return None

# 3) 輕量並行下載，加速
rows = []
with ThreadPoolExecutor(max_workers=6) as ex:
    futs = [ex.submit(fetch_one, t) for t in last_hours]
    for fut in as_completed(futs):
        r = fut.result()
        if r:
            rows.append(r)

df = pd.DataFrame(rows)
# === 本地快取累積（新增） ===
os.makedirs("data", exist_ok=True)
cache_path = f"data/{STATION_ID}_hourly.csv"
if os.path.exists(cache_path):
    old = pd.read_csv(cache_path, parse_dates=["DateTime"])
    all_df = pd.concat([old, df], ignore_index=True)
else:
    all_df = df.copy()

all_df["DateTime"] = pd.to_datetime(all_df["DateTime"])
all_df = all_df.drop_duplicates(subset=["DateTime"]).sort_values("DateTime")
all_df.to_csv(cache_path, index=False, encoding="utf-8-sig")

# 清整 & 時間窗鎖定到「最新往回 7 天」
end = all_df["DateTime"].max()
start = end - pd.Timedelta(days=7)
df7 = all_df[all_df["DateTime"].between(start, end)].copy()

print("7天筆數：", len(df7))
print(df7.tail(5).to_string(index=False))
if df7.empty:
    raise RuntimeError("快取尚未累積到 7 天資料；請稍後再跑或檢查 STATION_ID。")

# 4) 繪圖（3 個子圖，共用時間軸）
fig, axs = plt.subplots(3, 1, figsize=(12, 9), sharex=True)
axs[0].plot(df7["DateTime"], df7["Temperature"])
axs[0].set_ylabel("Temp (°C)")
axs[0].set_title(f"Taichung Dali — {df7['DateTime'].min():%m-%d %H:%M} ~ {df7['DateTime'].max():%m-%d %H:%M}")

axs[1].plot(df7["DateTime"], df7["RH"])
axs[1].set_ylabel("RH (%)")

axs[2].bar(df7["DateTime"], df7["Precip"].fillna(0))
axs[2].set_ylabel("Rain (mm)")
axs[2].set_xlabel("Local time")

for ax in axs:
    ax.grid(True, alpha=0.3)

plt.tight_layout()
out_path = "docs/dali_last7d.png"
plt.savefig(out_path, dpi=150)
print("Saved:", out_path)
