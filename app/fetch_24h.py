import os, requests
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("CWA_TOKEN")
STATION_ID = os.getenv("STATION_ID")
if not TOKEN or not STATION_ID:
    raise RuntimeError("請在 .env 設定 CWA_TOKEN 與 STATION_ID")

# 1) 取得「最近很多小時」的索引清單
meta_url = "https://opendata.cwa.gov.tw/historyapi/v1/getMetadata/O-A0001-001"
meta = requests.get(meta_url, params={"Authorization": TOKEN}, timeout=30).json()
times = meta["dataset"]["resources"]["resource"]["data"]["time"]  # 清單，內含 DateTime / ProductURL
times = sorted(times, key=lambda t: t["DateTime"])

# 2) 只取最近 24 小時
last_24 = times[-24:]

rows = []
for item in last_24:
    when = item["DateTime"]
    print(".", end="", flush=True)
    # ProductURL 內已帶 ?Authorization=...，改成自己帶參數避免把 token 印出到日誌
    url = item["ProductURL"]
    if "format=" not in url:
        url = url + "&downloadType=WEB&format=JSON"

    try:
        r = requests.get(url, timeout=30, headers={"Accept": "application/json"})
        r.raise_for_status()
    except requests.HTTPError as e:
        # 該小時檔尚未生成 → 404 就跳過
        if getattr(e.response, "status_code", None) == 404:
            continue
        raise

    # 優先解析 JSON，不行就用 XML 後援
    try:
        j = r.json()
    except Exception:
        import xmltodict
        j = xmltodict.parse(r.text)

    # 兼容不同包裝：優先走 records.Station，否則嘗試 cwaopendata.dataset.Station
    stations = (j.get("records") or {}).get("Station")
    if stations is None:
        stations = (j.get("cwaopendata") or {}).get("dataset", {}).get("Station", [])
    if isinstance(stations, dict):  # 有時候只回一筆會是 dict
        stations = [stations]

    # 找到你的測站
    for s in stations:
        if s.get("StationId") == STATION_ID:
            we = s.get("WeatherElement", {}) or {}
            now = we.get("Now") if isinstance(we.get("Now"), dict) else {}
            rows.append({
                "DateTime": when,
                "Temperature": pd.to_numeric(we.get("AirTemperature"), errors="coerce"),
                "RH": pd.to_numeric(we.get("RelativeHumidity"), errors="coerce"),
                "Precip": pd.to_numeric((now or {}).get("Precipitation"), errors="coerce"),
            })
            break

df = pd.DataFrame(rows).sort_values("DateTime")

# 第45–48行（新增四行）
df["DateTime"] = pd.to_datetime(df["DateTime"])
end = df["DateTime"].max()
start = end - pd.Timedelta(hours=23)
df = df[df["DateTime"].between(start, end)].sort_values("DateTime")
print("筆數：", len(df))
print(df.tail(5).to_string(index=False))
print()

# --- plot 24h Temperature / RH / Precip ---
fig, axs = plt.subplots(3, 1, figsize=(11, 8), sharex=True)

axs[0].plot(df["DateTime"], df["Temperature"])
axs[0].set_ylabel("Temp (°C)")
axs[0].set_title("Taichung Dali — Last 24h")

axs[1].plot(df["DateTime"], df["RH"])
axs[1].set_ylabel("RH (%)")

# 雨量用柱狀，缺值視為 0 方便觀察
axs[2].bar(df["DateTime"], df["Precip"].fillna(0))
axs[2].set_ylabel("Rain (mm)")
axs[2].set_xlabel("Local time")

for ax in axs:
    ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig("web/dali_last24h.png", dpi=300)
print("Saved: web/dali_last24h.png")
