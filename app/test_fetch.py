import os, requests, pandas as pd
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("CWA_TOKEN")

# 逐時觀測（全台測站最新整點）
URL_OA0001 = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0001-001"
params = {"Authorization": TOKEN}
r = requests.get(URL_OA0001, params=params, timeout=30)
r.raise_for_status()
data = r.json()

# 官方 JSON 的結構會放在 data["records"]["Station"]（若未來有異動，再依實際鍵名調整）
stations = data["records"]["Station"]

# 先用台中市／大里區篩選（CountyName / TownName 來自官方欄位）
rows = []
for s in stations:
    geo = s.get("GeoInfo", {})
    if geo.get("CountyName") == "臺中市" and geo.get("TownName") == "大里區":
        we = s.get("WeatherElement", {})
        # 取溫度、濕度；降水量可能在 O-A0001 的 Now.Precipitation，格式要清理
        temp = we.get("AirTemperature")
        rh   = we.get("RelativeHumidity")
        # 有些站的降水可能在 RainfallElement 底下（不同資料集），先抓 O-A0001 的當日降水欄位試試
        prec = we.get("Now", {}).get("Precipitation") if isinstance(we.get("Now"), dict) else None
        rows.append({
            "StationId": s.get("StationID"),
            "StationName": s.get("StationName"),
            "Time": s.get("ObsTime", {}).get("DateTime"),
            "Temperature": pd.to_numeric(temp, errors="coerce"),
            "RH": pd.to_numeric(rh, errors="coerce"),
            "Precip": None if prec in ["T","X","-99","-98"] else pd.to_numeric(prec, errors="coerce")
        })

df = pd.DataFrame(rows)
print(df.head())  # 確認有抓到台中市大里區的資料
