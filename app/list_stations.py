import os, requests
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("CWA_TOKEN")
URL = "https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0001-001"

params = {"Authorization": TOKEN}
r = requests.get(URL, params=params, timeout=30)
r.raise_for_status()
stations = r.json().get("records", {}).get("Station", [])

city, town = "臺中市", "大里區"  # 先固定，之後再做成參數
found = [(s.get("StationId"), s.get("StationName"))
         for s in stations
         if (s.get("GeoInfo") or {}).get("CountyName")==city
         and (s.get("GeoInfo") or {}).get("TownName")==town]

print(f"{city} {town} 測站列表：")
for sid, name in found:
    print(f"- {name} / StationID={sid}")
