import os, csv, json, requests, pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

TOKEN = os.environ["CWA_TOKEN"]               # 來自 GitHub Secrets
DAYS  = int(os.getenv("EXPORT_DAYS", "30"))   # 匯出幾天到 JSON（預設30）
MAX_HOURS_PER_RUN = int(os.getenv("HOURS_PER_RUN", "168"))  # 每次補抓多少小時

session = requests.Session()
session.headers.update({"Accept": "application/json", "Accept-Encoding": "gzip"})

def last_hours_list(hours=168):
    end_ts = pd.Timestamp.now(tz="Asia/Taipei").floor("h")
    return list(pd.date_range(end=end_ts, periods=hours, freq="h"))

def fetch_one_hour(dt, sid):
    url = (f"https://opendata.cwa.gov.tw/historyapi/v1/getData/O-A0001-001/"
           f"{dt:%Y/%m/%d/%H/00/00}?Authorization={TOKEN}&downloadType=WEB&format=JSON")
    try:
        r = session.get(url, timeout=10); r.raise_for_status()
    except requests.HTTPError as e:
        if getattr(e.response, "status_code", None) == 404:
            return None
        raise
    try:
        j = r.json()
    except Exception:
        import xmltodict
        j = xmltodict.parse(r.text)

    stations = (j.get("records") or {}).get("Station")
    if stations is None:
        stations = (j.get("cwaopendata") or {}).get("dataset", {}).get("Station", [])
    if isinstance(stations, dict):
        stations = [stations]

    for s in stations:
        if s.get("StationId") == sid:
            we = s.get("WeatherElement") or {}
            now = we.get("Now") if isinstance(we.get("Now"), dict) else {}
            return {
                "DateTime": dt.isoformat(),
                "Temperature": pd.to_numeric(we.get("AirTemperature"), errors="coerce"),
                "RH": pd.to_numeric(we.get("RelativeHumidity"), errors="coerce"),
                "Precip": pd.to_numeric((now or {}).get("Precipitation"), errors="coerce"),
            }
    return None

def update_station(sid, city, town, name):
    os.makedirs("data", exist_ok=True)
    os.makedirs("docs/data", exist_ok=True)

    # 抓近 MAX_HOURS_PER_RUN 小時並行補資料
    rows = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = [ex.submit(fetch_one_hour, dt, sid) for dt in last_hours_list(MAX_HOURS_PER_RUN)]
        for fut in as_completed(futs):
            rec = fut.result()
            if rec: rows.append(rec)

    df_new = pd.DataFrame(rows)
    cache_path = f"data/{sid}_hourly.csv"
    if os.path.exists(cache_path):
        old = pd.read_csv(cache_path, parse_dates=["DateTime"])
        df_all = pd.concat([old, df_new], ignore_index=True)
    else:
        df_all = df_new.copy()

    df_all["DateTime"] = pd.to_datetime(df_all["DateTime"])
    df_all = df_all.drop_duplicates(subset=["DateTime"]).sort_values("DateTime")
    df_all.to_csv(cache_path, index=False, encoding="utf-8-sig")

    # 匯出 JSON（最近 DAYS 天）
    end = df_all["DateTime"].max()
    start = end - pd.Timedelta(days=DAYS)
    df_out = df_all[df_all["DateTime"].between(start, end)].copy()

    records = [{
        "t": d.isoformat(),
        "temp": None if pd.isna(t) else float(t),
        "rh": None if pd.isna(h) else float(h),
        "rain": 0.0 if pd.isna(p) else float(p),
    } for d, t, h, p in zip(df_out["DateTime"], df_out["Temperature"], df_out["RH"], df_out["Precip"])]

    # === 新增：三個視圖的「未來時段佔位」 ==============================
    now = pd.Timestamp.now(tz="Asia/Taipei").floor("h")

    def future_hours(n):
        return [(now + pd.Timedelta(hours=i)).isoformat() for i in range(1, n+1)]

    forecast = {
        "24h": [{"t": t, "temp": None, "rh": None} for t in future_hours(8)],
        "7d":  [{"t": (now + pd.Timedelta(hours=i)).isoformat(), "temp": None, "rh": None}
                for i in range(1, 3*24 + 1)],
        "30d": [{"t": (now + pd.Timedelta(hours=i)).isoformat(), "temp": None, "rh": None}
                for i in range(1, 7*24 + 1)],
    }
    # ================================================================

    payload = {
        "station": sid,
        "city": city, "town": town, "name": name,
        "generated_at": pd.Timestamp.utcnow().isoformat() + "Z",
        "series": records,
        "forecast": forecast,
    }
    out_path = f"docs/data/{sid}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    return out_path, df_out["DateTime"].max()

def main():
    # 讀站點名冊
    stations = []
    with open("app/stations.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            stations.append(row)

    index = []
    for s in stations:
        sid = s["sid"]; city=s["city"]; town=s["town"]; name=s["name"]
        out_path, last_ts = update_station(sid, city, town, name)
        print(f"[ok] {sid} → {out_path}（最新：{last_ts}）")
        index.append({"sid": sid, "city": city, "town": town, "name": name, "latest": str(last_ts)})

    # 產生索引給前端下拉選單用
    with open("docs/data/index.json", "w", encoding="utf-8") as f:
        json.dump({"stations": index}, f, ensure_ascii=False)

if __name__ == "__main__":
    main()
