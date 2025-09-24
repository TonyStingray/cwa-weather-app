import os, requests, pprint
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("CWA_TOKEN")

# 1) 列出支援短期過去資料的 dataId 清單
url_ids = "https://opendata.cwa.gov.tw/historyapi/v1/getDataId/"
r1 = requests.get(url_ids, params={"Authorization": TOKEN}, timeout=30)
r1.raise_for_status()
ids = r1.json()                                   # 可能是 ['O-A0001-001', ...]
id_list = [x["dataId"] if isinstance(x, dict) else x for x in ids]
print("是否包含 O-A0001-001：", "O-A0001-001" in id_list)

# 2) 看看 O-A0001-001 的 metadata（會告訴你可用參數/時間欄位等）
url_meta = "https://opendata.cwa.gov.tw/historyapi/v1/getMetadata/O-A0001-001"
r2 = requests.get(url_meta, params={"Authorization": TOKEN}, timeout=30)
r2.raise_for_status()
print("\nO-A0001-001 metadata（節選）:")
pprint.pp(r2.json())
