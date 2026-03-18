import os
import requests
import json
import io
import pdfplumber
from datetime import datetime, timedelta
from google import genai

# --- 1. 配置 ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DB_TARGET = "32747eb5fd3c800e9c3bfe9b461aab94" 

RAW_DIR = "data/raw"
SUMMARY_DIR = "data/summary"
for d in [RAW_DIR, SUMMARY_DIR]:
    os.makedirs(d, exist_ok=True)

client = genai.Client(api_key=GEMINI_API_KEY)
HEADERS = {"Authorization": f"Bearer {NOTION_TOKEN}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}

DB_CONFIG = {
    "CFTC": {"id": "2dc47eb5fd3c80e48d59e0d46494752e", "file_col": "Files & media", "date_col": "Date"},
    "OI": {"id": "2f947eb5fd3c8068b034c7ebc74b88d0", "file_col": "File", "date_col": "Date"},
    "GOLD_INV": {"id": "2bc47eb5fd3c8083966eecfd9f396b44", "date_col": "Gold日期", "reg_col": "Gold Reg库存"},
    "SILVER_INV": {"id": "2bc47eb5fd3c80f3a71ad8de149a4943", "date_col": "Silver日期", "reg_col": "Silver Reg库存"},
    "PT_INV": {"id": "2d647eb5fd3c801a9ce5d5db4d0b961a", "date_col": "Pt日期", "reg_col": "Pt Reg库存"}
}

# --- 2. 核心：强制抓取与存档逻辑 ---

def fetch_and_save_raw_files():
    """强制从 Notion 抓取 30 天内所有 PDF 并存入 GitHub"""
    print(">>> 启动 30 天数据全量拉取任务...")
    new_files = 0
    # 时间窗口扩大到 30 天
    past_date = (datetime.utcnow() - timedelta(days=30)).isoformat()
    
    for db_type in ["CFTC", "OI"]:
        cfg = DB_CONFIG[db_type]
        url = f"https://api.notion.com/v1/databases/{cfg['id']}/query"
        payload = {"filter": {"property": cfg["date_col"], "date": {"on_or_after": past_date}}}
        
        res = requests.post(url, headers=HEADERS, json=payload).json()
        rows = res.get("results", [])
        print(f"    - 数据库 [{db_type}] 发现 {len(rows)} 条近期记录")

        for row in rows:
            date_str = row["properties"][cfg["date_col"]]["date"]["start"]
            files = row["properties"][cfg["file_col"]]["files"]
            if not files: continue
            
            local_txt = f"{RAW_DIR}/{db_type}_{date_str}.txt"
            # 如果本地文件不存在，或者文件大小极小，则重新抓取
            if os.path.exists(local_txt) and os.path.getsize(local_txt) > 100:
                continue 
            
            file_url = files[0]["external"]["url"] if files[0]["type"] == "external" else files[0]["file"]["url"]
            print(f"    - 正在同步并解析: {db_type} | {date_str} ...")
            try:
                f_res = requests.get(file_url, timeout=30)
                if f_res.status_code == 200:
                    with pdfplumber.open(io.BytesIO(f_res.content)) as pdf:
                        # 抓取前 10 页文本
                        content = "\n".join([p.extract_text() for p in pdf.pages[:10] if p.extract_text()])
                        if content:
                            with open(local_txt, "w", encoding="utf-8") as f:
                                f.write(content) # 第一次先存全量文本，确保 data 文件夹不为空
                            new_files += 1
            except Exception as e:
                print(f"      × 失败: {e}")
    
    print(f">>> 物理文件同步完成，本次新增 {new_files} 个本地文本。")

def update_master_json():
    """读取 raw 里的所有 txt，清洗出金银铂数据，生成汇总 JSON"""
    print(">>> 正在清洗数据并更新 master_data.json...")
    master_list = []
    all_dates = set()
    
    # 获取 raw 目录下所有日期
    for fn in os.listdir(RAW_DIR):
        if "_" in fn: all_dates.add(fn.split("_")[1].replace(".txt", ""))
    
    for d in sorted(list(all_dates)):
        entry = {"Date": d}
        # 寻找对应的 CFTC 和 OI 文件
        for t in ["CFTC", "OI"]:
            path = f"{RAW_DIR}/{t}_{d}.txt"
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    txt = f.read().upper()
                    # 简单清洗：只提取包含三大金属名称的行
                    lines = [l for l in txt.split("\n") if any(k in l for k in ["GOLD", "SILVER", "PLATINUM"])]
                    entry[t] = "\n".join(lines)[:8000] # 截断
        master_list.append(entry)

    with open(f"{SUMMARY_DIR}/master_data.json", "w", encoding="utf-8") as f:
        json.dump(master_list, f, ensure_ascii=False, indent=2)

def fetch_inventory():
    """获取库存数据"""
    inv = {}
    for m in ["GOLD", "SILVER", "PT"]:
        cfg = DB_CONFIG[f"{m}_INV"]
        res = requests.post(f"https://api.notion.com/v1/databases/{cfg['id']}/query", headers=HEADERS, 
                            json={"filter": {"property": cfg["date_col"], "date": {"on_or_after": (datetime.utcnow()-timedelta(days=30)).isoformat()}}}).json()
        for r in res.get("results", []):
            dt = r["properties"][cfg["date_col"]]["date"]["start"]
            num = r["properties"][cfg["reg_col"]]["number"]
            if dt not in inv: inv[dt] = {}
            inv[dt][f"{m}_Reg"] = num
    return inv

# --- 3. 结果回写 ---

def write_to_notion_page(report):
    print(">>> 正在向 Notion 写入审计报告...")
    jst_today = (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d")
    payload = {
        "parent": {"database_id": DB_TARGET},
        "properties": {"Name": {"title": [{"text": {"content": f"30D 物理审计报告: {jst_today}"}}]}, "Date": {"date": {"start": jst_today}}}
    }
    res = requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=payload).json()
    page_id = res.get("id")
    if page_id:
        chunks = [report[i:i+1900] for i in range(0, len(report), 1900)]
        blocks = [{"object":"block","type":"paragraph","paragraph":{"rich_text":[{"text":{"content":c}}]}} for c in chunks]
        requests.patch(f"https://api.notion.com/v1/blocks/{page_id}/children", headers=HEADERS, json={"children": blocks})

# --- 4. 运行 ---

def main():
    fetch_and_save_raw_files()
    update_master_json()
    
    with open(f"{SUMMARY_DIR}/master_data.json", "r") as f:
        m_data = json.load(f)
    inv_data = fetch_inventory()
    
    prompt = f"""
    分析以下源数据：
    1. 历史汇总(持仓/OI): {json.dumps(m_data[-15:])}
    2. 实时库存: {json.dumps(inv_data)}
    
    任务：
    - 为 GOLD, SILVER, PT 分别建立 30 天量化表。
    - 表格列：日期 | 非商业Net | 商业Net | 当月OI | 下月OI | 下下月OI | Reg库存 | 压力比率。
    """
    
    print(">>> 正在请求 Gemini 3.1 Pro 审计数据湖...")
    response = client.models.generate_content(model='gemini-3.1-pro-preview', contents=prompt)
    write_to_notion_page(response.text)

if __name__ == "__main__":
    main()
