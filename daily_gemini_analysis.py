import os
import requests
import json
import io
import pdfplumber
from datetime import datetime, timedelta
from google import genai

# --- 1. 核心配置 (已根据您提供的链接修正) ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# 目标：分析报告存入的数据库 ID
DB_TARGET = "32747eb5fd3c800e9c3bfe9b461aab94" 

# 数据湖目录
RAW_DIR = "data/raw"
SUMMARY_DIR = "data/summary"
for d in [RAW_DIR, SUMMARY_DIR]:
    os.makedirs(d, exist_ok=True)

# 来源数据库配置 (ID 已修正)
DB_CONFIG = {
    "CFTC": {"id": "2c747eb5fd3c808186ddd0aeb45d5046", "file_col": "File & media", "date_col": "Date"},
    "OI": {"id": "2fc47eb5fd3c8035ab22cabf3e6e41bb", "file_col": "File", "date_col": "Date"},
    "GOLD_INV": {"id": "2bc47eb5fd3c8083966eecfd9f396b44", "date_col": "Gold日期", "reg_col": "Gold Reg库存"},
    "SILVER_INV": {"id": "2bc47eb5fd3c80f3a71ad8de149a4943", "date_col": "Silver日期", "reg_col": "Silver Reg库存"},
    "PT_INV": {"id": "2d647eb5fd3c801a9ce5d5db4d0b961a", "date_col": "Pt日期", "reg_col": "Pt Reg库存"}
}

client = genai.Client(api_key=GEMINI_API_KEY)
HEADERS = {"Authorization": f"Bearer {NOTION_TOKEN}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}

# --- 2. 物理数据抓取与存档逻辑 ---

def fetch_and_save_raw_files():
    """全量拉取 30 天内 PDF 并存入 GitHub 文件夹"""
    print(">>> 正在启动物理数据同步任务...")
    new_files = 0
    past_date = (datetime.utcnow() - timedelta(days=30)).isoformat()
    
    for db_type in ["CFTC", "OI"]:
        cfg = DB_CONFIG[db_type]
        url = f"https://api.notion.com/v1/databases/{cfg['id']}/query"
        payload = {"filter": {"property": cfg["date_col"], "date": {"on_or_after": past_date}}}
        
        try:
            res = requests.post(url, headers=HEADERS, json=payload).json()
            rows = res.get("results", [])
            print(f"    - 数据库 [{db_type}] 发现 {len(rows)} 条近期记录")
            
            for row in rows:
                props = row["properties"]
                date_str = props[cfg["date_col"]]["date"]["start"]
                files = props[cfg["file_col"]]["files"]
                
                if not files: continue
                
                local_txt = f"{RAW_DIR}/{db_type}_{date_str}.txt"
                if os.path.exists(local_txt): continue # 跳过已有文件
                
                file_url = files[0]["external"]["url"] if files[0]["type"] == "external" else files[0]["file"]["url"]
                print(f"      √ 解析中: {date_str} 的 PDF...")
                
                f_res = requests.get(file_url, timeout=30)
                with pdfplumber.open(io.BytesIO(f_res.content)) as pdf:
                    # 抓取前 10 页文本并进行关键词清洗
                    full_text = "\n".join([p.extract_text() for p in pdf.pages[:10] if p.extract_text()])
                    keywords = ["GOLD", "SILVER", "PLATINUM", "COMMERCIAL", "NON-COMMERCIAL", "OPEN INTEREST"]
                    cleaned = [l for l in full_text.split('\n') if any(k in l.upper() for k in keywords)]
                    
                    with open(local_txt, "w", encoding="utf-8") as f:
                        f.write("\n".join(cleaned))
                new_files += 1
        except Exception as e:
            print(f"    × 数据库 [{db_type}] 请求异常: {e}")
            
    print(f">>> 数据湖同步结束，本次物理存档新增 {new_files} 个文件。")

def get_lake_summary():
    """读取本地所有的 txt 汇总给 Gemini"""
    lake_data = []
    all_files = sorted(os.listdir(RAW_DIR))
    for fn in all_files:
        if fn.endswith(".txt"):
            with open(f"{RAW_DIR}/{fn}", "r", encoding="utf-8") as f:
                lake_data.append({"Source": fn, "Content": f.read()[:5000]})
    return lake_data

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

# --- 3. 分析与回写逻辑 ---

def write_to_notion_page(report):
    print(">>> 正在创建 Notion 审计页面...")
    jst_today = (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d")
    payload = {
        "parent": {"database_id": DB_TARGET},
        "properties": {"Name": {"title": [{"text": {"content": f"30D深度审计 (数据湖模式): {jst_today}"}}]}, "Date": {"date": {"start": jst_today}}}
    }
    res = requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=payload).json()
    page_id = res.get("id")
    if page_id:
        chunks = [report[i:i+1900] for i in range(0, len(report), 1900)]
        blocks = [{"object":"block","type":"paragraph","paragraph":{"rich_text":[{"text":{"content":c}}]}} for c in chunks]
        requests.patch(f"https://api.notion.com/v1/blocks/{page_id}/children", headers=HEADERS, json={"children": blocks})

def main():
    fetch_and_save_raw_files()
    summary = get_lake_summary()
    inv = fetch_inventory()
    
    prompt = f"""
    分析数据湖数据：
    历史汇总(持仓/OI): {json.dumps(summary[-15:])}
    实时库存: {json.dumps(inv)}
    
    任务：
    - 为 GOLD, SILVER, PT 分别建立 30 天量化表。
    - 表格列：日期 | 非商业Net | 商业Net | 当月OI | 下月OI | 下下月OI | Reg库存 | 压力比率。
    """
    
    print(">>> 正在请求 Gemini 3.1 Pro 解析本地数据湖...")
    response = client.models.generate_content(model='gemini-3.1-pro-preview', contents=prompt)
    write_to_notion_page(response.text)

if __name__ == "__main__":
    main()
