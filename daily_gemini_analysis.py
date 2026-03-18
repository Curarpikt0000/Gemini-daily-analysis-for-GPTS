import os
import requests
import json
import io
import pdfplumber
import re
from datetime import datetime, timedelta
from google import genai

# --- 1. 配置 ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
RAW_DIR = "data/raw"
SUMMARY_FILE = "data/summary/master_data.json"

for d in [RAW_DIR, "data/summary"]:
    if not os.path.exists(d): os.makedirs(d)

client = genai.Client(api_key=GEMINI_API_KEY)
HEADERS = {"Authorization": f"Bearer {NOTION_TOKEN}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}

DB_CONFIG = {
    "CFTC": {"id": "2dc47eb5fd3c80e48d59e0d46494752e", "file_col": "Files & media", "date_col": "Date"},
    "OI": {"id": "2f947eb5fd3c8068b034c7ebc74b88d0", "file_col": "File", "date_col": "Date"},
    "GOLD_INV": {"id": "2bc47eb5fd3c8083966eecfd9f396b44", "date_col": "Gold日期", "reg_col": "Gold Reg库存"},
    "SILVER_INV": {"id": "2bc47eb5fd3c80f3a71ad8de149a4943", "date_col": "Silver日期", "reg_col": "Silver Reg库存"},
    "PT_INV": {"id": "2d647eb5fd3c801a9ce5d5db4d0b961a", "date_col": "Pt日期", "reg_col": "Pt Reg库存"}
}

# --- 2. 核心：增量同步与清洗 ---

def fetch_and_save_raw_files():
    """从 Notion 抓取 PDF 并存入 data/raw"""
    print(">>> 正在检查 Notion 中的新文件...")
    for db_type in ["CFTC", "OI"]:
        cfg = DB_CONFIG[db_type]
        url = f"https://api.notion.com/v1/databases/{cfg['id']}/query"
        res = requests.post(url, headers=HEADERS, json={"filter": {"property": cfg["date_col"], "date": {"on_or_after": (datetime.utcnow()-timedelta(days=14)).isoformat()}}}).json()
        
        for row in res.get("results", []):
            date_str = row["properties"][cfg["date_col"]]["date"]["start"]
            files = row["properties"][cfg["file_col"]]["files"]
            if not files: continue
            
            local_path = f"{RAW_DIR}/{db_type}_{date_str}.txt"
            if os.path.exists(local_path): continue # 已存在则跳过
            
            print(f">>> 发现新数据 {date_str}，正在解析 PDF...")
            file_url = files[0]["external"]["url"] if files[0]["type"] == "external" else files[0]["file"]["url"]
            try:
                pdf_data = requests.get(file_url).content
                with pdfplumber.open(io.BytesIO(pdf_data)) as pdf:
                    text = "\n".join([p.extract_text() for p in pdf.pages[:10] if p.extract_text()])
                    with open(local_path, "w", encoding="utf-8") as f: f.write(text)
            except Exception as e: print(f"解析失败: {e}")

def update_master_summary():
    """汇总 data/raw 里的所有文本，提取 Gold/Silver/PT 关键信息"""
    print(">>> 正在生成汇总汇总 master_data.json...")
    master_data = {}
    
    # 逻辑：遍历 raw 文件夹
    for filename in sorted(os.listdir(RAW_DIR)):
        if not filename.endswith(".txt"): continue
        date_part = filename.split("_")[1].replace(".txt", "")
        if date_part not in master_data: master_data[date_part] = {"Date": date_part}
        
        with open(f"{RAW_DIR}/{filename}", "r", encoding="utf-8") as f:
            content = f.read()
            # 这里的清洗逻辑交给 Gemini，我们将文本切片存入汇总
            # 只保留包含关键金属字段的段落，减少存储压力
            keywords = ["GOLD", "SILVER", "PLATINUM", "COMMERCIAL", "NON-COMMERCIAL"]
            relevant_lines = [line for line in content.split("\n") if any(k in line.upper() for k in keywords)]
            master_data[date_part][filename.split("_")[0]] = "\n".join(relevant_lines)[:5000]

    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump(list(master_data.values()), f, ensure_ascii=False, indent=2)

def fetch_notion_inventory():
    """获取库存数值数据"""
    inv_data = {}
    for metal in ["GOLD", "SILVER", "PT"]:
        cfg = DB_CONFIG[f"{metal}_INV"]
        url = f"https://api.notion.com/v1/databases/{cfg['id']}/query"
        res = requests.post(url, headers=HEADERS, json={"filter": {"property": cfg["date_col"], "date": {"on_or_after": (datetime.utcnow()-timedelta(days=30)).isoformat()}}}).json()
        for row in res.get("results", []):
            d = row["properties"][cfg["date_col"]]["date"]["start"]
            v = row["properties"][cfg["reg_col"]]["number"]
            if d not in inv_data: inv_data[d] = {}
            inv_data[d][f"{metal}_Reg"] = v
    return inv_data

# --- 3. 页面写入逻辑 (同前) ---
def write_to_notion(report):
    url = "https://api.notion.com/v1/pages"
    today = (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d")
    payload = {
        "parent": {"database_id": DB_TARGET},
        "properties": {"Name": {"title": [{"text": {"content": f"30D深度审计 (数据湖驱动): {today}"}}]}, "Date": {"date": {"start": today}}},
        "children": [{"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"text": {"content": chunk}}]}} for chunk in [report[i:i+1900] for i in range(0, len(report), 1900)]]
    }
    requests.post(url, headers=HEADERS, json=payload)

# --- 4. 主流程 ---
def main():
    # A. 同步与清洗
    fetch_and_save_raw_files()
    update_master_summary()
    
    # B. 读取汇总数据
    with open(SUMMARY_FILE, "r") as f: master_summary = json.load(f)
    inventory = fetch_notion_inventory()
    
    # C. 调用分析
    prompt = f"""
    作为顶级量化专家，请根据 GitHub 汇总数据湖进行【30天逐日交叉审计】。
    
    【源数据区】：
    历史持仓与OI汇总: {json.dumps(master_summary[-10:])} # 取最近10条历史记录
    实时库存序列: {json.dumps(inventory)}
    
    【任务】：
    1. 为 GOLD, SILVER, PT 分别建立 30 天量化表。
    2. 包含：日期 | 非商业Net | 商业Net | 当月OI | 下月OI | 下下月OI | Reg库存 | 压力比率。
    3. 识别头寸与库存的背离，评估逼空风险。
    """
    
    print(">>> 正在请求 Gemini 3.1 Pro 分析汇总数据...")
    res = client.models.generate_content(model='gemini-3.1-pro-preview', contents=prompt)
    write_to_notion(res.text)

if __name__ == "__main__":
    main()
