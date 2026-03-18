import os
import requests
import json
import io
import pdfplumber
from datetime import datetime, timedelta
from google import genai

# --- 配置 ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DATA_DIR = "data" # 存档文件夹名

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

client = genai.Client(api_key=GEMINI_API_KEY)
HEADERS = {"Authorization": f"Bearer {NOTION_TOKEN}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}

DB_CONFIG = {
    "CFTC": {"id": "2dc47eb5fd3c80e48d59e0d46494752e", "file_col": "Files & media", "date_col": "Date"},
    "OI": {"id": "2f947eb5fd3c8068b034c7ebc74b88d0", "file_col": "File", "date_col": "Date"}
}

# --- 核心：文件处理与本地存档逻辑 ---

def get_text_from_pdf(content):
    """解析 PDF 文本"""
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            return "\n".join([p.extract_text() for p in pdf.pages[:10] if p.extract_text()])
    except:
        return ""

def sync_and_get_data(db_type):
    """同步 Notion 文件到 GitHub 文件夹并读取内容"""
    cfg = DB_CONFIG[db_type]
    url = f"https://api.notion.com/v1/databases/{cfg['id']}/query"
    # 只查最近 30 天
    past_date = (datetime.utcnow() - timedelta(days=30)).isoformat()
    payload = {"filter": {"property": cfg["date_col"], "date": {"on_or_after": past_date}}}
    
    res = requests.post(url, headers=HEADERS, json=payload).json()
    all_content = []

    for row in res.get("results", []):
        date_str = row["properties"][cfg["date_col"]]["date"]["start"]
        file_prop = row["properties"][cfg["file_col"]]["files"]
        
        if not file_prop: continue
        
        file_url = file_prop[0]["external"]["url"] if file_prop[0]["type"] == "external" else file_prop[0]["file"]["url"]
        file_ext = ".pdf" if ".pdf" in file_url.lower() else ".txt"
        local_filename = f"{DATA_DIR}/{db_type}_{date_str}{file_ext}"
        local_txt_path = local_filename.replace(".pdf", ".txt")

        # 1. 检查本地是否已有解析好的文本
        if os.path.exists(local_txt_path):
            with open(local_txt_path, "r", encoding="utf-8") as f:
                text_content = f.read()
        else:
            # 2. 如果没有，从 Notion 下载并保存
            print(f">>> 正在从 Notion 下载新文件: {local_filename}")
            file_data = requests.get(file_url).content
            if file_ext == ".pdf":
                text_content = get_text_from_pdf(file_data)
            else:
                text_content = file_data.decode('utf-8', errors='ignore')
            
            # 保存文本到 GitHub 仓库，下次直接读
            if text_content:
                with open(local_txt_path, "w", encoding="utf-8") as f:
                    f.write(text_content)
        
        if text_content:
            all_content.append({"Date": date_str, "Content": text_content[:15000]})
            
    return sorted(all_content, key=lambda x: x['Date'])

# --- 写入 Notion 逻辑保持不变 (write_to_page_content) ---
# ... (此处省略前几次对话中已稳定的 write_to_page_content 代码)

def main():
    print(">>> 正在执行增量同步与本地数据读取...")
    cftc_list = sync_and_get_data("CFTC")
    oi_list = sync_and_get_data("OI")
    
    # 库存数据获取（这部分数据小，直接读数据库即可，代码同前）
    # au_inv = fetch_inv("GOLD") ...
    
    prompt = f"你是一个分析师... 请基于以下数据分析: \nCFTC: {json.dumps(cftc_list)} \nOI: {json.dumps(oi_list)} ..."
    
    # 请求 Gemini 并回写 Notion (代码同前)
    # ...

if __name__ == "__main__":
    main()
