import os
import requests
import json
import io
import pdfplumber
from datetime import datetime, timedelta
from google import genai

# --- 1. 核心配置 (请确保这些 ID 是正确的) ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# 目标：分析报告存入的数据库 ID
DB_TARGET = "32747eb5fd3c800e9c3bfe9b461aab94" 

# 数据湖目录
RAW_DIR = "data/raw"
SUMMARY_DIR = "data/summary"
for d in [RAW_DIR, SUMMARY_DIR]:
    os.makedirs(d, exist_ok=True)

# 来源数据库配置
DB_CONFIG = {
    "CFTC": {"id": "2dc47eb5fd3c80e48d59e0d46494752e", "file_col": "Files & media", "date_col": "Date"},
    "OI": {"id": "2f947eb5fd3c8068b034c7ebc74b88d0", "file_col": "File", "date_col": "Date"},
    "GOLD_INV": {"id": "2bc47eb5fd3c8083966eecfd9f396b44", "date_col": "Gold日期", "reg_col": "Gold Reg库存"},
    "SILVER_INV": {"id": "2bc47eb5fd3c80f3a71ad8de149a4943", "date_col": "Silver日期", "reg_col": "Silver Reg库存"},
    "PT_INV": {"id": "2d647eb5fd3c801a9ce5d5db4d0b961a", "date_col": "Pt日期", "reg_col": "Pt Reg库存"}
}

client = genai.Client(api_key=GEMINI_API_KEY)
HEADERS = {"Authorization": f"Bearer {NOTION_TOKEN}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}

# --- 2. 数据处理与清洗逻辑 ---

def fetch_and_save_raw_files():
    """抓取 PDF 内容并存档到 GitHub 文件夹"""
    print(">>> 正在检查 Notion 中的新文件并同步到数据湖...")
    for db_type in ["CFTC", "OI"]:
        cfg = DB_CONFIG[db_type]
        url = f"https://api.notion.com/v1/databases/{cfg['id']}/query"
        payload = {"filter": {"property": cfg["date_col"], "date": {"on_or_after": (datetime.utcnow()-timedelta(days=14)).isoformat()}}}
        res = requests.post(url, headers=HEADERS, json=payload).json()
        
        for row in res.get("results", []):
            date_str = row["properties"][cfg["date_col"]]["date"]["start"]
            files = row["properties"][cfg["file_col"]]["files"]
            if not files: continue
            
            local_path = f"{RAW_DIR}/{db_type}_{date_str}.txt"
            if os.path.exists(local_path): continue # 如果本地已存，则跳过
            
            file_url = files[0]["external"]["url"] if files[0]["type"] == "external" else files[0]["file"]["url"]
            try:
                print(f"    - 正在解析 {date_str} 的 PDF...")
                pdf_data = requests.get(file_url).content
                with pdfplumber.open(io.BytesIO(pdf_data)) as pdf:
                    # 抓取前 10 页文本
                    full_text = "\n".join([p.extract_text() for p in pdf.pages[:10] if p.extract_text()])
                    # 清洗：只保留含金、银、铂、持仓、OI 的行，大幅减小文件体积
                    keywords = ["GOLD", "SILVER", "PLATINUM", "COMMERCIAL", "NON-COMMERCIAL", "TOTAL OI", "OPEN INTEREST"]
                    cleaned_lines = [line for line in full_text.split('\n') if any(k in line.upper() for k in keywords)]
                    with open(local_path, "w", encoding="utf-8") as f:
                        f.write("\n".join(cleaned_lines))
            except Exception as e:
                print(f"    × 解析失败 {date_str}: {e}")

def get_master_data_lake():
    """读取所有本地存档的文本汇总"""
    all_content = ""
    for filename in sorted(os.listdir(RAW_DIR)):
        if filename.endswith(".txt"):
            with open(f"{RAW_DIR}/{filename}", "r", encoding="utf-8") as f:
                all_content += f"\n--- {filename} ---\n" + f.read()
    return all_content[:30000] # 截断防止超出 Gemini 限制

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

# --- 3. 结果回写逻辑 (写入 Page Content 以便展示表格) ---

def write_to_page_content(analysis_text):
    print(">>> 正在回写分析结果至 Notion 页面正文...")
    create_url = "https://api.notion.com/v1/pages"
    today_str = (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d")
    
    # 1. 创建页面
    page_payload = {
        "parent": {"database_id": DB_TARGET},
        "properties": {
            "Name": {"title": [{"text": {"content": f"30D深度审计 (数据湖驱动): {today_str}"}}]},
            "Date": {"date": {"start": today_str}}
        }
    }
    page_res = requests.post(create_url, headers=HEADERS, json=page_payload).json()
    page_id = page_res.get("id")
    
    if not page_id:
        print(f"创建页面失败: {page_res}")
        return

    # 2. 写入正文块
    append_url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    chunks = [analysis_text[i:i+1900] for i in range(0, len(analysis_text), 1900)]
    children_blocks = [{"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"text": {"content": c}}]}} for c in chunks]
    requests.patch(append_url, headers=HEADERS, json={"children": children_blocks})
    print(f">>> 成功！请查看 Notion 页面内容。")

# --- 4. 主流程 ---

def main():
    # A. 同步物理文件到本地 GitHub 环境
    fetch_and_save_raw_files()
    
    # B. 从本地数据湖读取汇总内容
    lake_content = get_master_data_lake()
    inventory = fetch_notion_inventory()
    
    # C. 调用 Gemini 分析
    prompt = f"""
    作为顶级量化专家，请根据 GitHub 汇总数据湖进行【30天逐日交叉审计】。
    
    【源数据区】：
    历史持仓与OI数据流: {lake_content}
    实时库存序列: {json.dumps(inventory)}
    
    【任务】：
    1. 为 GOLD, SILVER, PT 分别建立 30 天量化表。
    2. 包含：日期 | 非商业Net | 商业Net | 当月OI | 下月OI | 下下月OI | Reg库存 | 压力比率。
    3. 识别头寸与库存的背离，评估逼空风险。
    """
    
    print(">>> 正在请求 Gemini 3.1 Pro 分析汇总数据...")
    try:
        response = client.models.generate_content(
            model='gemini-3.1-pro-preview', 
            contents=prompt,
            config={'thinking_config': {'include_thoughts': True}}
        )
        write_to_page_content(response.text)
    except Exception as e:
        print(f"Gemini 调用失败: {e}")

if __name__ == "__main__":
    main()
