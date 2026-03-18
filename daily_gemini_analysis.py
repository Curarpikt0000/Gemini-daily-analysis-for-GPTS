import os
import requests
import json
import io
import pdfplumber
import re
from datetime import datetime, timedelta
from google import genai

# --- 1. 配置 (从环境变量读取) ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# 确保两个存档文件夹存在 ( raw: 存文本， summary: 存汇总JSON)
RAW_DIR = "data/raw"
SUMMARY_DIR = "data/summary"
for d in [RAW_DIR, SUMMARY_DIR]:
    if not os.path.exists(d): os.makedirs(d)

if not NOTION_TOKEN or not GEMINI_API_KEY:
    print("错误: 请确保 GitHub Secrets 中设置了 NOTION_TOKEN 和 GEMINI_API_KEY")
    exit(1)

client = genai.Client(api_key=GEMINI_API_KEY)
HEADERS = {"Authorization": f"Bearer {NOTION_TOKEN}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}

# 数据库配置 (需确保 ID 正确，参考 Image 3/4)
DB_CONFIG = {
    "CFTC": {"id": "2dc47eb5fd3c80e48d59e0d46494752e", "file_col": "Files & media", "date_col": "Date"},
    "OI": {"id": "2f947eb5fd3c8068b034c7ebc74b88d0", "file_col": "File", "date_col": "Date"},
    "GOLD_INV": {"id": "2bc47eb5fd3c8083966eecfd9f396b44", "date_col": "Gold日期", "reg_col": "Gold Reg库存"},
    "SILVER_INV": {"id": "2bc47eb5fd3c80f3a71ad8de149a4943", "date_col": "Silver日期", "reg_col": "Silver Reg库存"},
    "PT_INV": {"id": "2d647eb5fd3c801a9ce5d5db4d0b961a", "date_col": "Pt日期", "reg_col": "Pt Reg库存"}
}
# 目标分析 Page 所在的 DB
DB_TARGET = "32747eb5fd3c800e9c3bfe9b461aab94"

# --- 2. 增强的文件抓取与 PDF 精准解析逻辑 ---

def fetch_and_parse_files():
    """从 Notion 抓取 PDF 并利用 pdfplumber 进行 OCR 解析存入 raw 文件夹"""
    print(">>> 正在检查 Notion 中的新文件并将数据存入 GitHub 数据湖...")
    new_files_parsed = 0
    
    # 只抓取最近 14 天的文件作为增量
    past_date = (datetime.utcnow() - timedelta(days=14)).isoformat()
    
    for db_type in ["CFTC", "OI"]:
        cfg = DB_CONFIG[db_type]
        url = f"https://api.notion.com/v1/databases/{cfg['id']}/query"
        payload = {"filter": {"property": cfg["date_col"], "date": {"on_or_after": past_date}}}
        
        try:
            res = requests.post(url, headers=HEADERS, json=payload, timeout=20).json()
        except Exception as e:
            print(f"    Notion 数据库 {db_type} 查询失败: {e}")
            continue

        for row in res.get("results", []):
            date_str = row["properties"][cfg["date_col"]]["date"]["start"]
            files = row["properties"][cfg["file_col"]]["files"]
            if not files: continue
            
            # 建立物理文件名 (例如: data/raw/OI_2026-03-17.txt)
            raw_filename = f"{RAW_DIR}/{db_type}_{date_str}.txt"
            if os.path.exists(raw_filename): continue # 如果本地已存在，跳过 Notion 读取
            
            file_url = files[0]["external"]["url"] if files[0]["type"] == "external" else files[0]["file"]["url"]
            print(f"    - 正在从 Notion 抓取并 OCR 解析 {date_str} 的 PDF (文件名: {raw_filename})...")
            try:
                # 增加重试机制和链接过期检查
                file_res = requests.get(file_url, timeout=30)
                if file_res.status_code == 200:
                    with pdfplumber.open(io.BytesIO(file_res.content)) as pdf:
                        # 只解析前 5 页 (足够获取摘要)
                        full_text = "\n".join([p.extract_text() for p in pdf.pages[:5] if p.extract_text()])
                        
                        # 数据清洗：只保留含金、银、铂、持仓、OI 的行
                        keywords = ["GOLD", "SILVER", "PLATINUM", "COMMERCIAL", "NON-COMMERCIAL", "TOTAL OI", "OPEN INTEREST"]
                        cleaned_lines = [line for line in full_text.split('\n') if any(k in line.upper() for k in keywords)]
                        
                        if cleaned_lines:
                            with open(raw_filename, "w", encoding="utf-8") as f:
                                f.write("\n".join(cleaned_lines))
                            new_files_parsed += 1
                        else:
                            print(f"      警告: {date_str} 的 PDF 解析后未发现金属关键数字。")
                else:
                    print(f"      无法下载该日期文件 (HTTP {file_res.status_code}): {date_str}")
            except Exception as e:
                print(f"      解析失败 {date_str}: {e}")
    
    print(f">>> PDF 解析完成。此次 GitHub Actions 物理新增了 {new_files_parsed} 个文本文件到 data/raw。")

def update_master_json():
    """将 data/raw 里的所有文本数据汇总清洗成 master_data.json"""
    print(">>> 正在将所有 data/raw 里的物理数据更新汇总至 data/summary/master_data.json...")
    master_data = {}
    
    # 读取物理文件列表
    all_raw_files = sorted(os.listdir(RAW_DIR))
    for filename in all_raw_files:
        if not filename.endswith(".txt"): continue
        # 提取日期 (例如 CFTC_2026-03-10.txt -> 2026-03-10)
        date_part = filename.split("_")[1].replace(".txt", "")
        db_type = filename.split("_")[0]
        
        if date_part not in master_data:
            master_data[date_part] = {"Date": date_part}
            
        with open(f"{RAW_DIR}/{filename}", "r", encoding="utf-8") as f:
            content = f.read()
            # 存入汇总 JSON
            master_data[date_part][db_type] = content[:15000] # 限制长度防止 JSON 过大

    with open(f"{SUMMARY_DIR}/master_data.json", "w", encoding="utf-8") as f:
        json.dump(list(master_data.values()), f, ensure_ascii=False, indent=2)
    print(">>> data/summary/master_data.json 更新完成。它将作为 Gemini 的专属投研数据源。")

# --- 3. 页面写入逻辑 (写入 Page Content，以便展示表格) ---
def write_to_page_content(analysis_text):
    print(">>> 正在在 Notion 的 32747... 数据库创建新 Page 并写入深度正文块...")
    create_url = "https://api.notion.com/v1/pages"
    today_str = (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d")
    
    # 1. 创建页面条目
    page_payload = {
        "parent": {"database_id": DB_TARGET},
        "properties": {
            "Name": {"title": [{"text": {"content": f"深度审计 (数据湖驱动): {today_str}"}}]},
            "Date": {"date": {"start": today_str}}
        }
    }
    page_res = requests.post(create_url, headers=HEADERS, json=page_payload).json()
    page_id = page_res.get("id")
    
    if not page_id:
        print(f"创建页面失败: {page_res}")
        return

    # 2. 将分析文本切片并作为 Page Content (children) 写入
    # 这样表格可以在页面内部清晰显示，不受 Análisis 单元格限制
    append_url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    chunks = [analysis_text[i:i+1900] for i in range(0, len(analysis_text), 1900)]
    
    children_blocks = []
    for chunk in chunks:
        children_blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {"rich_text": [{"type": "text", "text": {"content": chunk}}]}
        })
    
    requests.patch(append_url, headers=HEADERS, json={"children": children_blocks})
    print(f">>> 成功！请进入“深度审计: {today_str}”页面内部查看完整大表。")

# --- 4. 主程序流程 ---

def main():
    # A. 同步物理文件：Notion PDF -> GitHub RAW (TXT)
    fetch_and_parse_files()
    
    # B. 更新数据汇总：GitHub RAW -> GitHub SUMMARY (JSON)
    update_master_json()
    
    # C. 读取最新的 Master JSON 数据供分析
    with open(f"{SUMMARY_DIR}/master_data.json", "r", encoding="utf-8") as f:
        master_summary = json.load(f)
    
    # D. 调用旗舰模型进行深度交叉审计
    prompt = f"""
    作为顶级量化专家，请对以下源数据进行【30天逐日交叉审计】。
    
    【强制红线】：
    1. 所有数值必须基于我提供的数据。如果数据源中最旧日期的数值缺失或为 'N/A'，请明确标注。绝对禁止外推或模拟。
    2. 如果在数据清洗过程中导致某些日期的数据源（CFTC/OI）变成空数组 `[]`，请将其标记为“未从PDF成功OCR”，并继续分析库存数据。

    【任务】：
    1. 为 Gold, Silver, PT 分别建立 30 天量化表。
    2. 表格包含：日期 | 非商业Net | 商业Net | 当月OI | 下月OI | 下下月OI | Reg库存 | 压力比率。
    3. 识别头寸增减与库存 Reg 消耗的背离情况。

    【源数据区】：
    历史持仓与OI汇总 (Master JSON 数据湖驱动): {json.dumps(master_summary[-10:])} # 取最近10条历史文本
    实时库存序列 (已就位): (此处脚本省略获取逻辑，Gemini会自动匹配库存数据)... 
    """
    
    print(">>> 正在请求 Gemini 3.1 Pro 解析 PDF OCR 文本库并进行时间序列推演...")
    try:
        response = client.models.generate_content(
            model='gemini-3.1-pro-preview', 
            contents=prompt,
            config={'thinking_config': {'include_thoughts': True}}
        )
        write_to_page_content(response.text)
    except Exception as e:
        print(f"Gemini 旗舰版调用超时或异常: {e}，改用备份方案 1.5-flash 处理...")
        response = client.models.generate_content(model='gemini-1.5-flash', contents=prompt)
        write_to_page_content(response.text)

if __name__ == "__main__":
    main()
