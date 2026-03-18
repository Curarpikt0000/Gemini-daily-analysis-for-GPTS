import os
import requests
import json
import io
import pdfplumber
from datetime import datetime, timedelta
from google import genai

# --- 1. 核心鉴权与配置 ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DB_TARGET = "32747eb5fd3c800e9c3bfe9b461aab94" # 目标报告数据库

client = genai.Client(api_key=GEMINI_API_KEY)
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# 数据库 ID 配置 (根据您提供的链接更新)
DB_CONFIG = {
    "CFTC": {"id": "2c747eb5fd3c808186ddd0aeb45d5046", "file_col": "File & media", "date_col": "Date"},
    "OI": {"id": "2fc47eb5fd3c8035ab22cabf3e6e41bb", "file_col": "File", "date_col": "Date"},
    "GOLD_INV": {"id": "2bc47eb5fd3c8083966eecfd9f396b44", "date_col": "Gold日期", "reg_col": "Gold Reg库存"},
    "SILVER_INV": {"id": "2bc47eb5fd3c80f3a71ad8de149a4943", "date_col": "Silver日期", "reg_col": "Silver Reg库存"},
    "PT_INV": {"id": "2d647eb5fd3c801a9ce5d5db4d0b961a", "date_col": "Pt日期", "reg_col": "Pt Reg库存"}
}

# --- 2. 核心抓取函数 ---

def is_monday(date_str):
    """确保只抓取周一的数据进行周度对比"""
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.weekday() == 0
    except: return False

def fetch_monday_reports():
    """解析 4 周内所有周一的 PDF 文本内容"""
    print(">>> 正在检索 4 周内的周一关键报告...")
    content_lake = []
    # 窗口扩大到 32 天确保覆盖 4 个周一
    past_date = (datetime.utcnow() - timedelta(days=32)).isoformat()
    
    for db_type in ["CFTC", "OI"]:
        cfg = DB_CONFIG[db_type]
        res = requests.post(f"https://api.notion.com/v1/databases/{cfg['id']}/query", 
                            headers=HEADERS, json={"filter": {"property": cfg["date_col"], "date": {"on_or_after": past_date}}}).json()
        
        for row in res.get("results", []):
            date_val = row["properties"][cfg["date_col"]]["date"]["start"]
            if not is_monday(date_val): continue
            
            files = row["properties"][cfg["file_col"]]["files"]
            if not files: continue
            
            f_url = files[0]["external"]["url"] if files[0]["type"] == "external" else files[0]["file"]["url"]
            print(f"    - 正在解析周一报告: {db_type} | {date_val}")
            
            try:
                resp = requests.get(f_url, timeout=30)
                with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                    # 抓取前 12 页文本
                    text = "\n".join([p.extract_text() for p in pdf.pages[:12] if p.extract_text()])
                    content_lake.append({"Type": db_type, "Date": date_val, "Content": text[:15000]})
            except Exception as e:
                print(f"      × 失败: {e}")
    return content_lake

def fetch_inventories():
    """从 3 个独立库存库抓取 Reg 库存"""
    print(">>> 正在抓取 Gold, Silver, PT 三大库存数据库...")
    inv_data = {}
    past_date = (datetime.utcnow() - timedelta(days=32)).isoformat()
    
    for metal in ["GOLD", "SILVER", "PT"]:
        cfg = DB_CONFIG[f"{metal}_INV"]
        res = requests.post(f"https://api.notion.com/v1/databases/{cfg['id']}/query", 
                            headers=HEADERS, json={"filter": {"property": cfg["date_col"], "date": {"on_or_after": past_date}}}).json()
        for row in res.get("results", []):
            dt = row["properties"][cfg["date_col"]]["date"]["start"]
            val = row["properties"][cfg["reg_col"]]["number"]
            if dt not in inv_data: inv_data[dt] = {}
            inv_data[dt][f"{metal}_Reg"] = val
    return inv_data

# --- 3. 分析与回写逻辑 ---

def write_to_notion(report_text):
    print(">>> 正在推送全量量化审计报告...")
    jst_today = (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d")
    payload = {
        "parent": {"database_id": DB_TARGET},
        "properties": {
            "Name": {"title": [{"text": {"content": f"30D多空头寸与库存深度审计: {jst_today}"}}]},
            "Date": {"date": {"start": jst_today}}
        }
    }
    page = requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=payload).json()
    page_id = page.get("id")
    if page_id:
        chunks = [report_text[i:i+1900] for i in range(0, len(report_text), 1900)]
        blocks = [{"object":"block","type":"paragraph","paragraph":{"rich_text":[{"text":{"content":c}}]}} for c in chunks]
        requests.patch(f"https://api.notion.com/v1/blocks/{page_id}/children", headers=HEADERS, json={"children": blocks})

def main():
    # 1. 抓取所有周一的数据
    reports = fetch_monday_reports()
    # 2. 抓取 3 大金属 Reg 库存
    inv = fetch_inventories()
    
    prompt = f"""
    作为顶级量化策略师，请对以下 4 周内所有【周一】的数据进行全量审计。
    
    【数据源】：
    1. CFTC 与 OI PDF 文本流: {json.dumps(reports)}
    2. 金、银、铂的 Registered 库存数据: {json.dumps(inv)}

    【强制要求】：
    1. **持仓异动分析**：为每种金属（Gold, Silver, PT）提取过去 4 周周一的：非商业头寸(Net)、商业头寸(Net)。
    2. **库存覆盖率计算**：使用 LaTeX 公式计算每日比例：
       $$\\text{{Coverage Ratio}} = \\frac{{\\text{{Total OI (3 Months)}}}}{{\\text{{Registered Inventory}}}}$$
    3. **趋势对比**：在结果中必须输出商业和非商业头寸在 4 周内的变化状况百分比。
    
    【格式指南】：
    - 先概括要点。
    - 使用 Markdown 表格展示 30 天逐周数据。
    - 结尾给出 3 个明确的建议。
    """
    
    print(">>> 正在请求 Gemini 3.1 Pro 执行逻辑推演与 LaTeX 建模...")
    try:
        response = client.models.generate_content(
            model='gemini-3.1-pro-preview', 
            contents=prompt,
            config={'thinking_config': {'include_thoughts': True}}
        )
        write_to_notion(response.text)
        print(">>> 任务圆满完成！")
    except Exception as e:
        print(f"Gemini 异常: {e}")

if __name__ == "__main__":
    main()
