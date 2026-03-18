import os
import requests
import json
from datetime import datetime, timedelta
from google import genai

# --- 1. 配置 (从环境变量读取) ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not NOTION_TOKEN or not GEMINI_API_KEY:
    print("错误: 请设置环境变量 NOTION_TOKEN 和 GEMINI_API_KEY")
    exit(1)

client = genai.Client(api_key=GEMINI_API_KEY)

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# 数据库映射
DB_TARGET = "32747eb5fd3c800e9c3bfe9b461aab94" 
DB_CONFIG = {
    "CFTC": {"id": "2dc47eb5fd3c80e48d59e0d46494752e", "date_col": "Date", "file_col": "Files & media"},
    "OI": {"id": "2f947eb5fd3c8068b034c7ebc74b88d0", "date_col": "Date", "file_col": "File"},
    "SILVER": {"id": "2bc47eb5fd3c80f3a71ad8de149a4943", "date_col": "Silver日期", "reg_col": "Silver Reg库存", "market_col": "市场"},
    "PT": {"id": "2d647eb5fd3c801a9ce5d5db4d0b961a", "date_col": "Pt日期", "reg_col": "Pt Reg库存", "market_col": "市场"},
    "GOLD": {"id": "2bc47eb5fd3c8083966eecfd9f396b44", "date_col": "Gold日期", "reg_col": "Gold Reg库存", "market_col": "市场"}
}

# --- 2. 工具函数 ---

def get_past_28_days_filter(date_col):
    past_date = (datetime.utcnow() - timedelta(days=28)).isoformat()
    return {"property": date_col, "date": {"on_or_after": past_date}}

def safe_extract(props, col, p_type):
    if col not in props: return None
    d = props[col]
    try:
        if p_type == "date": return d['date']['start'] if d['date'] else None
        if p_type == "number": return d['number']
        if p_type == "files":
            if d['files']:
                f = d['files'][0]
                return f['external']['url'] if f['type'] == 'external' else f['file']['url']
    except: return None
    return "N/A"

def fetch_inv(db_name):
    cfg = DB_CONFIG[db_name]
    url = f"https://api.notion.com/v1/databases/{cfg['id']}/query"
    payload = {"filter": {"and": [get_past_28_days_filter(cfg["date_col"]), {"property": cfg["market_col"], "select": {"equals": "CME"}}]}}
    res = requests.post(url, headers=HEADERS, json=payload).json()
    records = []
    for row in res.get("results", []):
        p = row["properties"]
        records.append({"Date": safe_extract(p, cfg["date_col"], "date"), "Reg": safe_extract(p, cfg["reg_col"], "number")})
    return sorted([r for r in records if r["Date"]], key=lambda x: x['Date'])

def fetch_files(db_name):
    cfg = DB_CONFIG[db_name]
    url = f"https://api.notion.com/v1/databases/{cfg['id']}/query"
    payload = {"filter": get_past_28_days_filter(cfg["date_col"])}
    res = requests.post(url, headers=HEADERS, json=payload).json()
    records = []
    for row in res.get("results", []):
        p = row["properties"]
        u = safe_extract(p, cfg["file_col"], "files")
        content = requests.get(u).text[:5000] if u else "N/A"
        records.append({"Date": safe_extract(p, cfg["date_col"], "date"), "Content": content})
    return sorted([r for r in records if r["Date"]], key=lambda x: x['Date'])

# --- 3. 高级写入逻辑 (Page Content) ---

def write_to_page_content(analysis_text):
    print(">>> 正在创建新页面并写入详细正文...")
    create_url = "https://api.notion.com/v1/pages"
    today_str = (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d")
    
    # 1. 创建页面条目
    page_payload = {
        "parent": {"database_id": DB_TARGET},
        "properties": {
            "Name": {"title": [{"text": {"content": f"深度研报: {today_str}"}}]},
            "Date": {"date": {"start": today_str}}
        }
    }
    page_res = requests.post(create_url, headers=HEADERS, json=page_payload).json()
    page_id = page_res.get("id")
    
    if not page_id:
        print(f"创建页面失败: {page_res}")
        return

    # 2. 将分析文本切片并作为 Page 的 Children (Block) 写入
    # 这样内容会出现在页面内部，而非单元格里
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
    print(f">>> 成功！请在 Notion 数据库点击进入“深度研报: {today_str}”页面查看正文。")

# --- 4. 主程序 ---

def main():
    print(">>> 正在抓取过去 30 天多维序列...")
    cftc = fetch_files("CFTC")
    oi = fetch_files("OI")
    au = fetch_inv("GOLD")
    ag = fetch_inv("SILVER")
    pt = fetch_inv("PT")
    
    prompt = f"""
    作为高级分析师，请根据源数据进行三类贵金属（黄金、白银、铂金）的【30天时间序列交叉分析】。
    
    【分析目标】：
    1. **分品种独立建表**：为 Gold, Silver, PT 分别建立表格。
    2. **每日数据精度**：表格需包含：日期、非商业(Net)、商业(Net)、COMEX Reg库存、库存覆盖率(OI/Reg)。
    3. **趋势对比**：
       - 计算过去30天头寸的移动趋势（Moving Trend）。
       - 识别头寸增减与库存消耗的背离情况。
    4. **实物交割预警**：基于每日数据，判断哪种金属的挤仓(Squeeze)风险最高。

    【输出格式要求】：
    # 黄金 (Gold) 30日量化表
    (此处放表格)
    *趋势分析：...*

    # 白银 (Silver) 30日量化表
    (此处放表格)
    *趋势分析：...*

    # 铂金 (PT) 30日量化表
    (此处放表格)
    *趋势分析：...*

    【源数据】：
    CFTC: {json.dumps(cftc)}
    OI: {json.dumps(oi)}
    库存(Au/Ag/Pt): {json.dumps({'Au':au, 'Ag':ag, 'Pt':pt})}
    """
    
    print(">>> 正在请求 Gemini 3.1 Pro 深度建模...")
    try:
        response = client.models.generate_content(
            model='gemini-3.1-pro-preview', 
            contents=prompt,
            config={'thinking_config': {'include_thoughts': True}}
        )
        write_to_page_content(response.text)
    except Exception as e:
        print(f"3.1 失败，降级至 1.5-flash: {e}")
        response = client.models.generate_content(model='gemini-1.5-flash', contents=prompt)
        write_to_page_content(response.text)

if __name__ == "__main__":
    main()
