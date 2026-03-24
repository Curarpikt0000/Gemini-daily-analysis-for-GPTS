import os
import requests
import json
import io
import pdfplumber
from datetime import datetime, timedelta
import vertexai
from vertexai.generative_models import GenerativeModel

# --- 1. 配置与鉴权 ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
# Vertex AI 配置
PROJECT_ID = "你的PROJECT_ID"  # 替换为你的真实项目 ID
LOCATION = "us-central1"
DB_TARGET = "32747eb5fd3c800e9c3bfe9b461aab94"

# 初始化 Vertex AI (鉴权由 GitHub Action 的 auth 步骤自动注入)
vertexai.init(project=PROJECT_ID, location=LOCATION)
model = GenerativeModel("gemini-1.5-pro-002")

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

DB_CONFIG = {
    "CFTC": {"id": "2c747eb5fd3c808186ddd0aeb45d5046", "file_col": "Files & media", "date_col": "Date"},
    "OI": {"id": "2fc47eb5fd3c8035ab22cabf3e6e41bb", "file_col": "File", "date_col": "Date"},
    "GOLD_INV": {"id": "2bc47eb5fd3c8083966eecfd9f396b44", "date_col": "Gold日期", "reg_col": "Gold Reg库存"},
    "SILVER_INV": {"id": "2bc47eb5fd3c80f3a71ad8de149a4943", "date_col": "Silver日期", "reg_col": "Silver Reg库存"},
    "PT_INV": {"id": "2d647eb5fd3c801a9ce5d5db4d0b961a", "date_col": "Pt日期", "reg_col": "Pt Reg库存"}
}

# --- 2. 抓取逻辑 ---

def get_weekly_best_records(db_type):
    cfg = DB_CONFIG[db_type]
    url = f"https://api.notion.com/v1/databases/{cfg['id']}/query"
    past_date = (datetime.utcnow() - timedelta(days=35)).isoformat()
    res = requests.post(url, headers=HEADERS, json={"filter": {"property": cfg["date_col"], "date": {"on_or_after": past_date}}}).json()
    
    weeks = {}
    for row in res.get("results", []):
        props = row.get("properties", {})
        dt_data = props.get(cfg["date_col"], {}).get("date")
        if not dt_data: continue
        
        dt_str = dt_data["start"]
        dt = datetime.fromisoformat(dt_str)
        week_id = dt.strftime("%Y-W%V")
        if week_id not in weeks: weeks[week_id] = []
        weeks[week_id].append({"date": dt, "row": row})
    
    best_records = []
    for week_id in sorted(weeks.keys()):
        group = weeks[week_id]
        group.sort(key=lambda x: x['date'].weekday())
        for entry in group:
            if entry['date'].weekday() in [0, 1, 2]:
                best_records.append(entry)
                break
    return best_records

def fetch_content_lake():
    print(">>> 正在执行抓取...")
    archive = []
    for db_type in ["CFTC", "OI"]:
        records = get_weekly_best_records(db_type)
        for entry in records:
            cfg = DB_CONFIG[db_type]
            props = entry['row'].get("properties", {})
            file_prop = props.get(cfg["file_col"], {})
            files = file_prop.get("files", [])
            date_val = props.get(cfg["date_col"], {}).get("date", {}).get("start", "Unknown")
            
            if not files: continue
            
            f_url = files[0]["external"]["url"] if files[0]["type"] == "external" else files[0]["file"]["url"]
            print(f"    - 抓取成功: {db_type} | {date_val}")
            
            try:
                resp = requests.get(f_url, timeout=30)
                with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                    text = "\n".join([p.extract_text() for p in pdf.pages[:12] if p.extract_text()])
                    archive.append({"Type": db_type, "Date": date_val, "Content": text[:15000]})
            except: pass
    return archive

# --- 3. 分析与回写 ---

def write_structured_to_notion(report_content):
    print(">>> 正在回写深度报告...")
    jst_now = (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d")
    
    page_payload = {
        "parent": {"database_id": DB_TARGET},
        "properties": {
            "Name": {"title": [{"text": {"content": f"贵金属量化周初审计: {jst_now}"}}]},
            "Date": {"date": {"start": jst_now}}
        }
    }
    page = requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=page_payload).json()
    page_id = page.get("id")
    if not page_id: return

    chunks = [report_content[i:i+1900] for i in range(0, len(report_content), 1900)]
    children = []
    for c in chunks:
        children.append({
            "object": "block",
            "type": "callout",
            "callout": {
                "rich_text": [{"type": "text", "text": {"content": c}}],
                "icon": {"emoji": "📉"},
                "color": "blue_background"
            }
        })
    requests.patch(f"https://api.notion.com/v1/blocks/{page_id}/children", headers=HEADERS, json={"children": children})

def main():
    content_lake = fetch_content_lake()
    
    prompt = f"""
    分析以下 4 周的周初数据：{json.dumps(content_lake)}
    
    【格式要求】：
    1. 严禁使用 Markdown 表格。
    2. 使用“### 金属名称”作为二级标题。
    3. 使用“- **指标名称**：数值”的列表形式。
    4. 必须包含商业(Commercial)与非商业(Non-commercial)头寸的 4 周趋势对比。
    5. 使用 LaTeX 公式展示库存压力：$$Pressure = \\frac{{OI}}{{Reg}}$$
    """
    
    print(">>> 请求 Vertex AI 生成深度研报...")
    response = model.generate_content(prompt)
    write_structured_to_notion(response.text)
    print(">>> 任务完成！")

if __name__ == "__main__":
    main()
