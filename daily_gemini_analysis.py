import os
import requests
import json
import io
import pdfplumber
from datetime import datetime, timedelta
from google import genai

# --- 1. 配置与鉴权 ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DB_TARGET = "32747eb5fd3c800e9c3bfe9b461aab94"

client = genai.Client(api_key=GEMINI_API_KEY)
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

DB_CONFIG = {
    "CFTC": {"id": "2c747eb5fd3c808186ddd0aeb45d5046", "file_col": "File & media", "date_col": "Date"},
    "OI": {"id": "2fc47eb5fd3c8035ab22cabf3e6e41bb", "file_col": "File", "date_col": "Date"},
    "GOLD_INV": {"id": "2bc47eb5fd3c8083966eecfd9f396b44", "date_col": "Gold日期", "reg_col": "Gold Reg库存"},
    "SILVER_INV": {"id": "2bc47eb5fd3c80f3a71ad8de149a4943", "date_col": "Silver日期", "reg_col": "Silver Reg库存"},
    "PT_INV": {"id": "2d647eb5fd3c801a9ce5d5db4d0b961a", "date_col": "Pt日期", "reg_col": "Pt Reg库存"}
}

# --- 2. 核心：周初择优抓取逻辑 ---

def get_weekly_best_records(db_type):
    """在每一周中寻找周一、周二或周三的数据"""
    cfg = DB_CONFIG[db_type]
    url = f"https://api.notion.com/v1/databases/{cfg['id']}/query"
    past_date = (datetime.utcnow() - timedelta(days=35)).isoformat()
    res = requests.post(url, headers=HEADERS, json={"filter": {"property": cfg["date_col"], "date": {"on_or_after": past_date}}}).json()
    
    # 按周分组
    weeks = {}
    for row in res.get("results", []):
        dt_str = row["properties"][cfg["date_col"]]["date"]["start"]
        dt = datetime.fromisoformat(dt_str)
        week_id = dt.strftime("%Y-W%V")
        if week_id not in weeks: weeks[week_id] = []
        weeks[week_id].append({"date": dt, "row": row})
    
    best_records = []
    for week_id in sorted(weeks.keys()):
        # 优先级：周一(0) > 周二(1) > 周三(2)
        group = weeks[week_id]
        group.sort(key=lambda x: x['date'].weekday())
        for entry in group:
            if entry['date'].weekday() in [0, 1, 2]:
                best_records.append(entry)
                break
    return best_records

def fetch_content_lake():
    print(">>> 正在执行【周初择优】抓取逻辑...")
    archive = []
    for db_type in ["CFTC", "OI"]:
        records = get_weekly_best_records(db_type)
        for entry in records:
            cfg = DB_CONFIG[db_type]
            props = entry['row']["properties"]
            date_val = props[cfg["date_col"]]["date"]["start"]
            files = props[cfg["file_col"]]["files"]
            if not files: continue
            
            f_url = files[0]["external"]["url"] if files[0]["type"] == "external" else files[0]["file"]["url"]
            print(f"    - 选中数据: {db_type} | {date_val} (周{entry['date'].weekday()+1})")
            
            try:
                resp = requests.get(f_url, timeout=30)
                with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                    text = "\n".join([p.extract_text() for p in pdf.pages[:12] if p.extract_text()])
                    archive.append({"Type": db_type, "Date": date_val, "Content": text[:12000]})
            except: pass
    return archive

# --- 3. Notion 样式优化写入 (使用区块而非纯文本表格) ---

def write_structured_to_notion(report_json):
    """
    接收 Gemini 生成的结构化分析，并将其转换为 Notion 呼应块和列表。
    注：此处简化演示，实际使用时 Gemini 输出应包含清晰的标识符。
    """
    print(">>> 正在以 Notion 优化样式回写报告...")
    jst_today = (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d")
    
    # 创建页面
    page_payload = {
        "parent": {"database_id": DB_TARGET},
        "properties": {
            "Name": {"title": [{"text": {"content": f"量化审计精报: {jst_today}"}}]},
            "Date": {"date": {"start": jst_today}}
        }
    }
    page = requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=page_payload).json()
    page_id = page.get("id")
    
    if not page_id: return

    # 构造富文本区块 (分块写入避免长度限制)
    chunks = [report_json[i:i+1900] for i in range(0, len(report_json), 1900)]
    blocks = []
    for c in chunks:
        blocks.append({
            "object": "block",
            "type": "callout",
            "callout": {
                "rich_text": [{"type": "text", "text": {"content": c}}],
                "icon": {"emoji": "📊"},
                "color": "gray_background"
            }
        })
    
    requests.patch(f"https://api.notion.com/v1/blocks/{page_id}/children", headers=HEADERS, json={"children": blocks})

# --- 4. 主流程 ---

def main():
    content_lake = fetch_content_lake()
    # 模拟抓取库存逻辑 (省略重复 fetch_all_inventories 代码)
    # ...
    
    prompt = f"""
    你是顶级贵金属量化分析师。请分析以下 4 周内每轮周初的数据。
    
    【源数据】：{json.dumps(content_lake)}
    
    【格式指南 - 针对 Notion 优化】：
    1. 不要生成 Markdown 表格。
    2. 请使用“标题 + 列表”的形式展示数据。例如：
       ### 黄金 (Gold) 审计
       - **日期**：2026-03-02
       - **商业头寸**：数值
       - **非商业头寸**：数值
       - **库存压力比率**：使用 LaTeX 公式 $$Ratio = \\frac{{OI}}{{Reg}}$$
    3. 重点分析 4 周内头寸的变化率。
    """
    
    response = client.models.generate_content(
        model='gemini-3.1-pro-preview', 
        contents=prompt,
        config={'thinking_config': {'include_thoughts': True}}
    )
    write_structured_to_notion(response.text)

if __name__ == "__main__":
    main()
