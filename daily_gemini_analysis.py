import os
import requests
import json
import io
import pdfplumber
from datetime import datetime, timedelta
from google import genai

# --- 1. 配置 (从环境变量读取) ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not NOTION_TOKEN or not GEMINI_API_KEY:
    print("错误: 请确保 GitHub Secrets 中设置了环境变量")
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

# --- 2. 增强型文件解析逻辑 ---

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

def fetch_files_deep_parse(db_name):
    """深度解析 PDF，覆盖多月份合约数据"""
    print(f">>> 正在深度扫描数据库: {db_name}")
    cfg = DB_CONFIG[db_name]
    url = f"https://api.notion.com/v1/databases/{cfg['id']}/query"
    payload = {"filter": get_past_28_days_filter(cfg["date_col"])}
    res = requests.post(url, headers=HEADERS, json=payload).json()
    
    records = []
    for row in res.get("results", []):
        p = row["properties"]
        file_url = safe_extract(p, cfg["file_col"], "files")
        date_val = safe_extract(p, cfg["date_col"], "date")
        
        content = "N/A"
        if file_url:
            try:
                response = requests.get(file_url)
                if file_url.lower().split('?')[0].endswith('.pdf'):
                    with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                        # 扩大扫描范围至前 8 页，以捕捉远期合约表格
                        content = "\n".join([page.extract_text() for page in pdf.pages[:8] if page.extract_text()])
                else:
                    content = response.text[:8000]
            except Exception as e:
                print(f"警告: 解析日期 {date_val} 失败: {e}")
        
        records.append({"Date": date_val, "Content": content})
    return sorted([r for r in records if r["Date"]], key=lambda x: x['Date'])

# --- 3. 页面回写逻辑 ---

def write_to_page_content(analysis_text):
    print(">>> 正在生成多月份深度审计报告...")
    create_url = "https://api.notion.com/v1/pages"
    jst_now = datetime.utcnow() + timedelta(hours=9)
    today_str = jst_now.strftime("%Y-%m-%d")
    
    page_payload = {
        "parent": {"database_id": DB_TARGET},
        "properties": {
            "Name": {"title": [{"text": {"content": f"30D多月份持仓与库存审计: {today_str}"}}]},
            "Date": {"date": {"start": today_str}}
        }
    }
    page_res = requests.post(create_url, headers=HEADERS, json=page_payload).json()
    page_id = page_res.get("id")
    
    if not page_id: return

    append_url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    chunks = [analysis_text[i:i+1900] for i in range(0, len(analysis_text), 1900)]
    children_blocks = [{"object": "block", "type": "paragraph", "paragraph": {"rich_text": [{"type": "text", "text": {"content": c}}]}} for c in chunks]
    
    requests.patch(append_url, headers=HEADERS, json={"children": children_blocks})
    print(f">>> 成功！请在 Notion 中进入该页面查看 [当月+后两月] 合约的详细表格。")

# --- 4. 主程序 ---

def main():
    print(">>> 启动 30 天多月份合约交叉审计引擎...")
    
    # 抓取数据
    cftc_data = fetch_files_deep_parse("CFTC")
    oi_data = fetch_files_deep_parse("OI")
    au_inv = fetch_inv("GOLD")
    ag_inv = fetch_inv("SILVER")
    pt_inv = fetch_inv("PT")
    
    prompt = f"""
    你是一个顶级量化策略专家。请针对提供的 PDF 文本内容，提取并分析黄金(Gold)、白银(Silver)、铂金(PT)的 30 天逐日数据。
    
    【关键指标要求】：
    1. **多月份 OI 覆盖**：必须从数据中识别并列出：
       - 当月合约 (Current Month) 的 OI 数量。
       - 后面第一个月 (Month + 1) 的 OI 数量。
       - 后面第二个月 (Month + 2) 的 OI 数量。
    2. **持仓净额趋势**：提取非商业(Non-Commercial Net)与商业(Commercial Net)的头寸变化。
    3. **实物覆盖审计**：计算 [总合计 OI (三个月之和) / COMEX Reg库存] 的压力比率。

    【输出格式】：
    为三种金属分别建立独立的【30天多合约量化审计表】：
    表头应包含：日期 | 非商业Net | 商业Net | 当月OI | 下月OI | 下下月OI | Reg库存 | 压力比率
    
    【分析重点】：
    - 识别是否存在“移仓换月”延迟导致的实物挤兑风险。
    - 识别商业空头在远期合约上的布局异动。

    【源数据区】：
    CFTC(含解析文本): {json.dumps(cftc_data)}
    OI(含多月份PDF文本): {json.dumps(oi_data)}
    库存: {json.dumps({'Au':au_inv, 'Ag':ag_inv, 'Pt':pt_inv})}
    """
    
    print(">>> 正在请求 Gemini 3.1 Pro 解析 PDF 并构建多月份模型...")
    try:
        response = client.models.generate_content(
            model='gemini-3.1-pro-preview', 
            contents=prompt,
            config={'thinking_config': {'include_thoughts': True}}
        )
        write_to_page_content(response.text)
    except Exception as e:
        print(f"3.1 预览版超时或异常: {e}，改用 1.5-flash 处理...")
        response = client.models.generate_content(model='gemini-1.5-flash', contents=prompt)
        write_to_page_content(response.text)

if __name__ == "__main__":
    main()
