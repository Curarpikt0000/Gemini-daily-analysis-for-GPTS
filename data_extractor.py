import os
import requests
import json
from datetime import datetime, timedelta
from google import genai

# --- 1. 核心鉴权配置 (从环境变量读取，确保 GitHub 安全) ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# 检查环境变量是否存在
if not NOTION_TOKEN or not GEMINI_API_KEY:
    print("错误: 请设置环境变量 NOTION_TOKEN 和 GEMINI_API_KEY")
    exit(1)

# 初始化 Gemini 客户端
client = genai.Client(api_key=GEMINI_API_KEY)

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# --- 2. 数据库映射配置 ---
DB_TARGET = "32747eb5fd3c800e9c3bfe9b461aab94"  # 目标：Gemini 分析数据库

DB_CONFIG = {
    "CFTC": {"id": "2dc47eb5fd3c80e48d59e0d46494752e", "date_col": "Date", "file_col": "Files & media"},
    "OI": {"id": "2f947eb5fd3c8068b034c7ebc74b88d0", "date_col": "Date", "file_col": "File"},
    "SILVER": {"id": "2bc47eb5fd3c80f3a71ad8de149a4943", "date_col": "Silver日期", "reg_col": "Silver Reg库存", "elig_col": "Silver Elig库存", "market_col": "市场"},
    "PT": {"id": "2d647eb5fd3c801a9ce5d5db4d0b961a", "date_col": "Pt日期", "reg_col": "Pt Reg库存", "elig_col": "Pt Elig库存", "market_col": "市场"},
    "GOLD": {"id": "2bc47eb5fd3c8083966eecfd9f396b44", "date_col": "Gold日期", "reg_col": "Gold Reg库存", "elig_col": "Gold Elig库存", "market_col": "市场"}
}

# --- 3. 数据抓取逻辑 ---

def get_past_28_days_filter(date_column_name):
    """获取过去28天的过滤条件"""
    past_date = (datetime.utcnow() - timedelta(days=28)).isoformat()
    return {"property": date_column_name, "date": {"on_or_after": past_date}}

def safe_extract_property(properties, col_name, prop_type):
    """安全提取 Notion 属性"""
    if col_name not in properties: return None
    prop_data = properties[col_name]
    try:
        if prop_type == "date": return prop_data['date']['start'] if prop_data['date'] else None
        elif prop_type == "number": return prop_data['number']
        elif prop_type == "files":
            if prop_data['files'] and len(prop_data['files']) > 0:
                file_info = prop_data['files'][0]
                return file_info['external']['url'] if file_info['type'] == 'external' else file_info['file']['url']
            return None
    except:
        return None
    return "N/A"

def fetch_inventory_data(db_name):
    """抓取 CME 金属库存数据"""
    config = DB_CONFIG[db_name]
    url = f"https://api.notion.com/v1/databases/{config['id']}/query"
    payload = {
        "filter": {
            "and": [
                get_past_28_days_filter(config["date_col"]), 
                {"property": config["market_col"], "select": {"equals": "CME"}}
            ]
        }
    }
    response = requests.post(url, headers=HEADERS, json=payload)
    results = response.json().get("results", [])
    
    records = []
    for row in results:
        props = row["properties"]
        records.append({
            "Date": safe_extract_property(props, config["date_col"], "date") or "N/A",
            "Reg": safe_extract_property(props, config["reg_col"], "number") or "N/A",
            "Elig": safe_extract_property(props, config["elig_col"], "number") or "N/A"
        })
    return records

def fetch_and_download_file_data(db_name):
    """抓取并解析 CFTC/OI 文件内容"""
    config = DB_CONFIG[db_name]
    url = f"https://api.notion.com/v1/databases/{config['id']}/query"
    payload = {"filter": get_past_28_days_filter(config["date_col"])}
    response = requests.post(url, headers=HEADERS, json=payload)
    results = response.json().get("results", [])
    
    records = []
    for row in results:
        props = row["properties"]
        file_url = safe_extract_property(props, config["file_col"], "files")
        content = "N/A"
        if file_url:
            try:
                # 限制长度防止 Token 过载
                content = requests.get(file_url).text[:2500] 
            except:
                pass
        records.append({
            "Date": safe_extract_property(props, config["date_col"], "date") or "N/A",
            "Content": content
        })
    return records

# --- 4. 结果回写逻辑 ---

def write_to_notion_analysis(analysis_text):
    """将分析长文切片并写入 Notion"""
    print(">>> 正在将分析结果回写至 Notion ...")
    url = "https://api.notion.com/v1/pages"
    # 获取东京时间 (JST) 用于标题展示
    jst_now = datetime.utcnow() + timedelta(hours=9)
    today_str = jst_now.strftime("%Y-%m-%d")
    
    # 自动切片处理（Notion 单个 text 块上限 2000 字符）
    text_chunks = [analysis_text[i:i+1999] for i in range(0, len(analysis_text), 1999)]
    rich_text_array = [{"text": {"content": chunk}} for chunk in text_chunks]

    payload = {
        "parent": {"database_id": DB_TARGET},
        "properties": {
            "Name": {"title": [{"text": {"content": f"Daily Analysis Report: {today_str}"}}]},
            "Date": {"date": {"start": today_str}},
            "Analysis": {"rich_text": rich_text_array}
        }
    }
    res = requests.post(url, headers=HEADERS, json=payload)
    if res.status_code == 200:
        print(">>> 成功写入 Notion 数据库！")
    else:
        print(f">>> 写入失败: {res.text}")

# --- 5. 主程序 ---

def main():
    print(">>> 正在从 5 个数据库并行拉取过去 4 周数据...")
    cftc_data = fetch_and_download_file_data("CFTC")
    oi_data = fetch_and_download_file_data("OI")
    ag_data = fetch_inventory_data("SILVER")
    pt_data = fetch_inventory_data("PT")
    au_data = fetch_inventory_data("GOLD")
    
    prompt = f"""
    你是一个严谨的贵金属量化分析师。请分析以下过去四周的数据（CFTC持仓、OI未平仓量、CME黄金/白银/铂金库存储备）。
    
    【强制红线纪律】：
    1. 所有数值必须基于我提供的数据。如果对应日期的确切数值缺失或为 'N/A'，该单元格/分析项必须标注为 "数据获取中" 或 "N/A"。
    2. 绝对禁止基于任何算法进行时间序列的趋势外推或数值模拟。
    
    【分析要求】：
    1. 提炼黄金、白银、PT的多头和空头趋势、异动情况及未平仓量变化。
    2. 结合对应的 Reg (Registered) 和 Elig (Eligible) 库存变化，重点分析近期的实物逼空风险（如：OI未平仓量 / Reg可用库存的比例变化）。
    3. 输出要求：先概括要点；正文较长时请分条列出；并在结尾给出3个明确的市场跟踪建议。
    
    【源数据】：
    [CFTC数据]: {json.dumps(cftc_data, ensure_ascii=False)}
    [OI未平仓量]: {json.dumps(oi_data, ensure_ascii=False)}
    [Gold库存]: {json.dumps(au_data, ensure_ascii=False)}
    [Silver库存]: {json.dumps(ag_data, ensure_ascii=False)}
    [PT库存]: {json.dumps(pt_data, ensure_ascii=False)}
    """
    
    print(">>> 正在请求 Gemini 3.1 Pro 旗舰模型进行深度分析...")
    
    try:
        response = client.models.generate_content(
            model='gemini-3.1-pro-preview', 
            contents=prompt,
        )
        print(">>> 分析完成，准备回写...")
        write_to_notion_analysis(response.text)
    except Exception as e:
        print(f">>> Gemini API 请求失败: {str(e)}")
        # 如果 3.1 失败（如配额突发限制），可在这里增加自动降级逻辑
        print(">>> 尝试使用 1.5-flash 进行降级处理...")
        response = client.models.generate_content(model='gemini-1.5-flash', contents=prompt)
        write_to_notion_analysis(response.text)

if __name__ == "__main__":
    main()
