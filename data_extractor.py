import os
import requests
import json
from datetime import datetime, timedelta

NOTION_TOKEN = os.environ.get("NOTION_TOKEN")
HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# --- 1. 数据库 ID 配置 ---
DB_CFTC = "2dc47eb5fd3c80e48d59e0d46494752e"
DB_OI = "2f947eb5fd3c8068b034c7ebc74b88d0"
DB_SILVER = "2bc47eb5fd3c80f3a71ad8de149a4943"
DB_PT = "2d647eb5fd3c801a9ce5d5db4d0b961a"
DB_GOLD = "2bc47eb5fd3c8083966eecfd9f396b44"

# --- 2. 真实列名映射字典（高容错处理） ---
# 左侧是代码内部调用的变量名，右侧是您 Notion 中实际的列名称。请核对右侧字符串！
COLUMN_MAP = {
    "date_col": "Date",             # 所有表格中记录时间的列名
    "file_col": "Files & media",    # CFTC 和 OI 表格中附件的列名
    "market_col": "Market",         # 库存表格中记录 CME 市场的列名
    "reg_col": "Reg",               # 库存表格中 Registered 确切数值的列名
    "elig_col": "Elig"              # 库存表格中 Eligible 确切数值的列名
}

def get_past_28_days_filter(date_column_name):
    # 生成 28 天前的 ISO 时间
    past_date = (datetime.utcnow() - timedelta(days=28)).isoformat()
    return {
        "property": date_column_name,
        "date": {"on_or_after": past_date}
    }

def safe_extract_property(properties, col_name, prop_type):
    """安全提取 Notion 属性，找不到列名或数据为空时返回 None 并提示"""
    if col_name not in properties:
        print(f"[警告] 致命错误：在数据行中找不到列名 '{col_name}'。请检查 COLUMN_MAP 或 Notion 表格结构！")
        return None
    
    prop_data = properties[col_name]
    try:
        if prop_type == "date":
            return prop_data['date']['start']
        elif prop_type == "number":
            return prop_data['number']
        elif prop_type == "rich_text":
            return prop_data['rich_text'][0]['plain_text'] if prop_data['rich_text'] else "N/A"
        elif prop_type == "select":
            return prop_data['select']['name'] if prop_data['select'] else "N/A"
        elif prop_type == "files":
            if prop_data['files'] and len(prop_data['files']) > 0:
                file_info = prop_data['files'][0]
                # 判断是外部直链还是 Notion 托管的文件
                if file_info['type'] == 'external':
                    return file_info['external']['url']
                elif file_info['type'] == 'file':
                    return file_info['file']['url']
            return None
    except KeyError as e:
        print(f"[警告] 列 '{col_name}' 的数据结构与预期 {prop_type} 不符: {e}")
        return None
    return "N/A"

def fetch_inventory_data(db_id, asset_name):
    """拉取并解析黄金/白银/PT的库存数据"""
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    # 构建查询条件：时间过去28天，且 Market = CME
    payload = {
        "filter": {
            "and": [
                get_past_28_days_filter(COLUMN_MAP["date_col"]),
                {
                    "property": COLUMN_MAP["market_col"],
                    "select": {"equals": "CME"}
                }
            ]
        }
    }
    
    response = requests.post(url, headers=HEADERS, json=payload)
    response.raise_for_status()
    results = response.json().get("results", [])
    
    inventory_records = []
    for row in results:
        props = row["properties"]
        date_val = safe_extract_property(props, COLUMN_MAP["date_col"], "date")
        reg_val = safe_extract_property(props, COLUMN_MAP["reg_col"], "number")
        elig_val = safe_extract_property(props, COLUMN_MAP["elig_col"], "number")
        
        # 绝对遵守严谨原则：若缺乏确切数值，标为 N/A
        inventory_records.append({
            "Date": date_val if date_val else "N/A",
            "Asset": asset_name,
            "Reg": reg_val if reg_val is not None else "N/A",
            "Elig": elig_val if elig_val is not None else "N/A"
        })
    return inventory_records

def fetch_and_download_file_data(db_id, source_name):
    """拉取 CFTC/OI 数据并直接下载文本附件内容"""
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    payload = {"filter": get_past_28_days_filter(COLUMN_MAP["date_col"])}
    
    response = requests.post(url, headers=HEADERS, json=payload)
    response.raise_for_status()
    results = response.json().get("results", [])
    
    file_records = []
    for row in results:
        props = row["properties"]
        date_val = safe_extract_property(props, COLUMN_MAP["date_col"], "date")
        file_url = safe_extract_property(props, COLUMN_MAP["file_col"], "files")
        
        file_content = "数据获取中或无附件"
        if file_url:
            try:
                # 临时下载附件内容至内存
                file_res = requests.get(file_url)
                if file_res.status_code == 200:
                    # 假定附件为纯文本格式（TXT/CSV）
                    file_content = file_res.text[:2000] # 为控制 Token，可按需截断或进一步正则提取
            except Exception as e:
                file_content = f"文件下载失败: {e}"
                
        file_records.append({
            "Date": date_val if date_val else "N/A",
            "Source": source_name,
            "Content": file_content
        })
    return file_records

# --- 本地测试运行 ---
if __name__ == "__main__":
    print("开始测试抓取 CME 白银过去 4 周库存数据...")
    ag_data = fetch_inventory_data(DB_SILVER, "Silver")
    print(json.dumps(ag_data, indent=2, ensure_ascii=False))
