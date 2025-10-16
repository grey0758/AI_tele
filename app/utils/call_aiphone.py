"""
帮我不断请求这个接口，隔30秒请求一次，直到请求成功为止
"""
import pandas as pd
import time
import os
import requests

def read_csv_data():
    """读取CSV文件中的电话号码数据"""
    csv_path = os.path.join(os.path.dirname(__file__), '四月线索表.csv')
    
    print(f"尝试读取CSV文件: {csv_path}")
    print(f"文件是否存在: {os.path.exists(csv_path)}")
    
    if not os.path.exists(csv_path):
        print(f"CSV文件不存在: {csv_path}")
        return []
    
    try:
        df = pd.read_csv(csv_path, header=None, names=['id', 'phone'])
        print(f"CSV数据形状: {df.shape}")
        print(f"前5行数据:")
        print(df.head())
        
        phone_data = []
        
        for index, row in df.iterrows():
            if pd.notna(row['phone']):
                try:
                    phone_value = str(int(row['phone'])).strip()
                    if phone_value.isdigit() and len(phone_value) >= 10:
                        phone_data.append({
                            'id': row['id'],
                            'phone': phone_value,
                            'row_index': index
                        })
                    else:
                        print(f"跳过非电话号码数据: {phone_value}")
                except Exception as e:
                    print(f"处理第{index}行数据时出错: {e}")
                    continue
        
        print(f"成功读取到 {len(phone_data)} 个电话号码")
        return phone_data

    except (FileNotFoundError, pd.errors.EmptyDataError) as e:
        print(f"读取CSV文件时出错: {e}")
        return []

def update_csv_marking(row_index, code):
    """更新CSV文件中的拨打标记"""
    csv_path = os.path.join(os.path.dirname(__file__), '四月线索表.csv')
    
    try:
        df = pd.read_csv(csv_path, header=None, names=['id', 'phone'])
        
        if 'status' not in df.columns:
            df['status'] = ''
        
        if code == 429:
            df.at[row_index, 'status'] = 1
        
        df.to_csv(csv_path, header=False, index=False)
        
    except (FileNotFoundError, pd.errors.EmptyDataError) as e:
        print(f"更新CSV文件时出错: {e}")

def make_call_request(phone_number, device_index=0, tts_opening=""):
    """发起拨打电话请求"""
    url = 'http://localhost:8020/api/v1/aicall/make_call'
    headers = {
        'Content-Type': 'application/json'
    }
    
    data = {
        "phone_number": phone_number,
        "device_index": device_index,
        "tts_opening": tts_opening,
        "custom_id": None
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=30)
        return response.json()
    except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
        print(f"请求失败: {e}")
        return {"code": -1, "message": str(e)}

def call_phone_continuously():
    """不断请求接口直到成功"""
    phone_data = read_csv_data()
    
    if not phone_data:
        print("没有找到电话号码数据")
        return
    
    for phone_info in phone_data:
        phone_number = phone_info['phone']
        row_index = phone_info['row_index']
        phone_id = phone_info['id']
        
        print(f"开始拨打 ID: {phone_id}, 电话: {phone_number}")
        
        while True:
            response = make_call_request(phone_number)
            code = response.get('code', -1)
            message = response.get('message', '')
            
            print(f"请求结果 - Code: {code}, Message: {message}")
            
            if code == 429:
                print(f"拨打错误 (429): {phone_number}, 30秒后重试...")
                time.sleep(30)
            else:
                print(f"拨打成功 (code: {code}): {phone_number}")
                break
        
        print(f"完成拨打: {phone_number}\n")

if __name__ == '__main__':
    call_phone_continuously()
