#!/usr/bin/env python3
"""
文档归类整理工具 v4 (修复版)
将work文件夹中的文档按规则分类整理到raw文件夹
"""

import os
import json
import hashlib
import shutil
import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd
import openpyxl
import xlrd

# 配置
SOURCE_DIR = Path(r'C:\work')
TARGET_DIR = Path(r'C:\Users\mingh\client-data\raw')
MASTER_FILE = SOURCE_DIR / '商务信息档案' / '客户主数据_20260306113642.xlsx'
OP_DIR = SOURCE_DIR / '运维工单'

MAPPING_FILE = TARGET_DIR / '_mapping.json'
SYNC_STATE_FILE = TARGET_DIR / '_sync_state.json'


def load_mapping():
    if MAPPING_FILE.exists():
        with open(MAPPING_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_mapping(mapping):
    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    with open(MAPPING_FILE, 'w', encoding='utf-8') as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)


def load_sync_state():
    if SYNC_STATE_FILE.exists():
        with open(SYNC_STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_sync_state(state):
    with open(SYNC_STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def get_file_hash(filepath):
    h = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def generate_mapping_from_master():
    if not MASTER_FILE.exists():
        return {}
    df = pd.read_excel(MASTER_FILE)
    mapping = {}
    for _, row in df.iterrows():
        full_name = row.iloc[1]
        short_name = row.iloc[2]
        if pd.notna(full_name) and pd.notna(short_name):
            mapping[short_name] = full_name
    return mapping


def get_client_from_fullname(fullname, mapping):
    for short, full in mapping.items():
        if full == fullname:
            return short
    return None


def copy_file(src, dst, sync_state):
    dst.parent.mkdir(parents=True, exist_ok=True)
    file_hash = get_file_hash(src)
    src_str = str(src)
    if src_str in sync_state and sync_state[src_str].get('hash') == file_hash:
        return False
    shutil.copy2(src, dst)
    sync_state[src_str] = {'hash': file_hash, 'dst': str(dst)}
    return True


def organize_products():
    """产品手册 -> 产品功能, 产品方案 -> 通用业务方案"""
    sync_state = load_sync_state()
    
    src_dir = SOURCE_DIR / '产品手册'
    dst_dir = TARGET_DIR / '产品功能'
    if src_dir.exists():
        for f in src_dir.glob('*'):
            if f.is_file():
                if copy_file(f, dst_dir / f.name, sync_state):
                    pass
    
    src_dir = SOURCE_DIR / '产品方案'
    dst_dir = TARGET_DIR / '通用业务方案'
    if src_dir.exists():
        for f in src_dir.glob('*'):
            if f.is_file():
                if copy_file(f, dst_dir / f.name, sync_state):
                    pass
    
    save_sync_state(sync_state)


def organize_customer_kb():
    """客户知识库 -> 优秀客户方案 + 客户档案"""
    sync_state = load_sync_state()
    mapping = load_mapping()
    
    kb_dir = SOURCE_DIR / '客户知识库'
    if not kb_dir.exists():
        return
    
    for customer_folder in kb_dir.iterdir():
        if not customer_folder.is_dir():
            continue
        
        customer_name = customer_folder.name
        if customer_name not in mapping:
            continue
        
        for subfolder in customer_folder.iterdir():
            if not subfolder.is_dir():
                continue
            
            subfolder_name = subfolder.name
            
            # 蓝图 -> 优秀客户方案 + 客户档案/蓝图方案
            if '蓝图' in subfolder_name:
                dst1 = TARGET_DIR / '优秀客户方案' / customer_name / '蓝图'
                dst2 = TARGET_DIR / '客户档案' / customer_name / '蓝图方案'
                for f in subfolder.glob('*'):
                    if f.is_file():
                        copy_file(f, dst1 / f.name, sync_state)
                        copy_file(f, dst2 / f.name, sync_state)
            
            # 运维工单 -> 客户档案/运维工单
            elif '运维工单' in subfolder_name:
                dst = TARGET_DIR / '客户档案' / customer_name / '运维工单'
                for f in subfolder.glob('*'):
                    if f.is_file():
                        copy_file(f, dst / f.name, sync_state)
            
            # 一线视图 -> 客户档案/蓝图方案
            elif '一线视图' in subfolder_name:
                dst = TARGET_DIR / '客户档案' / customer_name / '蓝图方案'
                for f in subfolder.glob('*'):
                    if f.is_file():
                        copy_file(f, dst / f.name, sync_state)
            
            # 其他 -> 客户档案/其他文档
            else:
                dst = TARGET_DIR / '客户档案' / customer_name / '其他文档'
                for f in subfolder.glob('*'):
                    if f.is_file():
                        copy_file(f, dst / f.name, sync_state)
    
    save_sync_state(sync_state)


def split_excel_with_openpyxl(src_file, target_dir, mapping, dst_filename, client_col_pattern):
    """使用openpyxl拆分Excel，处理编码问题"""
    try:
        wb = openpyxl.load_workbook(src_file)
        
        # 处理所有sheet
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            header_row = [c.value for c in ws[1]]
            
            # 找客户列
            client_col_idx = None
            for i, col in enumerate(header_row):
                if col and client_col_pattern in str(col):
                    client_col_idx = i
                    break
            
            if client_col_idx is None:
                continue
            
            # 按客户分组
            client_data = {}
            for row_idx in range(2, ws.max_row + 1):
                cell = ws[row_idx][client_col_idx]
                if cell.value:
                    client_name = str(cell.value).strip()
                    if client_name and client_name not in client_data:
                        client_data[client_name] = []
                    if client_name:
                        row_data = [c.value if c.value is not None else '' for c in ws[row_idx]]
                        client_data[client_name].append(row_data)
            
            # 保存每个客户
            for client_full, rows in client_data.items():
                short_name = get_client_from_fullname(client_full, mapping)
                if short_name is None:
                    continue
                
                # 命名：订阅台账.xlsx, 订阅明细.xlsx
                if '明细' in sheet_name:
                    fname = '订阅明细.xlsx'
                else:
                    fname = dst_filename
                
                dst_file = target_dir / short_name / fname
                dst_file.parent.mkdir(parents=True, exist_ok=True)
                
                df = pd.DataFrame(rows, columns=header_row)
                df.to_excel(dst_file, index=False)
        
        return True
    except Exception as e:
        print(f'  错误: {e}')
        return False


def split_excel_with_xlrd(src_file, target_dir, mapping, client_col_idx):
    """使用xlrd读取xls文件并拆分"""
    try:
        wb = xlrd.open_workbook(src_file)
        ws = wb.sheet_by_index(0)
        
        header_row = [ws.cell_value(0, col) for col in range(ws.ncols)]
        
        client_data = {}
        for row_idx in range(1, ws.nrows):
            cell = ws.cell_value(row_idx, client_col_idx)
            if cell:
                client_name = str(cell).strip()
                if client_name and client_name not in client_data:
                    client_data[client_name] = []
                if client_name:
                    row_data = [ws.cell_value(row_idx, col) for col in range(ws.ncols)]
                    client_data[client_name].append(row_data)
        
        for client_full, rows in client_data.items():
            short_name = get_client_from_fullname(client_full, mapping)
            if short_name is None:
                continue
            
            dst_file = target_dir / short_name / '订阅合同收款情况' / '订阅合同收款情况.xlsx'
            dst_file.parent.mkdir(parents=True, exist_ok=True)
            
            df = pd.DataFrame(rows, columns=header_row)
            df.to_excel(dst_file, index=False)
        
        return True
    except Exception as e:
        print(f'  错误: {e}')
        return False


def organize_business_summary():
    """拆分商务汇总表"""
    sync_state = load_sync_state()
    mapping = load_mapping()
    
    biz_dir = SOURCE_DIR / '商务信息档案'
    if not biz_dir.exists():
        return
    
    # 客户主数据
    for f in biz_dir.glob('*客户主数据*'):
        if f.is_file():
            split_excel_with_openpyxl(f, TARGET_DIR / '客户档案', mapping, '客户主数据.xlsx', '真实服务对象')
    
    # 订阅台账 (2个sheet)
    for f in biz_dir.glob('*订阅台账*'):
        if f.is_file():
            split_excel_with_openpyxl(f, TARGET_DIR / '客户档案', mapping, '订阅台账.xlsx', '真实服务对象')
    
    # 固定金额台账
    for f in biz_dir.glob('*固定金额*'):
        if f.is_file():
            split_excel_with_openpyxl(f, TARGET_DIR / '客户档案', mapping, '固定金额台账.xlsx', '最终服务对象')
    
    # 人天框架台账
    for f in biz_dir.glob('*人天框架*'):
        if f.is_file():
            split_excel_with_openpyxl(f, TARGET_DIR / '客户档案', mapping, '人天框架台账.xlsx', '最终服务对象')
    
    # 项目收款进度查询 (.xls)
    for f in biz_dir.glob('*项目收款*'):
        if f.is_file() and f.suffix == '.xls':
            # 找到客户列
            wb = xlrd.open_workbook(f)
            ws = wb.sheet_by_index(0)
            header_row = [ws.cell_value(0, col) for col in range(ws.ncols)]
            client_col_idx = None
            for i, col in enumerate(header_row):
                if col and '项目归属客户' in str(col):
                    client_col_idx = i
                    break
            if client_col_idx:
                split_excel_with_xlrd(f, TARGET_DIR / '客户档案', mapping, client_col_idx)
    
    save_sync_state(sync_state)


def merge_folders():
    """合并文件夹：固定金额+人天框架 -> 实施合同行"""
    base = TARGET_DIR / '客户档案'
    
    for client_dir in base.iterdir():
        if not client_dir.is_dir():
            continue
        
        # 合并固定金额
        fixed = client_dir / '实施合同行_固定金额'
        target = client_dir / '实施合同行'
        if fixed.exists():
            target.mkdir(parents=True, exist_ok=True)
            for f in fixed.glob('*'):
                if f.is_file():
                    dst = target / f.name
                    shutil.move(str(f), str(dst))
            fixed.rmdir()
        
        # 合并人天框架
        day = client_dir / '实施合同行_人天框架'
        if day.exists():
            target.mkdir(parents=True, exist_ok=True)
            for f in day.glob('*'):
                if f.is_file():
                    dst = target / f.name
                    shutil.move(str(f), str(dst))
            day.rmdir()


def move_files_to_subfolder():
    """移动根目录下的文件到正确子目录"""
    base = TARGET_DIR / '客户档案'
    
    for client_dir in base.iterdir():
        if not client_dir.is_dir():
            continue
        
        # 移动订阅合同收款情况.xlsx到订阅合同收款情况目录
        for f in client_dir.glob('订阅合同收款情况.xlsx'):
            target_dir = client_dir / '订阅合同收款情况'
            target_dir.mkdir(exist_ok=True)
            dst = target_dir / f.name
            f.replace(dst)
        
        # 删除重复的项目收款.xlsx
        sq = client_dir / '订阅合同收款情况'
        if sq.exists():
            proj = sq / '项目收款.xlsx'
            if proj.exists():
                proj.unlink()


def organize_work_orders():
    """运维工单"""
    sync_state = load_sync_state()
    mapping = load_mapping()
    
    if not OP_DIR.exists():
        return
    
    for f in OP_DIR.glob('*.xlsx'):
        try:
            df = pd.read_excel(f)
        except:
            continue
        
        client_col = None
        for col in df.columns:
            if '客户' in str(col) and '名称' in str(col):
                client_col = col
                break
        
        if client_col is None:
            continue
        
        for client_full in df[client_col].dropna().unique():
            client_full = str(client_full).strip()
            
            short_name = get_client_from_fullname(client_full, mapping)
            if short_name is None:
                continue
            
            client_df = df[df[client_col] == client_full]
            if len(client_df) == 0:
                continue
            
            dst_file = TARGET_DIR / '客户档案' / short_name / '运维工单' / f.name
            dst_file.parent.mkdir(parents=True, exist_ok=True)
            
            if dst_file.exists():
                existing = pd.read_excel(dst_file)
                if '编号' in existing.columns and '编号' in client_df.columns:
                    combined = pd.concat([existing, client_df]).drop_duplicates(subset=['编号'], keep='last')
                else:
                    combined = pd.concat([existing, client_df])
                combined.to_excel(dst_file, index=False)
            else:
                client_df.to_excel(dst_file, index=False)
        
        sync_state[str(f)] = {'hash': get_file_hash(f)}
    
    save_sync_state(sync_state)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--full', action='store_true')
    parser.add_argument('--mapping-only', action='store_true')
    args = parser.parse_args()
    
    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    
    if args.full:
        if SYNC_STATE_FILE.exists():
            SYNC_STATE_FILE.unlink()
    
    # 映射表
    mapping = generate_mapping_from_master()
    save_mapping(mapping)
    print(f'映射表: {len(mapping)}个客户')
    
    if args.mapping_only:
        return
    
    print()
    print('=== 产品和方案 ===')
    organize_products()
    
    print()
    print('=== 客户知识库 ===')
    organize_customer_kb()
    
    print()
    print('=== 商务汇总表拆分 ===')
    organize_business_summary()
    
    print()
    print('=== 合并文件夹 ===')
    merge_folders()
    
    print()
    print('=== 移动文件到子目录 ===')
    move_files_to_subfolder()
    
    print()
    print('=== 运维工单 ===')
    organize_work_orders()
    
    print()
    print('=== 完成 ===')


if __name__ == '__main__':
    main()
