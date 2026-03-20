#!/usr/bin/env python3
"""
文档归类整理工具 v5
将work文件夹中的文档按规则分类整理到raw文件夹
- 增量同步 + 快照覆盖 + 客户映射表
"""

import os
import json
import hashlib
import shutil
import argparse
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
import openpyxl
import xlrd

# === 配置加载 ===
CONFIG_FILE = Path(__file__).parent.parent / 'config.json'


def load_config():
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def get_path(key, default):
    cfg = load_config()
    val = cfg.get(key, default)
    return Path(val) if val else Path(default)


# 路径配置（可外置到 config.json）
SOURCE_DIR = get_path('source_dir', r'C:\work')
TARGET_DIR = get_path('target_dir', r'C:\Users\mingh\client-data\raw')
LOG_DIR = get_path('log_dir', Path(__file__).parent.parent / 'logs')

MASTER_FILE = SOURCE_DIR / '商务信息档案' / '客户主数据_20260306113642.xlsx'
OP_DIR = SOURCE_DIR / '运维工单'

MAPPING_FILE = TARGET_DIR / '_mapping.json'
SYNC_STATE_FILE = TARGET_DIR / '_sync_state.json'


# === 日志配置 ===
def setup_logging(dry_run=False):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f'organize_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
    level = logging.INFO
    fmt = '[%(asctime)s] %(message)s'
    date_fmt = '%H:%M:%S'
    
    # 文件日志
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(fmt, date_fmt))
    
    # 控制台日志
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(fmt, date_fmt))
    
    logger = logging.getLogger('doc-organizer')
    logger.setLevel(level)
    logger.handlers = []
    logger.addHandler(fh)
    logger.addHandler(ch)
    
    return logger, log_file


# === 状态管理 ===
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


# === Dry-run 记录 ===
class DryRunCounter:
    def __init__(self):
        self.count = 0
        self.files = []

    def record(self, action, src, dst):
        self.count += 1
        self.files.append((action, str(src), str(dst)))


def copy_file(src, dst, sync_state, logger, dry_run=False, dry_counter=None):
    dst.parent.mkdir(parents=True, exist_ok=True)
    file_hash = get_file_hash(src)
    src_str = str(src)
    
    if src_str in sync_state and sync_state[src_str].get('hash') == file_hash:
        return False
    
    if dry_run:
        dry_counter.record('复制', src, dst)
        return False
    
    shutil.copy2(src, dst)
    sync_state[src_str] = {'hash': file_hash, 'dst': str(dst)}
    logger.info(f'  新增/更新: {src.name} -> {dst.parent.name}/{dst.name}')
    return True


def write_excel(df, dst_file, logger, dry_run=False, dry_counter=None):
    dst_file.parent.mkdir(parents=True, exist_ok=True)
    if dry_run:
        dry_counter.record('写入Excel', dst_file.name, dst_file.parent)
        return
    df.to_excel(dst_file, index=False)


def organize_products(logger, sync_state, dry_run=False, dry_counter=None):
    """产品手册 -> 产品功能, 产品方案 -> 通用业务方案"""
    logger.info('=== 产品和方案 ===')
    
    src_dir = SOURCE_DIR / '产品手册'
    dst_dir = TARGET_DIR / '产品功能'
    if src_dir.exists():
        for f in src_dir.glob('*'):
            if f.is_file():
                if copy_file(f, dst_dir / f.name, sync_state, logger, dry_run, dry_counter):
                    pass
    else:
        logger.info(f'  目录不存在: {src_dir}')
    
    src_dir = SOURCE_DIR / '产品方案'
    dst_dir = TARGET_DIR / '通用业务方案'
    if src_dir.exists():
        for f in src_dir.glob('*'):
            if f.is_file():
                if copy_file(f, dst_dir / f.name, sync_state, logger, dry_run, dry_counter):
                    pass
    else:
        logger.info(f'  目录不存在: {src_dir}')


def organize_customer_kb(logger, sync_state, mapping, dry_run=False, dry_counter=None):
    """客户知识库 -> 优秀客户方案 + 客户档案"""
    logger.info('=== 客户知识库 ===')
    
    kb_dir = SOURCE_DIR / '客户知识库'
    if not kb_dir.exists():
        logger.info(f'  目录不存在: {kb_dir}')
        return
    
    for customer_folder in kb_dir.iterdir():
        if not customer_folder.is_dir():
            continue
        
        customer_name = customer_folder.name
        if customer_name not in mapping:
            logger.info(f'  跳过（不在映射表）: {customer_name}')
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
                        copy_file(f, dst1 / f.name, sync_state, logger, dry_run, dry_counter)
                        copy_file(f, dst2 / f.name, sync_state, logger, dry_run, dry_counter)
            
            # 运维工单 -> 客户档案/运维工单
            elif '运维工单' in subfolder_name:
                dst = TARGET_DIR / '客户档案' / customer_name / '运维工单'
                for f in subfolder.glob('*'):
                    if f.is_file():
                        copy_file(f, dst / f.name, sync_state, logger, dry_run, dry_counter)
            
            # 一线视图 -> 客户档案/蓝图方案
            elif '一线视图' in subfolder_name:
                dst = TARGET_DIR / '客户档案' / customer_name / '蓝图方案'
                for f in subfolder.glob('*'):
                    if f.is_file():
                        copy_file(f, dst / f.name, sync_state, logger, dry_run, dry_counter)
            
            # 其他 -> 客户档案/其他文档
            else:
                dst = TARGET_DIR / '客户档案' / customer_name / '其他文档'
                for f in subfolder.glob('*'):
                    if f.is_file():
                        copy_file(f, dst / f.name, sync_state, logger, dry_run, dry_counter)


def split_excel_with_openpyxl(src_file, target_dir, mapping, dst_filename, client_col_pattern, logger, dry_run=False, dry_counter=None):
    """使用openpyxl拆分Excel"""
    try:
        wb = openpyxl.load_workbook(src_file)
        
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            header_row = [c.value for c in ws[1]]
            
            client_col_idx = None
            for i, col in enumerate(header_row):
                if col and client_col_pattern in str(col):
                    client_col_idx = i
                    break
            
            if client_col_idx is None:
                continue
            
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
            
            for client_full, rows in client_data.items():
                short_name = get_client_from_fullname(client_full, mapping)
                if short_name is None:
                    continue
                
                if '明细' in sheet_name:
                    fname = '订阅明细.xlsx'
                else:
                    fname = dst_filename
                
                dst_file = target_dir / short_name / fname
                df = pd.DataFrame(rows, columns=header_row)
                write_excel(df, dst_file, logger, dry_run, dry_counter)
        
        return True
    except Exception as e:
        logger.error(f'  错误: {e}')
        return False


def split_excel_with_xlrd(src_file, target_dir, mapping, client_col_idx, logger, dry_run=False, dry_counter=None):
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
            df = pd.DataFrame(rows, columns=header_row)
            write_excel(df, dst_file, logger, dry_run, dry_counter)
        
        return True
    except Exception as e:
        logger.error(f'  错误: {e}')
        return False


def organize_business_summary(logger, sync_state, mapping, dry_run=False, dry_counter=None):
    """拆分商务汇总表"""
    logger.info('=== 商务汇总表拆分 ===')
    
    biz_dir = SOURCE_DIR / '商务信息档案'
    if not biz_dir.exists():
        logger.info(f'  目录不存在: {biz_dir}')
        return
    
    # 客户主数据
    for f in biz_dir.glob('*客户主数据*'):
        if f.is_file():
            split_excel_with_openpyxl(f, TARGET_DIR / '客户档案', mapping, '客户主数据.xlsx', '真实服务对象', logger, dry_run, dry_counter)
    
    # 订阅台账
    for f in biz_dir.glob('*订阅台账*'):
        if f.is_file():
            split_excel_with_openpyxl(f, TARGET_DIR / '客户档案', mapping, '订阅台账.xlsx', '真实服务对象', logger, dry_run, dry_counter)
    
    # 固定金额台账
    for f in biz_dir.glob('*固定金额*'):
        if f.is_file():
            split_excel_with_openpyxl(f, TARGET_DIR / '客户档案', mapping, '固定金额台账.xlsx', '最终服务对象', logger, dry_run, dry_counter)
    
    # 人天框架台账
    for f in biz_dir.glob('*人天框架*'):
        if f.is_file():
            split_excel_with_openpyxl(f, TARGET_DIR / '客户档案', mapping, '人天框架台账.xlsx', '最终服务对象', logger, dry_run, dry_counter)
    
    # 项目收款进度查询 (.xls)
    for f in biz_dir.glob('*项目收款*'):
        if f.is_file() and f.suffix == '.xls':
            wb = xlrd.open_workbook(f)
            ws = wb.sheet_by_index(0)
            header_row = [ws.cell_value(0, col) for col in range(ws.ncols)]
            client_col_idx = None
            for i, col in enumerate(header_row):
                if col and '项目归属客户' in str(col):
                    client_col_idx = i
                    break
            if client_col_idx:
                split_excel_with_xlrd(f, TARGET_DIR / '客户档案', mapping, client_col_idx, logger, dry_run, dry_counter)


def merge_folders(logger, dry_run=False, dry_counter=None):
    """合并文件夹：固定金额+人天框架 -> 实施合同行"""
    logger.info('=== 合并文件夹 ===')
    
    base = TARGET_DIR / '客户档案'
    if not base.exists():
        return
    
    for client_dir in base.iterdir():
        if not client_dir.is_dir():
            continue
        
        fixed = client_dir / '实施合同行_固定金额'
        target = client_dir / '实施合同行'
        if fixed.exists():
            target.mkdir(parents=True, exist_ok=True)
            for f in fixed.glob('*'):
                if f.is_file():
                    if dry_run:
                        dry_counter.record('移动', f, target / f.name)
                    else:
                        dst = target / f.name
                        shutil.move(str(f), str(dst))
            if not dry_run:
                fixed.rmdir()
        
        day = client_dir / '实施合同行_人天框架'
        if day.exists():
            target.mkdir(parents=True, exist_ok=True)
            for f in day.glob('*'):
                if f.is_file():
                    if dry_run:
                        dry_counter.record('移动', f, target / f.name)
                    else:
                        dst = target / f.name
                        shutil.move(str(f), str(dst))
            if not dry_run:
                day.rmdir()


def move_files_to_subfolder(logger, dry_run=False, dry_counter=None):
    """移动根目录下的文件到正确子目录"""
    logger.info('=== 移动文件到子目录 ===')
    
    base = TARGET_DIR / '客户档案'
    if not base.exists():
        return
    
    for client_dir in base.iterdir():
        if not client_dir.is_dir():
            continue
        
        for f in client_dir.glob('订阅合同收款情况.xlsx'):
            target_dir = client_dir / '订阅合同收款情况'
            target_dir.mkdir(exist_ok=True)
            dst = target_dir / f.name
            if dry_run:
                dry_counter.record('移动', f, dst)
            else:
                f.replace(dst)
        
        sq = client_dir / '订阅合同收款情况'
        if sq.exists():
            proj = sq / '项目收款.xlsx'
            if proj.exists() and not dry_run:
                proj.unlink()


def organize_work_orders(logger, sync_state, mapping, dry_run=False, dry_counter=None):
    """运维工单"""
    logger.info('=== 运维工单 ===')
    
    if not OP_DIR.exists():
        logger.info(f'  目录不存在: {OP_DIR}')
        return
    
    for f in OP_DIR.glob('*.xlsx'):
        df = None
        # 遍历所有 sheet，找到含"客户名称"列的那个
        try:
            xl = pd.ExcelFile(f)
            for sheet in xl.sheet_names:
                try:
                    _df = xl.parse(sheet)
                    for col in _df.columns:
                        if '客户' in str(col) and '名称' in str(col):
                            df = _df
                            break
                    if df is not None:
                        break
                except Exception:
                    continue
        except Exception:
            continue

        if df is None or df.empty:
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
                try:
                    existing = pd.read_excel(dst_file)
                except Exception:
                    logger.warning(f'  目标文件损坏，将被覆盖: {dst_file}')
                    existing = None
                if existing is not None:
                    if '编号' in existing.columns and '编号' in client_df.columns:
                        combined = pd.concat([existing, client_df]).drop_duplicates(subset=['编号'], keep='last')
                    else:
                        combined = pd.concat([existing, client_df])
                    write_excel(combined, dst_file, logger, dry_run, dry_counter)
                else:
                    write_excel(client_df, dst_file, logger, dry_run, dry_counter)
            else:
                write_excel(client_df, dst_file, logger, dry_run, dry_counter)
        
        sync_state[str(f)] = {'hash': get_file_hash(f)}


def print_dry_run_summary(dry_counter):
    if dry_counter.count == 0:
        print('  (无任何文件需要处理)')
        return
    
    print(f'  共 {dry_counter.count} 项操作待执行:')
    for action, src, dst in dry_counter.files:
        print(f'  [{action}] {Path(src).name} -> {Path(dst).parent.name}/{Path(dst).name}')


def main():
    parser = argparse.ArgumentParser(description='文档归类整理工具')
    parser.add_argument('--full', action='store_true', help='强制全量刷新（清除增量状态）')
    parser.add_argument('--mapping-only', action='store_true', help='仅更新客户映射表')
    parser.add_argument('--dry-run', action='store_true', help='预览模式（不执行，只显示将要进行的操作）')
    args = parser.parse_args()
    
    dry_run = args.dry_run
    
    # 日志初始化
    logger, log_file = setup_logging(dry_run)
    dry_counter = DryRunCounter()
    
    if dry_run:
        logger.info('【预览模式】以下操作将被执行:')
    
    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    
    if args.full:
        if SYNC_STATE_FILE.exists():
            SYNC_STATE_FILE.unlink()
            logger.info('已清除增量状态（全量刷新）')
    
    # 映射表
    mapping = generate_mapping_from_master()
    save_mapping(mapping)
    logger.info(f'映射表: {len(mapping)}个客户')
    
    if args.mapping_only:
        logger.info(f'日志已保存: {log_file}')
        return
    
    logger.info('')
    organize_products(logger, load_sync_state(), dry_run, dry_counter)
    
    logger.info('')
    organize_customer_kb(logger, load_sync_state(), mapping, dry_run, dry_counter)
    
    logger.info('')
    organize_business_summary(logger, load_sync_state(), mapping, dry_run, dry_counter)
    
    logger.info('')
    merge_folders(logger, dry_run, dry_counter)
    
    logger.info('')
    move_files_to_subfolder(logger, dry_run, dry_counter)
    
    logger.info('')
    organize_work_orders(logger, load_sync_state(), mapping, dry_run, dry_counter)
    
    logger.info('')
    if dry_run:
        print_dry_run_summary(dry_counter)
        logger.info('【预览模式结束】如需执行，请去掉 --dry-run 参数')
    else:
        logger.info('=== 完成 ===')
    
    logger.info(f'日志已保存: {log_file}')


if __name__ == '__main__':
    main()
