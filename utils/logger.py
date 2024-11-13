import logging
import os
from datetime import datetime

def setup_logger():
    """配置日志系统"""
    # 创建logs目录（如果不存在）
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # 生成日志文件名（使用当前日期）
    log_file = f'logs/app_{datetime.now().strftime("%Y%m%d")}.log'
    
    # 配置日志格式
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()  # 同时输出到控制台
        ]
    )
    
    logging.info("日志系统初始化完成") 