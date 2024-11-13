import os
from dotenv import load_dotenv

# 加载.env文件
load_dotenv()

class Config:
    # 数据库配置
    DB_CONFIG = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'port': int(os.getenv('DB_PORT', 3306)),
        'database': os.getenv('DB_DATABASE')
    }
    
    # Tushare配置
    TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN')
    
    # 邮件配置
    EMAIL_CONFIG = {
        'smtp_server': os.getenv('SMTP_SERVER'),
        'smtp_port': int(os.getenv('SMTP_PORT', 587)),
        'sender': os.getenv('EMAIL_SENDER'),
        'password': os.getenv('EMAIL_PASSWORD'),
        'receivers': os.getenv('EMAIL_RECEIVERS', '').split(',')
    }
    
    # 定时任务配置
    SCHEDULE_TIME = "17:00"