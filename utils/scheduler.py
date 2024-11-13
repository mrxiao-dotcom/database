import traceback
import sys
import os
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from services.data_update_service import DataUpdateService
import logging
from datetime import datetime

def _log_error(e, context=""):
    """统一的错误日志记录"""
    exc_type, exc_obj, exc_tb = sys.exc_info()
    file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    line_no = exc_tb.tb_lineno
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    error_msg = (
        f"\n{'='*50}\n"
        f"时间: {current_time}\n"
        f"错误发生在: {context}\n"
        f"文件位置: {file_name}\n"
        f"行号: {line_no}\n"
        f"错误类型: {exc_type.__name__}\n"
        f"错误信息: {str(e)}\n"
        f"堆栈跟踪:\n{traceback.format_exc()}\n"
        f"{'='*50}"
    )
    logging.error(error_msg)
    print(error_msg, file=sys.stderr)
    return error_msg

def sync_all_data():
    """每日定时更新数据的任务"""
    try:
        logging.info("开始执行定时数据更新任务")
        service = DataUpdateService()
        service.update_all_data()
        logging.info("定时数据更新任务完成")
    except Exception as e:
        error_msg = _log_error(e, "定时数据更新任务")
        # TODO: 在这里添加邮件通知逻辑
        raise

def setup_scheduler():
    """设置定时任务"""
    try:
        scheduler = BackgroundScheduler()
        # 每天17:00执行数据同步
        scheduler.add_job(sync_all_data, 'cron', hour=17)
        scheduler.start()
        logging.info("定时任务调度器启动成功")
    except Exception as e:
        error_msg = _log_error(e, "定时任务调度器启动")
        raise