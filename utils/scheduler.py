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

def daily_update():
    """每日定时更新任务"""
    try:
        logging.info("\n开始执行每日定时更新任务")
        service = DataUpdateService()
        
        # 1. 更新合约信息
        logging.info("1. 更新合约信息")
        if service.update_basic_info():
            logging.info("合约信息更新成功")
        else:
            logging.error("合约信息更新失败")
        
        # 2. 获取最新主力合约
        logging.info("2. 获取最新主力合约")
        success_count, fail_count = service.db.update_main_contracts()
        logging.info(f"主力合约更新完成: 成功{success_count}, 失败{fail_count}")
        
        # 3. 更新行情数据
        logging.info("3. 更新行情数据")
        success, skip, fail = service.update_all_quotes()
        logging.info(f"行情数据更新完成: 成功{success}, 跳过{skip}, 失败{fail}")
        
        # 4. 更新主力合约历史
        logging.info("4. 更新主力合约历史")
        success, skip, fail = service.update_main_contract_history()
        logging.info(f"主力合约历史更新完成: 成功{success}, 跳过{skip}, 失败{fail}")
        
        logging.info("每日定时更新任务完成")
        
    except Exception as e:
        error_msg = _log_error(e, "每日定时更新任务")
        # TODO: 添加邮件通知逻辑
        raise

def setup_scheduler():
    """设置定时任务"""
    try:
        scheduler = BackgroundScheduler()
        # 每天17:00执行数据同步
        scheduler.add_job(daily_update, 'cron', hour=17)
        scheduler.start()
        logging.info("定时任务调度器启动成功")
        return scheduler
    except Exception as e:
        error_msg = _log_error(e, "定时任务调度器启动")
        raise