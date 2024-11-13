from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from services.data_update_service import DataUpdateService
import logging

def data_update_job():
    """每日定时更新数据的任务"""
    try:
        logging.info("开始执行定时数据更新任务")
        service = DataUpdateService()
        service.update_all_data()
        logging.info("定时数据更新任务完成")
    except Exception as e:
        logging.error(f"定时任务执行失败: {str(e)}")
        # TODO: 添加邮件通知逻辑

def setup_scheduler():
    """设置定时任务"""
    try:
        scheduler = BackgroundScheduler()
        
        # 添加每日定时任务
        scheduler.add_job(
            data_update_job,
            trigger=CronTrigger.from_crontab(f"0 17 * * 1-5"),  # 每个工作日17:00执行
            id='daily_update',
            name='每日数据更新',
            misfire_grace_time=3600  # 允许延迟执行的最大时间（秒）
        )
        
        scheduler.start()
        logging.info("定时任务调度器启动成功")
    except Exception as e:
        logging.error(f"定时任务调度器启动失败: {str(e)}")