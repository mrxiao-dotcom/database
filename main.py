import sys
import os
import logging
import traceback
from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import Qt
from ui.main_window import MainWindow
from utils.scheduler import setup_scheduler
from utils.logger import setup_logger

def _log_error(e, context=""):
    """统一的错误日志记录"""
    exc_type, exc_obj, exc_tb = sys.exc_info()
    file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    error_msg = (
        f"\n{'='*50}\n"
        f"错误发生在: {context}\n"
        f"错误类型: {exc_type.__name__}\n"
        f"文件位置: {file_name}, 行号: {exc_tb.tb_lineno}\n"
        f"错误信息: {str(e)}\n"
        f"堆栈跟踪:\n{traceback.format_exc()}\n"
        f"{'='*50}"
    )
    logging.error(error_msg)
    print(error_msg, file=sys.stderr)
    return error_msg

def main():
    try:
        # 设置日志
        setup_logger()
        logging.info("应用程序启动")
        
        # 创建应用
        app = QApplication(sys.argv)
        
        # 创建主窗口
        window = MainWindow()
        window.setGeometry(100, 100, 1200, 800)
        window.setWindowState(Qt.WindowState.WindowActive)
        window.show()
        window.raise_()
        window.activateWindow()
        logging.info("主窗口创建成功")
        
        # 设置定时任务
        setup_scheduler()
        logging.info("定时任务设置完成")
        
        # 运行应用
        sys.exit(app.exec())
        
    except Exception as e:
        error_msg = _log_error(e, "程序启动")
        return 1

if __name__ == "__main__":
    # 将项目根目录添加到 Python 路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(current_dir)
    
    # 运行应用
    main()