import sys
import os
import logging
from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import Qt
from ui.main_window import MainWindow
from utils.scheduler import setup_scheduler
from utils.logger import setup_logger

def main():
    try:
        # 设置日志
        setup_logger()
        logging.info("应用程序启动")
        
        # 创建应用
        app = QApplication(sys.argv)
        
        # 创建主窗口
        window = MainWindow()
        # 确保窗口显示在屏幕中央
        window.setGeometry(100, 100, 1200, 800)
        window.setWindowState(Qt.WindowState.WindowActive)
        window.show()
        window.raise_()  # 将窗口提升到最前
        window.activateWindow()  # 激活窗口
        logging.info("主窗口创建成功")
        
        # 设置定时任务
        setup_scheduler()
        logging.info("定时任务设置完成")
        
        # 运行应用
        sys.exit(app.exec())
        
    except Exception as e:
        logging.error(f"程序启动失败: {str(e)}")
        return 1

if __name__ == "__main__":
    # 将项目根目录添加到 Python 路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.append(current_dir)
    
    # 运行应用
    main()