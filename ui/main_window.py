from PyQt6.QtWidgets import QMainWindow, QTabWidget, QWidget, QVBoxLayout
from PyQt6.QtCore import Qt
from .contract_view import ContractView
import logging

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        logging.info("开始初始化主窗口")
        
        self.setWindowTitle("期货数据管理系统")
        self.setMinimumSize(1200, 800)
        logging.info("设置窗口基本属性完成")
        
        try:
            # 创建中心部件
            self.central_widget = QWidget()
            self.setCentralWidget(self.central_widget)
            logging.info("创建中心部件完成")
            
            # 创建布局
            layout = QVBoxLayout(self.central_widget)
            
            # 创建标签页
            self.tab_widget = QTabWidget()
            logging.info("创建标签页控件完成")
            
            # 添加合约信息标签页
            try:
                contract_view = ContractView()
                self.tab_widget.addTab(contract_view, "期货合约信息")
                logging.info("添加合约信息标签页完成")
            except Exception as e:
                logging.error(f"创建合约视图失败: {str(e)}")
                raise
            
            # 添加其他标签页（暂时为空）
            self.tab_widget.addTab(QWidget(), "期货行情数据")
            self.tab_widget.addTab(QWidget(), "持仓数据")
            
            # 将标签页添加到布局中
            layout.addWidget(self.tab_widget)
            
            logging.info("主窗口界面初始化完成")
            
        except Exception as e:
            logging.error(f"主窗口初始化失败: {str(e)}")
            raise
            
    def showEvent(self, event):
        """窗口显示事件"""
        super().showEvent(event)
        logging.info("主窗口显示事件触发")
        
    def closeEvent(self, event):
        """窗口关闭事件"""
        logging.info("主窗口关闭事件触发")
        super().closeEvent(event)