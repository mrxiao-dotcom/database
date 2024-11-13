from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, 
                            QTableWidget, QTableWidgetItem, QTabWidget, QScrollArea,
                            QGridLayout, QLabel, QMessageBox, QFrame, QSizePolicy,
                            QHeaderView)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QBrush, QColor
from .progress_dialog import ProgressDialog
from services.tushare_service import TushareService
from database.db_manager import DatabaseManager
from .db_config_dialog import DbConfigDialog
from config.config import Config
from datetime import datetime
import logging
import traceback
from services.data_update_service import DataUpdateService
import time

class DataFetchThread(QThread):
    progress_updated = pyqtSignal(int, str)
    data_ready = pyqtSignal(object)
    
    def __init__(self, exchange=None, fut_code=None):
        super().__init__()
        self.exchange = exchange
        self.fut_code = fut_code
        self.tushare = TushareService()
        self.db = DatabaseManager()
        
    def run(self):
        self.progress_updated.emit(10, "连接数据库...")
        if not self.db.connect():
            return
            
        self.progress_updated.emit(30, "获取合约数据...")
        df = self.tushare.get_future_contracts(self.exchange, self.fut_code)
        
        if df is not None:
            self.progress_updated.emit(60, "保存数据到数据库...")
            self.db.update_contracts(df)
            
        self.progress_updated.emit(90, "读取最新数据...")
        result_df = self.db.get_contracts_by_future_code(self.exchange, self.fut_code)
        self.data_ready.emit(result_df)

class ContractView(QWidget):
    def __init__(self):
        try:
            super().__init__()
            logging.info("开始初始化ContractView")
            
            # 初始化变量
            self.db = None
            self.current_exchange = None
            self.current_fut_code = None
            self.main_contracts = {}  # 用于存储主力合约信息
            
            # 设置UI
            self.setup_ui()
            logging.info("UI设置完成")
            
        except Exception as e:
            logging.error(f"ContractView初始化失败: {str(e)}\n{traceback.format_exc()}")
            QMessageBox.critical(self, "错误", "初始化失败，请检查日志")
    
    def setup_ui(self):
        try:
            logging.info("开始设置UI")
            main_layout = QVBoxLayout(self)
            main_layout.setSpacing(5)  # 减小布局间距
            main_layout.setContentsMargins(5, 5, 5, 5)  # 减小边距
            
            # 添加数据库连接控制面板
            db_control_layout = QHBoxLayout()
            db_control_layout.setSpacing(5)  # 减小按钮间距
            
            # 状态标签使用固定宽度
            self.db_status_label = QLabel("数据库未连接")
            self.db_status_label.setMinimumWidth(100)
            self.db_status_label.setStyleSheet("""
                QLabel {
                    padding: 5px;
                    border: 1px solid #ccc;
                    border-radius: 3px;
                    background-color: #f8f9fa;
                }
            """)
            db_control_layout.addWidget(self.db_status_label)
            
            # 创建按钮组
            button_style = """
                QPushButton {
                    padding: 5px 15px;
                    border: 1px solid #ddd;
                    border-radius: 3px;
                    background-color: #f8f9fa;
                }
                QPushButton:hover {
                    background-color: #e9ecef;
                }
                QPushButton:pressed {
                    background-color: #dee2e6;
                }
                QPushButton:disabled {
                    background-color: #e9ecef;
                    color: #6c757d;
                }
            """
            
            self.config_btn = QPushButton("配置数据库")
            self.config_btn.setStyleSheet(button_style)
            self.config_btn.clicked.connect(self.show_db_config)
            db_control_layout.addWidget(self.config_btn)
            
            self.connect_btn = QPushButton("连接数据库")
            self.connect_btn.setStyleSheet(button_style)
            self.connect_btn.clicked.connect(self.connect_database)
            db_control_layout.addWidget(self.connect_btn)
            
            self.disconnect_btn = QPushButton("断开连接")
            self.disconnect_btn.setStyleSheet(button_style)
            self.disconnect_btn.setEnabled(False)
            self.disconnect_btn.clicked.connect(self.disconnect_database)
            db_control_layout.addWidget(self.disconnect_btn)
            
            db_control_layout.addStretch()
            main_layout.addLayout(db_control_layout)
            
            # 创建分割线
            line = QFrame()
            line.setFrameShape(QFrame.Shape.HLine)
            line.setFrameShadow(QFrame.Shadow.Sunken)
            main_layout.addWidget(line)
            
            # 创建主内容区域
            content_widget = QWidget()
            content_layout = QVBoxLayout(content_widget)
            content_layout.setSpacing(5)
            content_layout.setContentsMargins(0, 0, 0, 0)
            
            # 上部分：交易所和品种选择（高度固定）
            upper_widget = QWidget()
            upper_widget.setMaximumHeight(120)  # 限制最大高度
            upper_layout = QVBoxLayout(upper_widget)
            upper_layout.setSpacing(2)
            upper_layout.setContentsMargins(0, 0, 0, 0)
            
            # 交易所标签页
            self.exchange_tab = QTabWidget()
            self.exchange_tab.setMaximumHeight(120)  # 限制最大高度
            self.exchange_tab.currentChanged.connect(self.on_exchange_changed)
            upper_layout.addWidget(self.exchange_tab)
            
            content_layout.addWidget(upper_widget)
            
            # 添加分隔线
            line = QFrame()
            line.setFrameShape(QFrame.Shape.HLine)
            line.setFrameShadow(QFrame.Shadow.Sunken)
            content_layout.addWidget(line)
            
            # 下部分：合约列表和行情数据（占用剩余空间）
            lower_widget = QWidget()
            lower_layout = QHBoxLayout(lower_widget)
            lower_layout.setSpacing(5)
            lower_layout.setContentsMargins(0, 0, 0, 0)
            
            # 左侧：合约列表
            contract_widget = QWidget()
            contract_layout = QVBoxLayout(contract_widget)
            
            contract_label = QLabel("合约列表")
            contract_layout.addWidget(contract_label)
            
            self.contract_table = QTableWidget()
            self.contract_table.setColumnCount(5)
            self.contract_table.setHorizontalHeaderLabels(["合约代码", "名称", "交易所", "品种代码", "退市日期"])
            self.contract_table.itemClicked.connect(self.on_contract_selected)
            contract_layout.addWidget(self.contract_table)
            
            lower_layout.addWidget(contract_widget)
            
            # 右侧：行情据
            quote_widget = QWidget()
            quote_layout = QVBoxLayout(quote_widget)
            
            quote_header = QHBoxLayout()
            quote_label = QLabel("行情数据")
            self.update_quote_btn = QPushButton("更新行情")
            self.update_quote_btn.clicked.connect(self.update_quotes)
            self.update_basic_btn = QPushButton("更新合约信息")
            self.update_basic_btn.clicked.connect(self.update_basic_info)
            self.update_main_history_btn = QPushButton("更新主力合约历史")
            self.update_main_history_btn.clicked.connect(self.update_main_contract_history)
            self.update_main_btn = QPushButton("获取最新主力合约")
            self.update_main_btn.clicked.connect(self.update_main_contracts)
            quote_header.addWidget(quote_label)
            quote_header.addWidget(self.update_quote_btn)
            quote_header.addWidget(self.update_basic_btn)
            quote_header.addWidget(self.update_main_history_btn)
            quote_header.addWidget(self.update_main_btn)
            quote_layout.addLayout(quote_header)
            
            self.quote_table = QTableWidget()
            self.quote_table.setColumnCount(8)  # 减少一列
            self.quote_table.setHorizontalHeaderLabels([
                "交易日期", "开盘价", "最高价", "最低价", "收盘价",
                "成交量", "成交额", "持仓量"
            ])
            quote_layout.addWidget(self.quote_table)
            
            lower_layout.addWidget(quote_widget)
            
            content_layout.addWidget(lower_widget, 1)  # 添加拉伸因子1
            main_layout.addWidget(content_widget)
            
            # 设置表样式
            for table in [self.contract_table, self.quote_table]:
                table.setStyleSheet("""
                    QTableWidget {
                        border: 1px solid #ddd;
                        border-radius: 3px;
                        gridline-color: #ddd;
                    }
                    QHeaderView::section {
                        background-color: #f8f9fa;
                        padding: 5px;
                        border: none;
                        border-right: 1px solid #ddd;
                        border-bottom: 1px solid #ddd;
                    }
                    QTableWidget::item {
                        padding: 5px;
                    }
                """)
                table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
                table.verticalHeader().setVisible(False)
                
            # 初始禁用
            self.update_quote_btn.setEnabled(False)
            self.update_basic_btn.setEnabled(False)
            self.update_main_history_btn.setEnabled(False)
            
            logging.info("UI设置完成")
            
        except Exception as e:
            logging.error(f"UI设置失败: {str(e)}\n{traceback.format_exc()}")
            raise
    
    def connect_database(self):
        """连接数据库"""
        try:
            # 禁用连接按钮，避免重复点击
            self.connect_btn.setEnabled(False)
            
            # 检查配置是否存在
            if not hasattr(Config, 'DB_CONFIG') or not Config.DB_CONFIG:
                QMessageBox.warning(self, "警告", "请先配置数据库连接信息")
                return
            
            # 创建数据库管理器实例
            self.db = DatabaseManager()
            
            # 尝试连接
            if self.db.connect():
                self.db_status_label.setText("数据库已连接")
                self.connect_btn.setEnabled(False)
                self.disconnect_btn.setEnabled(True)
                self.exchange_tab.setEnabled(True)
                self.update_quote_btn.setEnabled(True)
                self.update_basic_btn.setEnabled(True)
                self.update_main_history_btn.setEnabled(True)
                
                # 加载数据
                self.load_initial_data()
            else:
                self.db = None
                QMessageBox.warning(self, "警告", "无法连接到数据库，请检查配置")
                
        except Exception as e:
            logging.error(f"连接数据库失败: {str(e)}\n{traceback.format_exc()}")
            self.db = None
            QMessageBox.warning(self, "警告", f"连接数据库失败: {str(e)}")
            
        finally:
            # 恢复连接按钮状态
            self.connect_btn.setEnabled(True)
    
    def disconnect_database(self):
        """断开数据库连接"""
        try:
            # 禁用断开连接按钮
            self.disconnect_btn.setEnabled(False)
            
            if self.db and self.db.connection:
                try:
                    self.db.connection.close()
                except Exception as e:
                    logging.error(f"关闭数据库连接失败: {str(e)}")
                finally:
                    self.db = None
                
            # 更新UI状态
            self.db_status_label.setText("数据库未连接")
            self.connect_btn.setEnabled(True)
            self.disconnect_btn.setEnabled(False)
            self.exchange_tab.setEnabled(False)
            self.update_quote_btn.setEnabled(False)
            self.update_basic_btn.setEnabled(False)
            self.update_main_history_btn.setEnabled(False)
            
            # 清空数据
            self.exchange_tab.clear()
            self.quote_table.setRowCount(0)
            
            logging.info("数据库连接已断开")
            
        except Exception as e:
            logging.error(f"断开数据库连接失败: {str(e)}\n{traceback.format_exc()}")
            QMessageBox.warning(self, "警告", f"断开连接失败: {str(e)}")
            
        finally:
            # 确保按钮状态正确
            self.connect_btn.setEnabled(True)
            self.disconnect_btn.setEnabled(False)
    
    def load_initial_data(self):
        """加载初始数据"""
        try:
            if not self.db:
                return
                
            # 从数据库获取交易所列表
            exchanges = self.db.get_exchanges()
            if not exchanges:
                return
                
            # 添加交易所标签页
            for exchange in exchanges:
                scroll = self.create_future_buttons(exchange)
                self.exchange_tab.addTab(scroll, exchange)
                
        except Exception as e:
            logging.error(f"加载初始数据失败: {str(e)}")
            QMessageBox.warning(self, "警告", f"加载数据失败: {str(e)}")
    
    def create_future_buttons(self, exchange):
        try:
            logging.info(f"开始创建{exchange}的期货品种按钮")
            # 创建滚动区域
            scroll = QScrollArea()
            scroll.setWidgetResizable(True)
            scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
            scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)  # 禁用垂直滚动
            scroll.setMaximumHeight(80)  # 减小高度
            scroll.setStyleSheet("""
                QScrollArea {
                    border: none;
                    background: transparent;
                }
                QScrollBar:horizontal {
                    height: 8px;
                }
            """)
            
            # 创建容器widget
            container = QWidget()
            flow_layout = QHBoxLayout(container)  # 使用水平布局
            flow_layout.setSpacing(2)  # 减小按钮间距
            flow_layout.setContentsMargins(2, 2, 2, 2)  # 减小边距
            flow_layout.setAlignment(Qt.AlignmentFlag.AlignLeft)  # 左对齐
            
            # 获取该交易所的期货品种
            future_codes = self.db.get_future_codes(exchange)
            logging.info(f"获取到{len(future_codes)}期货品种")
            
            button_style = """
                QPushButton {
                    padding: 3px 8px;
                    border: 1px solid #ddd;
                    border-radius: 2px;
                    background-color: #fff;
                    min-width: 50px;
                    max-width: 80px;
                    height: 24px;
                    font-size: 12px;
                }
                QPushButton:hover {
                    background-color: #e9ecef;
                    border-color: #bbb;
                }
                QPushButton:pressed {
                    background-color: #dee2e6;
                }
            """
            
            if not future_codes:
                label = QLabel("无可用期货种")
                label.setAlignment(Qt.AlignmentFlag.AlignLeft)
                flow_layout.addWidget(label)
            else:
                # 创建按钮（单行）
                for code in future_codes:
                    btn = QPushButton(code)
                    btn.setStyleSheet(button_style)
                    btn.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)  # 固定大小
                    btn.clicked.connect(lambda checked, e=exchange, c=code: self.on_future_code_clicked(e, c))
                    flow_layout.addWidget(btn)
                
                # 添加弹簧，确保按钮靠左对齐
                flow_layout.addStretch()
            
            scroll.setWidget(container)
            return scroll
            
        except Exception as e:
            logging.error(f"创建期货品种按钮失败: {str(e)}")
            return QWidget()  # 返回空白widget避免程序崩溃
    
    def on_exchange_changed(self, index):
        """交易所切换事件"""
        if index >= 0:
            self.current_exchange = self.exchange_tab.tabText(index)
            self.current_fut_code = None
            self.contract_table.setRowCount(0)  # 清空表格
    
    def on_future_code_clicked(self, exchange, fut_code):
        """期货品种按钮点击事件"""
        self.current_exchange = exchange
        self.current_fut_code = fut_code
        df = self.db.get_contracts_by_future_code(exchange, fut_code)
        self.update_table(df)
        
    def fetch_data(self):
        """获取/更新数据"""
        if not self.current_exchange or not self.current_fut_code:
            return
            
        self.progress_dialog = ProgressDialog(self)
        self.progress_dialog.show()
        
        self.fetch_thread = DataFetchThread(self.current_exchange, self.current_fut_code)
        self.fetch_thread.progress_updated.connect(self.progress_dialog.update_progress)
        self.fetch_thread.data_ready.connect(self.update_table)
        self.fetch_thread.finished.connect(self.progress_dialog.accept)
        self.fetch_thread.start()
        
    def update_table(self, df):
        """更新表格数据"""
        if df is None:
            return
        
        try:
            # 获取当前所���主力合约
            self.main_contracts = self._get_current_main_contracts()
            
            self.contract_table.setRowCount(len(df))
            for row, (index, data) in enumerate(df.iterrows()):
                ts_code = str(data['ts_code'])
                
                # 检查是否是主力合约
                is_main = ts_code in self.main_contracts.values()
                
                # 设置所有列的数据
                items = [
                    QTableWidgetItem(ts_code),
                    QTableWidgetItem(str(data['name'])),
                    QTableWidgetItem(str(data['exchange'])),
                    QTableWidgetItem(str(data['fut_code'])),
                    QTableWidgetItem(str(data['delist_date']))
                ]
                
                # 如果是主力合约，设置整行的样式
                if is_main:
                    for item in items:
                        # 设置红色字体
                        item.setForeground(QBrush(QColor("#FF4444")))
                        # 设置字体加粗
                        font = item.font()
                        font.setBold(True)
                        item.setFont(font)
                
                # 将项目添加到表格
                for col, item in enumerate(items):
                    self.contract_table.setItem(row, col, item)
            
            # 调整列宽以适应内容
            self.contract_table.resizeColumnsToContents()
            
        except Exception as e:
            logging.error(f"更新表格失败: {str(e)}\n{traceback.format_exc()}")
    
    def _get_current_main_contracts(self):
        """获取当前所有主力合约"""
        try:
            query = """
            SELECT exchange, fut_code, ts_code
            FROM futures_main_contract
            WHERE trade_date = (
                SELECT MAX(trade_date)
                FROM futures_main_contract
            )
            """
            
            with self.db.connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                
                # 创建映射字典 {(exchange, fut_code): ts_code}
                return {(row[0], row[1]): row[2] for row in results}
                
        except Exception as e:
            logging.error(f"获取主力合约失败: {str(e)}")
            return {}
    
    def show_db_config(self):
        """显示数据库配置对话框"""
        try:
            dialog = DbConfigDialog(self)
            dialog.config_updated.connect(self.update_db_config)
            dialog.exec()
        except Exception as e:
            logging.error(f"显示配置对话框失败: {str(e)}\n{traceback.format_exc()}")
            QMessageBox.critical(self, "错误", "无法显示配置对话框")
    
    def update_db_config(self, new_config):
        """更新数据库配置"""
        try:
            # 更新配置
            Config.DB_CONFIG.update(new_config)
            
            # 如果当前已连接，则断开连接
            if self.db and self.db.connection:
                try:
                    self.disconnect_database()
                except:
                    pass
                
            # 清空当前状态
            self.db = None
            self.current_exchange = None
            self.current_fut_code = None
            
            # 更新UI状态
            self.db_status_label.setText("数据库未连接")
            self.connect_btn.setEnabled(True)
            self.disconnect_btn.setEnabled(False)
            self.exchange_tab.setEnabled(False)
            self.update_quote_btn.setEnabled(False)
            self.update_basic_btn.setEnabled(False)
            self.update_main_history_btn.setEnabled(False)
            
            # 清空数据
            self.exchange_tab.clear()
            self.quote_table.setRowCount(0)
            
            logging.info("数据库配置更新成功")
            
        except Exception as e:
            logging.error(f"更新数据库配置失败: {str(e)}\n{traceback.format_exc()}")
            QMessageBox.warning(self, "警告", f"更新配置失败: {str(e)}")
    
    def is_main_contract(self, ts_code, fut_code):
        """判断是否是主力合约"""
        try:
            # 从合约代码中提取月份
            contract_month = ts_code.split('.')[0][-4:]  # 获取合约月份部分
            
            # 获取同品种所有合约
            df = self.db.get_contracts_by_future_code(data['exchange'], fut_code)
            if df is None or df.empty:
                return False
                
            # 按交易量排序（如果有交易量数据）
            # TODO: 这里需要添加交易量的判断逻辑
            
            # 暂时使简单的判断逻辑：最近月份的合约为主力合约
            current_month = datetime.now().strftime('%y%m')
            return contract_month >= current_month
            
        except Exception as e:
            logging.error(f"判断主力合约失败: {str(e)}")
            return False
    
    def on_contract_selected(self, item):
        """合约选中事件"""
        try:
            row = item.row()
            ts_code = self.contract_table.item(row, 0).text()
            self.current_contract = ts_code
            self.update_quote_btn.setEnabled(True)
            self.load_quote_data(ts_code)
        except Exception as e:
            logging.error(f"处理合约选择失败: {str(e)}")
    
    def load_quote_data(self, ts_code, days=30):
        """加载行情数据"""
        try:
            df = self.db.get_contract_quotes(ts_code, days)
            if df is None or df.empty:
                self.quote_table.setRowCount(0)
                return
                
            self.quote_table.setRowCount(len(df))
            for row, (index, data) in enumerate(df.iterrows()):
                self.quote_table.setItem(row, 0, QTableWidgetItem(str(data['trade_date'])))
                self.quote_table.setItem(row, 1, QTableWidgetItem(f"{data['open']:.2f}"))
                self.quote_table.setItem(row, 2, QTableWidgetItem(f"{data['high']:.2f}"))
                self.quote_table.setItem(row, 3, QTableWidgetItem(f"{data['low']:.2f}"))
                self.quote_table.setItem(row, 4, QTableWidgetItem(f"{data['close']:.2f}"))
                self.quote_table.setItem(row, 5, QTableWidgetItem(f"{data['vol']:.0f}"))
                self.quote_table.setItem(row, 6, QTableWidgetItem(f"{data['amount']:.2f}"))
                self.quote_table.setItem(row, 7, QTableWidgetItem(f"{data['oi']:.0f}"))
            
            # 调整列宽以适应内容
            self.quote_table.resizeColumnsToContents()
            
        except Exception as e:
            logging.error(f"加载行情数据失败: {str(e)}\n{traceback.format_exc()}")
            QMessageBox.warning(self, "警告", "加载行情数据失败")
    
    def update_quotes(self):
        """更新所有有效合约的行情数据"""
        try:
            # 检查数据库连接
            if not self.db or not self.db.connection:
                error_msg = "请先连接数据库"
                print(f"错误: {error_msg}")  # 添加控制台输出
                QMessageBox.warning(self, "警告", error_msg)
                return
                
            # 禁用更新按钮
            self.update_quote_btn.setEnabled(False)
            
            try:
                # 显示进度对话框
                progress_dialog = ProgressDialog(self, "更新行情数据")
                progress_dialog.show()
                
                # 创建更新服务
                service = DataUpdateService()
                
                # 创建工作线程
                class UpdateThread(QThread):
                    progress_updated = pyqtSignal(int, str)
                    finished = pyqtSignal(bool, str)
                    
                    def __init__(self, service):
                        super().__init__()
                        self.service = service
                        self.is_cancelled = False
                        
                    def run(self):
                        try:
                            success, skip, fail = 0, 0, 0
                            contracts_df = self.service.get_valid_contracts()
                            
                            if contracts_df is None or len(contracts_df) == 0:
                                error_msg = "无有效合约"
                                print(f"错误: {error_msg}")  # 添加控制台输出
                                self.finished.emit(False, error_msg)
                                return
                                
                            total = len(contracts_df)
                            
                            for i, (_, contract) in enumerate(contracts_df.iterrows()):
                                if self.is_cancelled:  # 检查是否取消
                                    msg = f"已取消更新\n成功: {success}\n跳过: {skip}\n失败: {fail}"
                                    print(msg)  # 添加控制台输出
                                    self.finished.emit(True, msg)
                                    return
                                    
                                ts_code = contract['ts_code']
                                try:
                                    self.progress_updated.emit(
                                        int((i + 1) * 100 / total),
                                        f"更新合约 {ts_code} ({i+1}/{total})"
                                    )
                                    
                                    if self.service.update_contract_quotes(ts_code):
                                        success += 1
                                    else:
                                        skip += 1
                                        
                                except Exception as e:
                                    fail_count += 1
                                    error_msg = f"更新合约{ts_code}失败: {str(e)}"
                                    print(f"错误: {error_msg}")  # 添加控制台输出
                                    logging.error(error_msg)
                                    
                            msg = f"更新完成\n成功: {success}\n跳过: {skip}\n失败: {fail}"
                            print(msg)  # 添加控制台输出
                            self.finished.emit(True, msg)
                            
                        except Exception as e:
                            error_msg = str(e)
                            print(f"错误: {error_msg}")  # 添加控制台输出
                            self.finished.emit(False, error_msg)
                            
                    def cancel(self):
                        self.is_cancelled = True
                
                # 创建并启动线程
                self.update_thread = UpdateThread(service)
                
                # 连接信号
                self.update_thread.progress_updated.connect(progress_dialog.update_progress)
                progress_dialog.cancelled.connect(self.update_thread.cancel)
                
                def on_update_finished(success, message):
                    progress_dialog.accept()
                    if success:
                        print(f"成功: {message}")  # 添加控制台输出
                        QMessageBox.information(self, "成功", message)
                        if self.current_exchange and self.current_fut_code:
                            self.on_future_code_clicked(self.current_exchange, self.current_fut_code)
                    else:
                        print(f"警告: {message}")  # 添加控制台输出
                        QMessageBox.warning(self, "警告", message)
                    self.update_quote_btn.setEnabled(True)
                    
                self.update_thread.finished.connect(on_update_finished)
                
                # 启动线程
                self.update_thread.start()
                
            except Exception as e:
                error_msg = f"创建更新线程失败: {str(e)}\n{traceback.format_exc()}"
                print(f"错误: {error_msg}")  # 添加控制台输出
                logging.error(error_msg)
                progress_dialog.accept()
                QMessageBox.warning(self, "警告", f"更新初始化失: {str(e)}")
                self.update_quote_btn.setEnabled(True)
                
        except Exception as e:
            error_msg = f"更新行情数据失败: {str(e)}\n{traceback.format_exc()}"
            print(f"错误: {error_msg}")  # 添加控制台输出
            logging.error(error_msg)
            QMessageBox.warning(self, "警告", f"更新失败: {str(e)}")
            self.update_quote_btn.setEnabled(True)
    
    def update_main_contract_history(self):
        """更新主力合约历史行情"""
        try:
            # 检查数据库连接
            if not self.db or not self.db.connection:
                QMessageBox.warning(self, "警告", "请先连接数据库")
                return
                
            # 禁用更新按钮
            self.update_main_history_btn.setEnabled(False)
            
            try:
                # 显示进度对话框
                progress_dialog = ProgressDialog(self, "更新主力合约历史行情")
                progress_dialog.show()
                
                # 创建更新服务
                service = DataUpdateService()
                
                # 创建工作线程
                class UpdateMainHistoryThread(QThread):
                    progress_updated = pyqtSignal(int, str)  # 进度信号
                    finished = pyqtSignal(bool, str)  # 完成信号
                    
                    def __init__(self, service):
                        super().__init__()
                        self.service = service
                        
                    def run(self):
                        try:
                            # 获取所有交易所
                            self.progress_updated.emit(0, "获取交易所列表...")
                            exchanges = self.service.db.get_exchanges()
                            if not exchanges:
                                self.finished.emit(False, "无可用交易所")
                                return
                                
                            total_steps = 0
                            current_step = 0
                            
                            # 计总步骤数
                            for exchange in exchanges:
                                fut_codes = self.service.db.get_future_codes(exchange)
                                total_steps += len(fut_codes)
                            
                            # 更新每个品种的主力合约历史行情
                            for exchange in exchanges:
                                fut_codes = self.service.db.get_future_codes(exchange)
                                for fut_code in fut_codes:
                                    try:
                                        current_step += 1
                                        self.progress_updated.emit(
                                            int(current_step * 100 / total_steps),
                                            f"更新 {exchange} {fut_code} ({current_step}/{total_steps})"
                                        )
                                        
                                        # 获取主力合约
                                        main_contract = self.service.db.get_main_contract(exchange, fut_code)
                                        if main_contract:
                                            # 获取最近30个交易日的行情
                                            self.service.update_contract_quotes(main_contract, days=30)
                                            
                                    except Exception as e:
                                        logging.error(f"更新{exchange} {fut_code}主力合约历史失败: {str(e)}")
                                        
                            self.finished.emit(True, f"更新完成，共处理{current_step}个品种")
                            
                        except Exception as e:
                            self.finished.emit(False, f"更新败: {str(e)}")
                
                # 创建并启动线程
                self.update_main_history_thread = UpdateMainHistoryThread(service)
                
                # 连接信号
                self.update_main_history_thread.progress_updated.connect(progress_dialog.update_progress)
                
                def on_update_finished(success, message):
                    progress_dialog.accept()
                    if success:
                        QMessageBox.information(self, "成功", message)
                        # 刷新当前显示的数据
                        if self.current_exchange and self.current_fut_code:
                            self.on_future_code_clicked(self.current_exchange, self.current_fut_code)
                    else:
                        QMessageBox.warning(self, "警告", message)
                    self.update_main_history_btn.setEnabled(True)
                    
                self.update_main_history_thread.finished.connect(on_update_finished)
                
                # 启动线程
                self.update_main_history_thread.start()
                
            except Exception as e:
                logging.error(f"创建更新线程失败: {str(e)}")
                progress_dialog.accept()
                QMessageBox.warning(self, "警告", f"更新初始化失败: {str(e)}")
                self.update_main_history_btn.setEnabled(True)
                
        except Exception as e:
            logging.error(f"更新主力合约历史失败: {str(e)}")
            QMessageBox.warning(self, "警告", f"更新失败: {str(e)}")
            self.update_main_history_btn.setEnabled(True)
    
    def update_basic_info(self):
        """更新期货合约基础信息"""
        try:
            # 检查数据库连接
            if not self.db or not self.db.connection:
                QMessageBox.warning(self, "警告", "请先连接数据库")
                return
            
            # 禁用更新按钮
            self.update_basic_btn.setEnabled(False)
            
            # 显示进度对话框
            progress_dialog = ProgressDialog(self, "更新期货合约信息")
            progress_dialog.show()
            
            # 创建更新线程
            class UpdateBasicThread(QThread):
                progress_updated = pyqtSignal(int, str)
                finished = pyqtSignal(bool, str)
                
                def __init__(self, service):
                    super().__init__()
                    self.service = service
                    self.is_cancelled = False
                    
                def run(self):
                    try:
                        self.progress_updated.emit(10, "获取期货合约信息...")
                        
                        # 调用更新服务
                        if self.service.update_basic_info():
                            self.finished.emit(True, "合约信息更新成功")
                        else:
                            self.finished.emit(False, "更新失败")
                            
                    except Exception as e:
                        self.finished.emit(False, str(e))
                        
                    def cancel(self):
                        self.is_cancelled = True
            
            try:
                # 创建更新服务
                service = DataUpdateService()
                
                # 创建并启动线程
                self.update_basic_thread = UpdateBasicThread(service)
                
                # 连接信号
                self.update_basic_thread.progress_updated.connect(progress_dialog.update_progress)
                progress_dialog.cancelled.connect(self.update_basic_thread.cancel)
                
                def on_update_finished(success, message):
                    progress_dialog.accept()
                    if success:
                        QMessageBox.information(self, "成功", message)
                        # 刷新当前显示的数据
                        self.load_initial_data()
                    else:
                        QMessageBox.warning(self, "警告", message)
                    self.update_basic_btn.setEnabled(True)
                    
                self.update_basic_thread.finished.connect(on_update_finished)
                
                # 启动线程
                self.update_basic_thread.start()
                
            except Exception as e:
                error_msg = f"创建更新线程失败: {str(e)}\n{traceback.format_exc()}"
                print(error_msg)
                logging.error(error_msg)
                progress_dialog.accept()
                QMessageBox.warning(self, "警告", f"更新初始化失败: {str(e)}")
                self.update_basic_btn.setEnabled(True)
                
        except Exception as e:
            error_msg = f"更新期货合约信息失败: {str(e)}\n{traceback.format_exc()}"
            print(error_msg)
            logging.error(error_msg)
            QMessageBox.warning(self, "警告", f"更新失败: {str(e)}")
            self.update_basic_btn.setEnabled(True)

    def update_main_contracts(self):
        """更新最新主力合约信息"""
        try:
            # 检查数据库连接
            if not self.db or not self.db.connection:
                QMessageBox.warning(self, "警告", "请先连接数据库")
                return

            # 禁用更新按钮
            self.update_main_btn.setEnabled(False)

            # 显示进度对话框
            progress_dialog = ProgressDialog(self, "更新主力合约信息")
            progress_dialog.show()

            # 创建更新线程
            class UpdateMainThread(QThread):
                progress_updated = pyqtSignal(int, str)
                finished = pyqtSignal(bool, str)

                def __init__(self, db_manager):
                    super().__init__()
                    self.db = db_manager

                def run(self):
                    try:
                        self.progress_updated.emit(10, "开始更新主力合约...")

                        # 调用数据库管理器的批量更新方法
                        success_count, fail_count = self.db.update_main_contracts()

                        # 构建结果消息
                        msg = (
                            f"主力合约更新完成\n"
                            f"成功: {success_count}\n"
                            f"失败: {fail_count}"
                        )
                        self.finished.emit(True, msg)

                    except Exception as e:
                        error_msg = f"更新失败: {str(e)}"
                        logging.error(f"{error_msg}\n{traceback.format_exc()}")
                        self.finished.emit(False, error_msg)

            try:
                # 创建并启动线程
                self.update_main_thread = UpdateMainThread(self.db)

                # 连接信号
                self.update_main_thread.progress_updated.connect(progress_dialog.update_progress)

                def on_update_finished(success, message):
                    progress_dialog.accept()
                    if success:
                        QMessageBox.information(self, "成功", message)
                        # 刷新当前显示的数据
                        if self.current_exchange and self.current_fut_code:
                            self.on_future_code_clicked(self.current_exchange, self.current_fut_code)
                    else:
                        QMessageBox.warning(self, "警告", message)
                    self.update_main_btn.setEnabled(True)

                self.update_main_thread.finished.connect(on_update_finished)

                # 启动线程
                self.update_main_thread.start()

            except Exception as e:
                error_msg = f"创建更新线程失败: {str(e)}\n{traceback.format_exc()}"
                logging.error(error_msg)
                progress_dialog.accept()
                QMessageBox.warning(self, "警告", f"更新初始化失败: {str(e)}")
                self.update_main_btn.setEnabled(True)

        except Exception as e:
            error_msg = f"更新主力合约信息失败: {str(e)}\n{traceback.format_exc()}"
            logging.error(error_msg)
            QMessageBox.warning(self, "警告", f"更新失败: {str(e)}")
            self.update_main_btn.setEnabled(True)