from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, 
                            QPushButton, QFormLayout, QSpinBox, QMessageBox,
                            QApplication)
from PyQt6.QtCore import pyqtSignal, Qt
from PyQt6.QtGui import QCursor
from config.config import Config
from database.db_manager import DatabaseManager
import logging
import traceback
import mysql.connector

class DbConfigDialog(QDialog):
    config_updated = pyqtSignal(dict)
    
    def __init__(self, parent=None):
        try:
            super().__init__(parent)
            self.setWindowTitle("数据库配置")
            self.setMinimumWidth(400)
            self.setup_ui()
            self.load_config()
            
            # 设置对话框模态
            self.setWindowModality(Qt.WindowModality.ApplicationModal)
            
        except Exception as e:
            logging.error(f"数据库配置对话框初始化失败: {str(e)}\n{traceback.format_exc()}")
            QMessageBox.critical(self, "错误", "初始化失败，请检查日志")
    
    def setup_ui(self):
        try:
            layout = QVBoxLayout(self)
            
            # 创建表单布局
            form_layout = QFormLayout()
            
            # 主机地址
            self.host_edit = QLineEdit()
            self.host_edit.setPlaceholderText("例如: localhost 或 127.0.0.1")
            form_layout.addRow("主机地址:", self.host_edit)
            
            # 端口
            self.port_spin = QSpinBox()
            self.port_spin.setRange(1, 65535)
            self.port_spin.setValue(3306)
            form_layout.addRow("端口:", self.port_spin)
            
            # 数据库名
            self.database_edit = QLineEdit()
            self.database_edit.setPlaceholderText("数据库名称")
            form_layout.addRow("数据库名:", self.database_edit)
            
            # 用户名
            self.user_edit = QLineEdit()
            self.user_edit.setPlaceholderText("数据库用户名")
            form_layout.addRow("用户名:", self.user_edit)
            
            # 密码
            self.password_edit = QLineEdit()
            self.password_edit.setEchoMode(QLineEdit.EchoMode.Password)
            self.password_edit.setPlaceholderText("数据库密码")
            form_layout.addRow("密码:", self.password_edit)
            
            layout.addLayout(form_layout)
            
            # 按钮布局
            button_layout = QHBoxLayout()
            
            # 测试连接按钮
            self.test_btn = QPushButton("测试连接")
            self.test_btn.clicked.connect(self.safe_test_connection)
            button_layout.addWidget(self.test_btn)
            
            button_layout.addStretch()
            
            self.save_btn = QPushButton("保存配置")
            self.save_btn.clicked.connect(self.safe_save_config)
            
            self.cancel_btn = QPushButton("取消")
            self.cancel_btn.clicked.connect(self.reject)
            
            button_layout.addWidget(self.save_btn)
            button_layout.addWidget(self.cancel_btn)
            
            layout.addLayout(button_layout)
            
        except Exception as e:
            logging.error(f"设置UI失败: {str(e)}\n{traceback.format_exc()}")
            raise
    
    def load_config(self):
        try:
            config = Config.DB_CONFIG
            self.host_edit.setText(str(config.get('host', '')))
            self.port_spin.setValue(int(config.get('port', 3306)))
            self.database_edit.setText(str(config.get('database', '')))
            self.user_edit.setText(str(config.get('user', '')))
            self.password_edit.setText(str(config.get('password', '')))
        except Exception as e:
            logging.error(f"加载配置失败: {str(e)}\n{traceback.format_exc()}")
            QMessageBox.warning(self, "警告", "加载当前配置失败，将使用默认值")
    
    def get_current_config(self):
        try:
            return {
                'host': self.host_edit.text().strip(),
                'port': self.port_spin.value(),
                'database': self.database_edit.text().strip(),
                'user': self.user_edit.text().strip(),
                'password': self.password_edit.text()
            }
        except Exception as e:
            logging.error(f"获取当前配置失败: {str(e)}\n{traceback.format_exc()}")
            return None
    
    def validate_config(self, config):
        """验证配置是否完整"""
        if not config:
            return False
            
        # 检查必填字段
        required_fields = ['host', 'database', 'user', 'password']
        for field in required_fields:
            if not config.get(field):
                QMessageBox.warning(self, "警告", f"请填写{field}字段")
                return False
        return True
    
    def safe_test_connection(self):
        """安全的测试连接方法"""
        try:
            # 禁用所有按钮，避免重复操作
            self.test_btn.setEnabled(False)
            self.save_btn.setEnabled(False)
            self.cancel_btn.setEnabled(False)
            
            config = self.get_current_config()
            if not self.validate_config(config):
                return
                
            try:
                # 使用with语句确保资源正确释放
                with mysql.connector.connect(
                    host=config['host'],
                    user=config['user'],
                    password=config['password'],
                    port=config['port'],
                    database=config['database'],
                    connect_timeout=5,  # 设置较短的超时时间
                    charset='utf8mb4',
                    use_pure=True  # 使用纯Python实现，避免C扩展的潜在问题
                ) as conn:
                    # 测试查询
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        cursor.fetchone()
                    
                QMessageBox.information(self, "成功", "数据库连接测试成功！")
                
            except mysql.connector.Error as e:
                error_msg = str(e)
                if "Access denied" in error_msg:
                    QMessageBox.critical(self, "错误", "访问被拒绝，请检查用户名和密码")
                elif "Unknown database" in error_msg:
                    QMessageBox.critical(self, "错误", "数据库不存在")
                elif "Can't connect" in error_msg:
                    QMessageBox.critical(self, "错误", "无法连接到服务器，请检查主机地址和端口")
                else:
                    QMessageBox.critical(self, "错误", f"连接失败: {error_msg}")
                    
        except Exception as e:
            logging.error(f"测试连接过程发生错误: {str(e)}\n{traceback.format_exc()}")
            QMessageBox.critical(self, "错误", f"测试过程发生错误: {str(e)}")
            
        finally:
            # 恢复按钮状态
            try:
                self.test_btn.setEnabled(True)
                self.save_btn.setEnabled(True)
                self.cancel_btn.setEnabled(True)
            except:
                pass  # 忽略恢复按钮状态时的错误
    
    def safe_save_config(self):
        """安全的保存配置方法"""
        try:
            # 禁用所有按钮，避免重复操作
            self.test_btn.setEnabled(False)
            self.save_btn.setEnabled(False)
            self.cancel_btn.setEnabled(False)
            
            new_config = self.get_current_config()
            if not self.validate_config(new_config):
                return
                
            try:
                # 测试新配置是否可用
                with mysql.connector.connect(
                    host=new_config['host'],
                    user=new_config['user'],
                    password=new_config['password'],
                    port=new_config['port'],
                    database=new_config['database'],
                    connect_timeout=5,
                    charset='utf8mb4',
                    use_pure=True,
                    autocommit=True  # 自动提交模式
                ) as conn:
                    # 测试查询
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        cursor.fetchone()
                    
                # 配置测试成功，发送更新信号
                self.config_updated.emit(new_config)
                
                # 显示成功消息
                QMessageBox.information(self, "成功", "配置已保存")
                
                # 关闭对话框
                self.accept()
                
            except mysql.connector.Error as e:
                error_msg = str(e)
                response = QMessageBox.question(
                    self,
                    "警告",
                    f"无法连接到数据库 ({error_msg})，是否仍要保存配置？",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                    QMessageBox.StandardButton.No
                )
                
                if response == QMessageBox.StandardButton.Yes:
                    # 即使连接失败也保存配置
                    self.config_updated.emit(new_config)
                    self.accept()
                    
        except Exception as e:
            logging.error(f"保存配置失败: {str(e)}\n{traceback.format_exc()}")
            QMessageBox.critical(self, "错误", f"保存配置失败: {str(e)}")
            
        finally:
            try:
                # 恢复按钮状态
                self.test_btn.setEnabled(True)
                self.save_btn.setEnabled(True)
                self.cancel_btn.setEnabled(True)
            except:
                pass

    def closeEvent(self, event):
        """关闭事件处理"""
        try:
            logging.info("关闭数据库配置对话框")
            event.accept()
        except Exception as e:
            logging.error(f"关闭对话框失败: {str(e)}")
            event.accept()  # 强制关闭

    def reject(self):
        """取消按钮处理"""
        try:
            logging.info("取消数据库配置")
            super().reject()
        except Exception as e:
            logging.error(f"取消配置失败: {str(e)}")
            self.close()  # 强制关闭

    def accept(self):
        """确认按钮处理"""
        try:
            logging.info("接受数据库配置")
            super().accept()
        except Exception as e:
            logging.error(f"接受配置失败: {str(e)}")
            self.close()  # 强制关闭