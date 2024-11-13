import sys
import os
from datetime import datetime

class AppError(Exception):
    """应用程序异常基类"""
    def __init__(self, message, source=None):
        # 获取调用栈信息
        frame = sys._getframe(1)
        file_name = os.path.basename(frame.f_code.co_filename)
        line_no = frame.f_lineno
        func_name = frame.f_code.co_name
        
        # 获取当前时间
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        self.message = message
        self.source = source or self.__class__.__name__
        self.file_name = file_name
        self.line_no = line_no
        self.func_name = func_name
        
        # 构建详细的错误消息
        error_msg = (
            f"\n{'='*50}\n"
            f"时间: {current_time}\n"
            f"错误类型: {self.__class__.__name__}\n"
            f"错误来源: {self.source}\n"
            f"文件位置: {self.file_name}\n"
            f"函数名称: {self.func_name}\n"
            f"行号: {self.line_no}\n"
            f"错误信息: {self.message}\n"
            f"{'='*50}"
        )
        
        super().__init__(error_msg)

class DatabaseError(AppError):
    """数据库相关异常"""
    pass

class APIError(AppError):
    """API调用相关异常"""
    pass 