import logging
import traceback
import sys
import os
from datetime import datetime

def error_handler(logger=None):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # 获取详细的错误信息
                exc_type, exc_obj, exc_tb = sys.exc_info()
                file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                line_no = exc_tb.tb_lineno
                
                # 获取完整的堆栈跟踪
                error_stack = traceback.format_exc()
                
                # 获取当前时间
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # 构建错误消息
                error_msg = (
                    f"\n{'='*50}\n"
                    f"时间: {current_time}\n"
                    f"错误发生在: {func.__module__}.{func.__name__}\n"
                    f"文件位置: {file_name}\n"
                    f"行号: {line_no}\n"
                    f"错误类型: {exc_type.__name__}\n"
                    f"错误信息: {str(e)}\n"
                    f"堆栈跟踪:\n{error_stack}\n"
                    f"{'='*50}"
                )
                
                # 记录错误日志
                if logger:
                    logger.error(error_msg)
                else:
                    logging.error(error_msg)
                    
                # 打印到控制台
                print(error_msg, file=sys.stderr)
                
                # 重新抛出异常，但包含更多信息
                raise type(e)(
                    f"{str(e)} (在 {file_name} 第 {line_no} 行)"
                ).with_traceback(sys.exc_info()[2])
                
        return wrapper
    return decorator 