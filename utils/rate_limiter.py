import time
import logging
from collections import deque
from datetime import datetime
from threading import Lock
from utils.decorators import error_handler

class RateLimiter:
    """
    频率限制器
    max_calls: 在时间窗口内允许的最大调用次数
    time_window: 时间窗口大小（秒）
    """
    def __init__(self, max_calls, time_window):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = deque()
        self.lock = Lock()
        logging.info(f"初始化频率限制器: {max_calls}次/{time_window}秒")

    def _clean_old_calls(self):
        """清理过期的调用记录"""
        now = time.time()
        while self.calls and now - self.calls[0] >= self.time_window:
            self.calls.popleft()

    @error_handler(logger=logging)
    def acquire(self, wait=True):
        """
        获取调用许可
        wait: 如果超过限制是否等待
        返回: 是否获取到许可
        """
        with self.lock:
            while True:
                now = time.time()
                self._clean_old_calls()
                
                current_calls = len(self.calls)
                if current_calls < self.max_calls:
                    self.calls.append(now)
                    remaining = self.max_calls - current_calls
                    if current_calls % 10 == 0:  # 每10次调用输出一次日志
                        logging.debug(
                            f"频率限制状态: {current_calls}/{self.max_calls} "
                            f"剩余: {remaining} "
                            f"时间: {datetime.now().strftime('%H:%M:%S')}"
                        )
                    return True
                
                if not wait:
                    return False
                
                # 计算需要等待的时间
                wait_time = self.time_window - (now - self.calls[0])
                if wait_time > 0:
                    logging.info(f"达到频率限制，等待 {wait_time:.2f} 秒")
                    time.sleep(wait_time)

    def get_status(self):
        """获取当前状态"""
        with self.lock:
            self._clean_old_calls()
            current_calls = len(self.calls)
            return {
                'current_calls': current_calls,
                'max_calls': self.max_calls,
                'remaining': self.max_calls - current_calls,
                'time_window': self.time_window
            }

    def __str__(self):
        status = self.get_status()
        return (
            f"RateLimiter(当前调用: {status['current_calls']}, "
            f"最大调用: {status['max_calls']}, "
            f"剩余调用: {status['remaining']}, "
            f"时间窗口: {status['time_window']}秒)"
        )