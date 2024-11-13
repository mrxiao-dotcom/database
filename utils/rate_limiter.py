import time
from threading import Lock
import logging
from collections import deque
from datetime import datetime, timedelta

class RateLimiter:
    """接口调用频率限制器"""
    def __init__(self, max_calls=200, time_window=60):
        self.max_calls = max_calls  # 最大调用次数
        self.time_window = time_window  # 时间窗口（秒）
        self.calls = deque()  # 调用记录队列
        self.lock = Lock()  # 线程锁
        
    def acquire(self):
        """获取调用许可"""
        while True:  # 循环直到获取许可
            with self.lock:
                now = datetime.now()
                
                # 清理过期的调用记录
                while self.calls and (now - self.calls[0]) > timedelta(seconds=self.time_window):
                    self.calls.popleft()
                
                current_calls = len(self.calls)
                print(f"当前一分钟内的调用次数: {current_calls}/{self.max_calls}")
                
                # 如果未达到限制，直接授予许可
                if current_calls < self.max_calls:
                    self.calls.append(now)
                    return True
                
                # 计算需要等待的时间
                if self.calls:
                    wait_time = (self.calls[0] + timedelta(seconds=self.time_window) - now).total_seconds()
                    if wait_time > 0:
                        print(f"达到接口调用限制({current_calls}次)，等待 {wait_time:.2f} 秒")
                        logging.info(f"达到接口调用限制({current_calls}次)，等待 {wait_time:.2f} 秒")
                
            # 释放锁后等待
            if wait_time > 0:
                time.sleep(min(wait_time, 1.0))  # 最多等待1秒，然后重新检查
            else:
                time.sleep(0.1)  # 短暂等待后重试