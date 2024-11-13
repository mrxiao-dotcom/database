import tushare as ts
from config.config import Config
import logging
from datetime import datetime, timedelta
import pandas as pd
import time
from utils.rate_limiter import RateLimiter
from utils.decorators import error_handler
from utils.exceptions import APIError

class TushareService:
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TushareService, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            try:
                # 确保token存在
                token = Config.TUSHARE_TOKEN
                if not token:
                    raise ValueError("Tushare token未设置，请在.env文件中设置TUSHARE_TOKEN")
                
                # 设置token
                ts.set_token(token)
                
                # 初始化API（带重试机制）
                max_retries = 3
                retry_delay = 1
                
                for attempt in range(max_retries):
                    try:
                        self.pro = ts.pro_api()
                        # 使用简单的API调用测试token
                        self.pro.query('trade_cal', start_date='20240101', end_date='20240101')
                        logging.info("Tushare API 初始化成功")
                        break
                    except Exception as e:
                        if attempt == max_retries - 1:
                            raise ValueError(f"Tushare API 初始化失败: {str(e)}")
                        time.sleep(retry_delay)
                
                # 初始化频率限制器
                self.rate_limiter = RateLimiter(max_calls=180, time_window=60)
                
                self._initialized = True
                
            except Exception as e:
                logging.error(f"Tushare服务初始化失败: {str(e)}")
                self.pro = None
                raise

    def ensure_api_ready(self):
        """确保API可用"""
        if not hasattr(self, 'pro') or self.pro is None:
            raise APIError("Tushare API未初始化")
        return True
    
    @staticmethod
    def _format_date(date_value):
        """统一日期格式转换"""
        if isinstance(date_value, datetime):
            return date_value.strftime('%Y%m%d')
        elif isinstance(date_value, str):
            return date_value.replace('-', '')
        return date_value.strftime('%Y%m%d')
    
    @staticmethod
    def _process_dataframe(df, date_columns=None, numeric_columns=None):
        """统一处理DataFrame的日期和数值列"""
        if df is None or df.empty:
            return df
            
        # 处理日期列
        if date_columns:
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
        
        # 处理数值列
        if numeric_columns:
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    @error_handler(logger=logging)
    def get_futures_basic(self):
        """获取期货基础信息"""
        self.ensure_api_ready()
        exchanges = ['CFFEX', 'SHFE', 'DCE', 'CZCE', 'INE', 'GFEX']
        all_data = []
        
        for exchange in exchanges:
            self.rate_limiter.acquire()
            df = self.pro.fut_basic(
                exchange=exchange,
                fields='ts_code,symbol,exchange,name,fut_code,multiplier,trade_unit,'
                       'per_unit,quote_unit,quote_unit_desc,d_mode_desc,'
                       'list_date,delist_date,d_month,last_ddate,trade_time_desc'
            )
            
            if df is not None and not df.empty:
                # 处理日期和数值
                df = self._process_dataframe(
                    df,
                    date_columns=['list_date', 'delist_date', 'last_ddate'],
                    numeric_columns=['multiplier', 'per_unit']
                )
                
                # 过滤有效合约
                today = datetime.now().strftime('%Y%m%d')
                df = df[df['delist_date'] > today]
                
                if not df.empty:
                    all_data.append(df)
        
        return pd.concat(all_data, ignore_index=True) if all_data else None

    @error_handler(logger=logging)
    def get_futures_daily(self, ts_code, days=None, start_date=None, end_date=None):
        """获取期货日线数据"""
        self.ensure_api_ready()
        
        params = {
            'ts_code': ts_code,
            'fields': 'ts_code,trade_date,open,high,low,close,pre_close,'
                     'pre_settle,settle,vol,amount,oi'
        }
        
        # 处理日期参数
        if days:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
        
        if start_date:
            params['start_date'] = self._format_date(start_date)
        if end_date:
            params['end_date'] = self._format_date(end_date)
        
        self.rate_limiter.acquire()
        df = self.pro.fut_daily(**params)
        
        return self._process_dataframe(
            df,
            date_columns=['trade_date'],
            numeric_columns=['open', 'high', 'low', 'close', 'pre_close', 
                           'pre_settle', 'settle', 'vol', 'amount', 'oi']
        )