import tushare as ts
from config.config import Config
import logging
from datetime import datetime, timedelta
import pandas as pd
import time
from utils.rate_limiter import RateLimiter

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
        if not self.pro:
            raise ValueError("Tushare API未初始化")
        return True
    
    def get_futures_basic(self):
        """获取期货基础信息"""
        try:
            self.ensure_api_ready()
            
            # 获取所有交易所的期货合约信息
            exchanges = ['CFFEX', 'SHFE', 'DCE', 'CZCE', 'INE', 'GFEX']
            all_data = []
            
            for exchange in exchanges:
                print(f"获取 {exchange} 的期货合约信息...")
                params = {
                    'exchange': exchange,
                    'fields': (
                        'ts_code,symbol,exchange,name,fut_code,multiplier,trade_unit,'
                        'per_unit,quote_unit,quote_unit_desc,d_mode_desc,'
                        'list_date,delist_date,d_month,last_ddate,trade_time_desc'
                    )
                }
                
                self.rate_limiter.acquire()  # 添加频率限制
                df = self.pro.fut_basic(**params)
                
                if df is not None and not df.empty:
                    # 过滤掉已退市的合约
                    today = datetime.now().strftime('%Y%m%d')
                    df['delist_date'] = pd.to_datetime(df['delist_date']).dt.strftime('%Y%m%d')
                    df = df[df['delist_date'] > today]
                    
                    # 处理日期字段
                    date_fields = ['list_date', 'last_ddate']
                    for field in date_fields:
                        if field in df.columns:
                            df[field] = pd.to_datetime(df[field]).dt.strftime('%Y%m%d')
                    
                    # 处理数值字段
                    numeric_fields = ['multiplier', 'per_unit']
                    for field in numeric_fields:
                        if field in df.columns:
                            df[field] = pd.to_numeric(df[field], errors='coerce')
                    
                    # 确保字符串字段不为空
                    string_fields = ['ts_code', 'symbol', 'exchange', 'name', 'fut_code', 
                                   'trade_unit', 'quote_unit', 'quote_unit_desc', 
                                   'd_mode_desc', 'd_month', 'trade_time_desc']
                    for field in string_fields:
                        if field in df.columns:
                            df[field] = df[field].fillna('')
                    
                    # 过滤掉无效数据
                    df = df.dropna(subset=['ts_code', 'exchange', 'fut_code'])
                    
                    if not df.empty:
                        all_data.append(df)
                        print(f"获取到 {len(df)} 条有效的 {exchange} 合约信息")
                
            if all_data:
                # 合并所有交易所的数据
                result = pd.concat(all_data, ignore_index=True)
                print(f"总共获取到 {len(result)} 条合约信息")
                print("\n数据示例:")
                print(result.head())
                return result
            else:
                print("未获取到任何合约信息")
                return None
                
        except Exception as e:
            error_msg = f"获取期货基础信息失败: {str(e)}"
            print(error_msg)
            logging.error(error_msg)
            return None
    
    def get_futures_daily(self, ts_code, days=None, start_date=None, end_date=None):
        """获取期货日线数据"""
        try:
            self.ensure_api_ready()
            
            # 构建查询参数
            params = {
                'ts_code': ts_code,
                'fields': 'ts_code,trade_date,open,high,low,close,pre_close,pre_settle,settle,vol,amount,oi'
            }
            
            # 如果指定了天数，计算开始日期
            if days:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days)
                params['start_date'] = start_date.strftime('%Y%m%d')
                params['end_date'] = end_date.strftime('%Y%m%d')
            else:
                # 使用传入的日期参数
                if start_date:
                    # 确保日期格式正确
                    if isinstance(start_date, datetime):
                        params['start_date'] = start_date.strftime('%Y%m%d')
                    elif isinstance(start_date, str):
                        # 如果是字符串，确保格式为YYYYMMDD
                        params['start_date'] = start_date.replace('-', '')
                    else:  # 如果是date对象
                        params['start_date'] = start_date.strftime('%Y%m%d')
                        
                if end_date:
                    # 确保日期格式正确
                    if isinstance(end_date, datetime):
                        params['end_date'] = end_date.strftime('%Y%m%d')
                    elif isinstance(end_date, str):
                        # 如果是字符串，确保格式为YYYYMMDD
                        params['end_date'] = end_date.replace('-', '')
                    else:  # 如果是date对象
                        params['end_date'] = end_date.strftime('%Y%m%d')
            
            #print(f"调用 fut_daily 接口，参数: {params}")
            df = self.pro.fut_daily(**params)
            
            if df is None:
                raise ValueError("获取数据失败")
            
            if df.empty:
                logging.info(f"未获取到{ts_code}的行情数据")
                return None
            
            # 转换日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
            
            # 确保所有数值列都是浮点数类型
            numeric_columns = ['open', 'high', 'low', 'close', 'pre_close', 
                               'pre_settle', 'settle', 'vol', 'amount', 'oi']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            return df.sort_values('trade_date', ascending=False)
            
        except Exception as e:
            error_msg = f"获取期货日线数据失败: {str(e)}"
            print(error_msg)  # 打印错误信息
            logging.error(error_msg)
            raise
    
    def get_dominant_contract(self, exchange, fut_code):
        """获取主力合约信息
        
        Args:
            exchange: 交易所代码 ('CFFEX', 'SHFE', 'DCE'等)
            fut_code: 期货品种代码 ('IF', 'IC', 'IH'等)
            
        Returns:
            DataFrame: 主力合约信息，包含字段:
                ts_code: 合约代码
                trade_date: 交易日期
                mapping_ts_code: 主力合约代码
                vol: 成交量
                amount: 成交额
                oi: 持仓量
        """
        try:
            self.ensure_api_ready()
            
            # 获取上一个交易日
            today = datetime.now()
            if today.hour < 17:  # 如果当前时间在17点之前
                # 获取前一天
                prev_date = today - timedelta(days=1)
                # 如果前一天是周末，继续往前找
                while prev_date.weekday() >= 5:  # 5是周六，6是周日
                    prev_date = prev_date - timedelta(days=1)
                trade_date = prev_date.strftime('%Y%m%d')
            else:
                trade_date = today.strftime('%Y%m%d')
            
            # 构建主力合约代码
            ts_code = f"{fut_code}.{exchange}"
            
            params = {
                'ts_code': ts_code,  # 主力合约代码
                'trade_date': trade_date,  # 交易日期
                'fields': 'ts_code,trade_date,mapping_ts_code,vol,amount,oi'
            }
            
            print(f"\n获取主力合约信息:")
            print(f"主力合约代码: {ts_code}")
            print(f"交易日期: {trade_date}")
            print(f"请求参数: {params}")
            
            self.rate_limiter.acquire()  # 添加频率限制
            df = self.pro.fut_mapping(**params)
            
            if df is not None and not df.empty:
                print("\n获取到主力合约数据:")
                print(df)
                
                # 转换日期格式
                if 'trade_date' in df.columns:
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
                
                # 确保数值字段为浮点数
                numeric_fields = ['vol', 'amount', 'oi']
                for field in numeric_fields:
                    if field in df.columns:
                        df[field] = pd.to_numeric(df[field], errors='coerce')
                    
                print("\n处理后的数据:")
                print(df)
                print(f"数据字段: {df.columns.tolist()}")
                print(f"数据类型:\n{df.dtypes}")
                
                return df
            else:
                print(f"未获取到 {ts_code} 的主力合约数据")
                return None
                
        except Exception as e:
            error_msg = f"获取主力合约信息失败: {str(e)}"
            print(error_msg)
            logging.error(error_msg)
            return None