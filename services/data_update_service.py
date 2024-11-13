from datetime import datetime, timedelta
import logging
from .tushare_service import TushareService
from database.db_manager import DatabaseManager
from utils.rate_limiter import RateLimiter
import pandas as pd
import traceback
import sys
import time

class DataUpdateService:
    def __init__(self):
        try:
            print("初始化数据更新服务...")
            self.tushare = TushareService()
            self.db = DatabaseManager()
            self.rate_limiter = RateLimiter(max_calls=180, time_window=60)
            print("数据更新服务初始化成功")
        except Exception as e:
            error_msg = f"初始化数据更新服务失败: {str(e)}\n{traceback.format_exc()}"
            print(error_msg, file=sys.stderr)
            logging.error(error_msg)
            raise
    
    def update_all_data(self, progress_callback=None):
        """更新所有数据"""
        try:
            if not self.db.connect():
                error_msg = "数据库连接失败"
                print(error_msg, file=sys.stderr)
                raise Exception(error_msg)
                
            # 获取所有有效合约
            if progress_callback:
                progress_callback(0, "获取有效合约...")
            print("获取有效合约列表...")
            contracts_df = self.db.get_valid_contracts()
            
            if contracts_df is None:
                error_msg = "无法获取合约信息"
                print(error_msg, file=sys.stderr)
                raise Exception(error_msg)
                
            if len(contracts_df) == 0:
                error_msg = "无有效合约信息"
                print(error_msg, file=sys.stderr)
                raise Exception(error_msg)
                
            # 更新行情数据
            total_contracts = len(contracts_df)
            print(f"开始更新{total_contracts}个有效合约的行情数据...")
            
            success_count = 0
            fail_count = 0

            # 获取最后交易日
            last_trade_date = self.db.get_last_trade_date()
            if not last_trade_date:
                error_msg = "无法获取最新交易日"
                print(error_msg)
                raise Exception(error_msg)

            print(f"计算得到的最新交易日: {last_trade_date}")

            
            for i, (_, contract) in enumerate(contracts_df.iterrows()):
                ts_code = contract['ts_code']
                print(f"更新合约 {ts_code} ({i+1}/{total_contracts})")
                
                if progress_callback:
                    progress_callback(
                        int((i + 1) * 100 / total_contracts),
                        f"更新行情数据 ({i+1}/{total_contracts}): {ts_code}"
                    )
                    
                try:
                    if self.update_contract_quotes(ts_code,last_trade_date):
                        success_count += 1
                    else:
                        fail_count += 1
                except Exception as e:
                    fail_count += 1
                    print(f"更新合约{ts_code}失败: {str(e)}", file=sys.stderr)
                    
            msg = f"数据更新完成，成功: {success_count}，失败: {fail_count}"
            print(msg)
            if progress_callback:
                progress_callback(100, msg)
                
        except Exception as e:
            error_msg = f"数据更新失败: {str(e)}\n{traceback.format_exc()}"
            print(error_msg, file=sys.stderr)
            logging.error(error_msg)
            if progress_callback:
                progress_callback(-1, f"更新失败: {str(e)}")
            raise
            
    def update_futures_basic(self):
        """更新期货基础信息"""
        self.rate_limiter.acquire()
        df = self.tushare.get_futures_basic()
        if df is not None and len(df) > 0:
            self.db.update_contracts(df)
            
    def get_valid_contracts(self):
        """获取所有有效合约"""
        return self.db.get_contracts()
        
    def update_contract_quotes(self, ts_code, last_trade_date=None):
        """更新单个合约的行情数据"""
        try:
            if not self.db.connect():
                error_msg = "数据库连接失败"
                print(error_msg)
                raise Exception(error_msg)
                
            # 如果没有传入交易日，则获取最新交易日
            if last_trade_date is None:
                last_trade_date = self.db.get_last_trade_date()
                if not last_trade_date:
                    error_msg = "无法获取最新交易日"
                    print(error_msg)
                    raise Exception(error_msg)
                
            print(f"更新{ts_code}在{last_trade_date}的行情数据")
                
            # 检查是否需要更新
            if self.db.check_quote_exists(ts_code, last_trade_date):
                print(f"{ts_code}在{last_trade_date}的行情数据已存在，跳过更新")
                return True
                
            # 获取行情数据前等待限流器许可
            self.rate_limiter.acquire()
            try:
                # 只获取最新交易日的数据
                df = self.tushare.get_futures_daily(
                    ts_code, 
                    start_date=last_trade_date.strftime('%Y%m%d'),
                    end_date=last_trade_date.strftime('%Y%m%d')
                )
            except ValueError as ve:
                error_msg = f"Tushare API错误: {str(ve)}"
                print(error_msg)
                raise Exception(error_msg)
            except Exception as e:
                error_msg = f"获取行情数据失败: {str(e)}"
                print(error_msg)
                raise Exception(error_msg)
            
            if df is None:
                print(f"{ts_code}在{last_trade_date}无可用行情数据")
                return False
                
            if len(df) == 0:
                print(f"{ts_code}在{last_trade_date}无新行情数据")
                return True
                
            # 保存数据
            if not self.db.save_quotes(df):
                error_msg = "保存行情数据失败"
                print(error_msg)
                raise Exception(error_msg)
                
            print(f"{ts_code}在{last_trade_date}的行情数据更新成功")
            return True
            
        except Exception as e:
            error_msg = f"更新合约{ts_code}行情数据失败: {str(e)}\n{traceback.format_exc()}"
            print(error_msg)
            logging.error(error_msg)
            raise
            
    def batch_update_quotes(self, ts_codes, last_trade_date, days=1):
        """批量更新合约行情数据"""
        success_count = 0
        fail_count = 0
        
        for i, ts_code in enumerate(ts_codes):
            try:
                print(f"更新进度: {i+1}/{len(ts_codes)} - {ts_code}")
                if self.update_contract_quotes(ts_code, last_trade_date,days):
                    success_count += 1
                else:
                    fail_count += 1
                    
                # 每更新50个合约暂停1秒，避免频率过高
                if (i + 1) % 50 == 0:
                    time.sleep(1)
                    
            except Exception as e:
                fail_count += 1
                print(f"更新{ts_code}失败: {str(e)}")
                continue
                
        return success_count, fail_count

    def update_main_contracts(self):
        """更新主力合约信息"""
        try:
            # 获取最新交易日
            latest_date = self.db.get_last_trade_date()
            if not latest_date:
                error_msg = "无法获取最新交易日"
                print(error_msg)
                raise Exception(error_msg)
            
            print(f"\n开始更新主力合约信息，交易日期: {latest_date}")
            
            # 获取所有交易所和品种
            exchanges = self.db.get_exchanges()
            if not exchanges:
                error_msg = "无可用交易所"
                print(error_msg)
                raise Exception(error_msg)
            
            total_success = 0
            total_fail = 0
            
            # 遍历每个交易所和品种
            for exchange in exchanges:
                fut_codes = self.db.get_future_codes(exchange)
                if not fut_codes:
                    continue
                    
                for fut_code in fut_codes:
                    try:
                        print(f"\n处理 {exchange} {fut_code}")
                        # 从数据库获取主力合约
                        main_contract = self.db.get_main_contracts(exchange, fut_code)
                        
                        if main_contract:
                            print(f"找到主力合约: {main_contract}")
                            # 获取主力合约的最新行情数据
                            df = self.db.get_contract_quotes(main_contract, days=1)
                            if df is not None and not df.empty:
                                row = df.iloc[0]
                                # 保存主力合约信息
                                if self.db.save_main_contract(
                                    trade_date=latest_date,
                                    exchange=exchange,
                                    fut_code=fut_code,
                                    ts_code=main_contract,
                                    vol=float(row['vol']) if 'vol' in row else 0,
                                    amount=float(row['amount']) if 'amount' in row else 0,
                                    oi=float(row['oi']) if 'oi' in row else 0
                                ):
                                    total_success += 1
                                    print(f"保存主力合约信息成功")
                                else:
                                    total_fail += 1
                                    print(f"保存主力合约信息失败")
                            else:
                                total_fail += 1
                                print(f"未找到主力合约{main_contract}的行情数据")
                        else:
                            total_fail += 1
                            print(f"未找到主力合约")
                            
                    except Exception as e:
                        total_fail += 1
                        error_msg = f"更新{exchange} {fut_code}主力合约失败: {str(e)}"
                        print(error_msg)
                        logging.error(error_msg)
                        continue
                        
            summary = f"\n主力合约更新完成\n成功: {total_success}\n失败: {total_fail}"
            print(summary)
            return total_success, total_fail
            
        except Exception as e:
            error_msg = f"更新主力合约失败: {str(e)}"
            print(error_msg)
            logging.error(error_msg)
            raise
            
    def update_all_quotes(self, progress_callback=None):
        """更新所有有效合约的行情数据"""
        try:
            if not self.db.connect():
                error_msg = "数据库连接失败"
                print(error_msg, file=sys.stderr)
                raise Exception(error_msg)
                
            # 1. 获取最新交易日（只计算一次）
            latest_trade_date = self.db.get_last_trade_date()
            if not latest_trade_date:
                error_msg = "无法获取最新交易日"
                print(error_msg, file=sys.stderr)
                raise Exception(error_msg)
                
            trade_date_msg = f"最新交易日: {latest_trade_date}"
            print(f"\n{'-'*50}")
            print(trade_date_msg)
            print(f"{'-'*50}")
            
            if progress_callback:
                progress_callback(0, trade_date_msg)
                
            # 2. 获取所有有效合约（未到期的合约）
            print("\n开始获取有效合约列表...")
            if progress_callback:
                progress_callback(5, f"{trade_date_msg}\n正在获取有效合约...")
            
            valid_contracts = self.db.get_valid_contracts()  # 这里已经过滤了到期日
            if valid_contracts is None or len(valid_contracts) == 0:
                error_msg = "无有效合约信息"
                print(error_msg, file=sys.stderr)
                raise Exception(error_msg)
                
            total_contracts = len(valid_contracts)
            print(f"\n找到{total_contracts}个有效合约")
            print(f"第一个合约: {valid_contracts.iloc[0]['ts_code']}")
            print(f"最后一个合约: {valid_contracts.iloc[-1]['ts_code']}")
            
            success_count = 0
            skip_count = 0
            fail_count = 0
            
            # 3. 遍历处理每个合约
            print(f"\n{'-'*50}")
            print("开始更新合约行情数据")
            print(f"{'-'*50}")
            
            for i, (_, contract) in enumerate(valid_contracts.iterrows()):
                ts_code = contract['ts_code']
                try:
                    current_progress = int((i + 1) * 100 / total_contracts)
                    remaining = total_contracts - (i + 1)
                    
                    # 构建进度消息
                    progress_msg = (
                        f"{trade_date_msg}\n"
                        f"处理合约 {ts_code} ({i+1}/{total_contracts})\n"
                        f"成功: {success_count}  跳过: {skip_count}  失败: {fail_count}  剩余: {remaining}"
                    )
                    
                    print(f"\n当前处理: {ts_code} ({i+1}/{total_contracts})")
                    print(f"到期日: {contract['last_ddate']}")
                    
                    if progress_callback:
                        progress_callback(current_progress, progress_msg)
                        
                    # 检查是否已有数据
                    if self.db.check_quote_exists(ts_code, latest_trade_date):
                        print(f"合约 {ts_code} 已有最新数据，跳过")
                        skip_count += 1
                        continue
                        
                    # 获取行情数据
                    print(f"获取 {ts_code} 的行情数据...")
                    self.rate_limiter.acquire()
                    df = self.tushare.get_futures_daily(ts_code, start_date=latest_trade_date, end_date=latest_trade_date)
                    
                    if df is not None and not df.empty:
                        print(f"获取到 {len(df)} 条数据")
                        # 保存数据
                        if self.db.save_quotes(df):
                            success_count += 1
                            print(f"合约 {ts_code} 更新成功")
                        else:
                            fail_count += 1
                            print(f"合约 {ts_code} 保存失败")
                    else:
                        print(f"合约 {ts_code} 无数据")
                        skip_count += 1
                        
                except Exception as e:
                    fail_count += 1
                    error_msg = f"更新合约 {ts_code} 失败: {str(e)}"
                    print(error_msg, file=sys.stderr)
                    logging.error(f"{error_msg}\n{traceback.format_exc()}")
                    continue
                    
                # 每50个合约暂停1秒
                if (i + 1) % 50 == 0:
                    print("\n达到50个合约，暂停1秒...")
                    time.sleep(1)
                    
            # 4. 完成处理
            summary = (
                f"\n{'-'*50}\n"
                f"更新完成 (交易日: {latest_trade_date})\n"
                f"总计: {total_contracts} 个合约\n"
                f"成功: {success_count}\n"
                f"跳过: {skip_count}\n"
                f"失败: {fail_count}\n"
                f"{'-'*50}"
            )
            print(summary)
            
            if progress_callback:
                progress_callback(100, summary)
                
            return success_count, skip_count, fail_count
            
        except Exception as e:
            error_msg = f"更新行情数据失败: {str(e)}\n{traceback.format_exc()}"
            print(error_msg, file=sys.stderr)
            logging.error(error_msg)
            if progress_callback:
                progress_callback(-1, f"更新失败: {str(e)}")
            raise
            
    def update_main_contract_history(self):
        """更新主力合约历史行情"""
        try:
            # 1. 确保主力合约表存在
            self.db.create_main_contract_table()
            
            # 2. 获取最新交易日
            latest_date = self.db.get_last_trade_date()
            if not latest_date:
                error_msg = "无法获取最新交易日"
                print(error_msg)
                raise Exception(error_msg)
                
            print(f"最新交易日: {latest_date}")
            
            # 3. 获取所有交易所和品种
            exchanges = self.db.get_exchanges()
            if not exchanges:
                error_msg = "无可用交易所"
                print(error_msg)
                raise Exception(error_msg)
                
            total_success = 0
            total_skip = 0
            total_fail = 0
            
            # 4. 遍历每个交易所和品种
            for exchange in exchanges:
                fut_codes = self.db.get_future_codes(exchange)
                if not fut_codes:
                    continue
                    
                for fut_code in fut_codes:
                    try:
                        # 调用tushare接口获取主力合约
                        self.rate_limiter.acquire()
                        df = self.tushare.get_dominant_contract(exchange, fut_code)
                        
                        if df is not None and len(df) > 0:
                            row = df.iloc[0]
                            main_ts_code = row['mapping_ts_code']
                            
                            # 保存主力合约信息
                            if self.db.save_main_contract(
                                trade_date=latest_date,
                                exchange=exchange,
                                fut_code=fut_code,
                                ts_code=main_ts_code,
                                vol=row.get('vol', 0),
                                amount=row.get('amount', 0),
                                oi=row.get('oi', 0)
                            ):
                                # 获取主力合约的历史行情
                                self.rate_limiter.acquire()
                                df = self.tushare.get_futures_daily(main_ts_code, days=30)
                                if df is not None and not df.empty:
                                    if self.db.save_quotes(df):
                                        total_success += 1
                                        print(f"更新主力合约{main_ts_code}历史行情成功")
                                    else:
                                        total_fail += 1
                                        print(f"保存主力合约{main_ts_code}历史行情失败")
                                else:
                                    total_skip += 1
                                    print(f"主力合约{main_ts_code}无历史行情数据")
                            else:
                                total_fail += 1
                                print(f"保存主力合约信息失败: {exchange} {fut_code}")
                        else:
                            total_skip += 1
                            print(f"无主力合约信息: {exchange} {fut_code}")
                            
                    except Exception as e:
                        total_fail += 1
                        error_msg = f"更新{exchange} {fut_code}主力合约历史失败: {str(e)}"
                        print(error_msg)
                        logging.error(error_msg)
                        continue
                        
            return total_success, total_skip, total_fail
            
        except Exception as e:
            error_msg = f"更新主力合约历史失败: {str(e)}"
            print(error_msg)
            logging.error(error_msg)
            raise
            
    def update_basic_info(self):
        """更新期货基础信息"""
        try:
            # 确保数据库连接
            if not self.db.connect():
                error_msg = "数据库连接失败"
                print(error_msg)
                raise Exception(error_msg)
                
            # 获取当前日期
            today = datetime.now().strftime('%Y%m%d')
            print(f"当前日期: {today}")
            
            # 获取所有交易所的期货合约信息
            exchanges = ['CFFEX', 'SHFE', 'DCE', 'CZCE', 'INE', 'GFEX']
            all_data = []
            
            for exchange in exchanges:
                print(f"获取 {exchange} 的期货合约信息...")
                # 调用tushare接口获取数据
                self.rate_limiter.acquire()
                df = self.tushare.pro.fut_basic(
                    exchange=exchange,
                    fields=(
                        'ts_code,symbol,exchange,name,fut_code,multiplier,trade_unit,'
                        'per_unit,quote_unit,quote_unit_desc,delivery_month,'
                        'last_trade_date,delist_date,list_date,list_status,is_delist'
                    )
                )
                
                if df is not None and len(df) > 0:
                    # 1. 过滤掉已退市的合约
                    df['delist_date'] = pd.to_datetime(df['delist_date']).dt.strftime('%Y%m%d')
                    df = df[df['delist_date'] > today]
                    
                    # 2. 检查并处理必要字段
                    required_fields = {
                        'ts_code': str,
                        'symbol': str,
                        'exchange': str,
                        'name': str,
                        'fut_code': str
                    }
                    
                    # 确保所有必要字段存在且类型正确
                    for field, field_type in required_fields.items():
                        if field not in df.columns:
                            print(f"缺少必要字段: {field}")
                            continue
                        df[field] = df[field].astype(field_type)
                    
                    # 3. 处理数值字段
                    numeric_fields = {
                        'multiplier': float,
                        'per_unit': float,
                        'delivery_month': int
                    }
                    
                    for field, field_type in numeric_fields.items():
                        if field in df.columns:
                            df[field] = pd.to_numeric(df[field], errors='coerce')
                    
                    # 4. 处理日期字段
                    date_fields = ['list_date', 'last_trade_date', 'last_ddate']
                    for field in date_fields:
                        if field in df.columns:
                            df[field] = pd.to_datetime(df[field]).dt.strftime('%Y%m%d')
                    
                    # 5. 添加空的 d_mode_desc 字段
                    df['d_mode_desc'] = ''
                    
                    # 6. 过滤掉无效数据
                    df = df.dropna(subset=['ts_code', 'exchange', 'fut_code'])
                    
                    if not df.empty:
                        all_data.append(df)
                        print(f"获取到 {len(df)} 条有效的 {exchange} 合约信息")
                
            if all_data:
                # 合并所有交易所的数据
                result = pd.concat(all_data, ignore_index=True)
                print(f"总共获取到 {len(result)} 条有效合约信息")
                
                # 确保数据库连接仍然有效
                if not self.db.ensure_connected():
                    error_msg = "数据库连接已断开"
                    print(error_msg)
                    raise Exception(error_msg)
                
                # 保存到数据库
                if self.db.update_contracts(result):
                    print("合约信息更新成功")
                    return True
                else:
                    error_msg = "保存合约数据失败"
                    print(error_msg)
                    raise Exception(error_msg)
            else:
                error_msg = "未获取到任何有效合约信息"
                print(error_msg)
                raise Exception(error_msg)
                
        except Exception as e:
            error_msg = f"更新期货基础信息失败: {str(e)}"
            print(error_msg)
            logging.error(error_msg)
            raise
            