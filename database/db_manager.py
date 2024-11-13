import mysql.connector
from config.config import Config
import logging
import pandas as pd
from datetime import datetime, timedelta
import time
import traceback
import sys
import os
from utils.decorators import error_handler
from utils.exceptions import DatabaseError
import contextlib

class QueryBuilder:
    """SQL查询构建器"""
    @staticmethod
    def build_select(table, fields='*', where=None):
        query = f"SELECT {fields} FROM {table}"
        if where:
            query += f" WHERE {where}"
        return query
    
    @staticmethod
    def build_insert(table, fields):
        placeholders = ', '.join(['%s'] * len(fields))
        return f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({placeholders})"
    
    @staticmethod
    def build_update(table, fields, where):
        set_clause = ', '.join([f"{field} = %s" for field in fields])
        return f"UPDATE {table} SET {set_clause} WHERE {where}"

class ConnectionManager:
    """数据库连接管理器"""
    def __enter__(self):
        self.ensure_connected()
        return self.connection.cursor()
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.connection.rollback()
        else:
            self.connection.commit()

class DatabaseManager:
    def __init__(self):
        self.config = Config.DB_CONFIG
        self.connection = None
        self.max_retries = 3
        self.retry_delay = 1  # 重试延迟（秒）
        
    def connect(self):
        """连接数据库，带重试机制"""
        retries = 0
        while retries < self.max_retries:
            try:
                if self.connection:
                    try:
                        if self.connection.is_connected():
                            return True
                    except:
                        self.connection = None
                    
                logging.info(f"尝试连接数据库 (尝试 {retries + 1}/{self.max_retries})")
                
                # 验证配置
                if not self.validate_config():
                    logging.error("数据库配置无效")
                    return False
                
                # 创新连接
                self.connection = mysql.connector.connect(
                    host=str(self.config['host']),
                    user=str(self.config['user']),
                    password=str(self.config['password']),
                    port=int(self.config['port']),
                    database=str(self.config['database']),
                    connect_timeout=10,
                    charset='utf8mb4',
                    use_pure=True,  # 使用纯Python实现
                    autocommit=True  # 自动提交模式
                )
                
                # 测试连接
                with self.connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    
                logging.info("数据库连接成功")
                return True
                
            except mysql.connector.Error as e:
                error_msg = str(e)
                if "Access denied" in error_msg:
                    logging.error("访问被拒绝，请检查用户名和密码")
                elif "Unknown database" in error_msg:
                    logging.error("数据库不存在")
                elif "Can't connect" in error_msg:
                    logging.error("无法连接到服务器，请检查主机地址和端口")
                else:
                    logging.error(f"数据库连接错误: {error_msg}")
                
                if self.connection:
                    try:
                        self.connection.close()
                    except:
                        pass
                self.connection = None
                
                retries += 1
                if retries < self.max_retries:
                    time.sleep(self.retry_delay)
                    
            except Exception as e:
                logging.error(f"未预期的错误: {str(e)}\n{traceback.format_exc()}")
                if self.connection:
                    try:
                        self.connection.close()
                    except:
                        pass
                self.connection = None
                return False
                
        logging.error("数据库连接失败，已达到最大重试次数")
        return False
    
    def validate_config(self):
        """验证数据库配置"""
        try:
            required_fields = ['host', 'user', 'password', 'port', 'database']
            for field in required_fields:
                if field not in self.config or not self.config[field]:
                    logging.error(f"数据库配置缺少必要字段: {field}")
                    return False
                    
            # 验证端口号
            try:
                port = int(self.config['port'])
                if port <= 0 or port > 65535:
                    logging.error(f"无的端口号: {port}")
                    return False
            except:
                logging.error("端口号必须是有效的整数")
                return False
                
            return True
            
        except Exception as e:
            logging.error(f"验证配置失败: {str(e)}")
            return False
    
    def ensure_connected(self):
        """确保数据库连接有效"""
        try:
            if self.connection:
                try:
                    if self.connection.is_connected():
                        return True
                except:
                    self.connection = None
            return self.connect()
        except Exception as e:
            logging.error(f"检查数据库连接失败: {str(e)}")
            return False
    
    @contextlib.contextmanager
    def transaction(self):
        """事务管理器"""
        cursor = None
        try:
            if not self.ensure_connected():
                raise DatabaseError("无法建立数据库连接")
            cursor = self.connection.cursor()
            yield cursor
            self.connection.commit()
        except Exception as e:
            if self.connection:
                self.connection.rollback()
            raise DatabaseError(f"事务执行失败: {str(e)}")
        finally:
            if cursor:
                cursor.close()

    @error_handler(logger=logging)
    def execute_query(self, query, params=None):
        """执行查询"""
        with self.transaction() as cursor:
            cursor.execute(query, params or ())
            return cursor
    
    def get_contracts(self, exchange=None):
        """获取合约信息"""
        try:
            if not self.ensure_connected():
                return None
                
            today = datetime.now().strftime('%Y%m%d')
            query = """
            SELECT DISTINCT
                ts_code,
                name,
                exchange,
                fut_code,
                delist_date
            FROM futures_basic
            WHERE delist_date > %s
            """
            params = [today]
            
            if exchange:
                query += " AND exchange = %s"
                params.append(exchange)
                
            query += " ORDER BY fut_code, ts_code"
            
            # 使用原生MySQL查询
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                
            # 手动创建DataFrame
            df = pd.DataFrame(data, columns=columns)
            
            # 转换日期格式
            if not df.empty and 'delist_date' in df.columns:
                df['delist_date'] = pd.to_datetime(df['delist_date']).dt.strftime('%Y-%m-%d')
                
            return df
            
        except Exception as e:
            logging.error(f"获取合约数据失败: {str(e)}")
            return None
    
    def get_exchanges(self):
        """获取所有交易所"""
        try:
            if not self.ensure_connected():
                return None
                
            today = datetime.now().strftime('%Y-%m-%d')
            query = """
            SELECT DISTINCT exchange 
            FROM futures_basic 
            WHERE delist_date > %s
            ORDER BY exchange
            """
            
            # 用原生MySQL查询
            with self.connection.cursor() as cursor:
                cursor.execute(query, (today,))
                result = cursor.fetchall()
                return [row[0] for row in result]
                
        except Exception as e:
            logging.error(f"获取交易所数据失败: {str(e)}")
            return None
    
    def get_future_codes(self, exchange):
        """获取指定交易所的期货品种代码"""
        today = datetime.now().strftime('%Y-%m-%d')
        query = """
        SELECT DISTINCT fut_code 
        FROM futures_basic 
        WHERE exchange = %s 
        AND delist_date > %s
        ORDER BY fut_code
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (exchange, today))
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"获取期货品种代码失败: {str(e)}")
            return None
    
    def update_contracts(self, df):
        """更新合约信息（先清空表，再插入新数据）"""
        if df is None or df.empty:
            return False
            
        try:
            # 确保数据库连接有效
            if not self.ensure_connected():
                error_msg = "数据库连接失败"
                print(error_msg)
                logging.error(error_msg)
                return False
                
            cursor = self.connection.cursor()
            try:
                # 获取表结构信息
                cursor.execute("DESCRIBE futures_basic")
                field_info = {row[0]: row[1] for row in cursor.fetchall()}
                print("\n结构信息:")
                for field, type_info in field_info.items():
                    print(f"字段: {field}, 类型: {type_info}")
                
                # 打印数据框的列名和数据类型
                print("\nDataFrame信息:")
                print(df.dtypes)
                print("\nDataFrame前几行数据:")
                print(df.head())
                
                # 1. 先清空表
                truncate_query = "TRUNCATE TABLE futures_basic"
                print("清空期货合约表...")
                cursor.execute(truncate_query)
                
                # 2. 插入新数据
                insert_query = """
                INSERT INTO futures_basic (
                    ts_code, symbol, exchange, name, fut_code, multiplier,
                    trade_unit, per_unit, quote_unit, quote_unit_desc, d_mode_desc,
                    list_date, delist_date, d_month, last_ddate, trade_time_desc
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                """
                
                insert_count = 0
                skip_count = 0
                
                for _, row in df.iterrows():
                    try:
                        # 准备插入数据，并检查字段长度
                        data = {}
                        for field, value in row.items():
                            if field not in field_info:
                                continue
                            
                            field_type = field_info[field].upper()
                            
                            # 处理字符串类型字段
                            if 'VARCHAR' in field_type:
                                max_length = int(field_type.split('(')[1].split(')')[0])
                                str_value = str(value) if pd.notna(value) else ''
                                if len(str_value) > max_length:
                                    print(f"警告: 字段 {field} 的值超出长度限制 ({len(str_value)} > {max_length})")
                                    str_value = str_value[:max_length]
                                data[field] = str_value
                                
                            # 处理数值类型字段
                            elif 'DECIMAL' in field_type or 'INT' in field_type:
                                data[field] = float(value) if pd.notna(value) else None
                                
                            # 处理期类型字段
                            elif 'DATE' in field_type:
                                # 转换日期格式为 YYYYMMDD
                                data[field] = pd.to_datetime(value).strftime('%Y%m%d') if pd.notna(value) else None
                                
                            else:
                                data[field] = str(value) if pd.notna(value) else None
                        
                        # 检查必要字段是否为空
                        if not data.get('ts_code') or not data.get('exchange'):
                            print(f"跳过无效数据: ts_code={data.get('ts_code')}, exchange={data.get('exchange')}")
                            skip_count += 1
                            continue
                        
                        # 按字段顺序准备数据
                        insert_data = (
                            data.get('ts_code'),
                            data.get('symbol'),
                            data.get('exchange'),
                            data.get('name'),
                            data.get('fut_code'),
                            data.get('multiplier'),
                            data.get('trade_unit'),
                            data.get('per_unit'),
                            data.get('quote_unit'),
                            data.get('quote_unit_desc'),
                            data.get('d_mode_desc'),
                            data.get('list_date'),
                            data.get('delist_date'),
                            data.get('d_month'),
                            data.get('last_ddate'),
                            data.get('trade_time_desc')
                        )
                        
                        cursor.execute(insert_query, insert_data)
                        insert_count += 1
                        
                    except Exception as e:
                        import sys
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        error_msg = f"插入数据失败: {str(e)} (文件: {fname}, 行号: {exc_tb.tb_lineno})"
                        print(error_msg)
                        print(f"问题数据: {data}")
                        skip_count += 1
                        continue
                        
                self.connection.commit()
                print(f"合约信息更新完成: 插入 {insert_count} 条记录，跳过 {skip_count} 条记录")
                return True
                
            except Exception as e:
                error_msg = f"新合约数据失败: {str(e)}"
                print(error_msg)
                logging.error(error_msg)
                if self.connection:
                    self.connection.rollback()
                return False
            finally:
                if cursor:
                    cursor.close()
                
        except Exception as e:
            error_msg = f"更新合约数据失败: {str(e)}"
            print(error_msg)
            logging.error(error_msg)
            return False
    
    def get_contracts_by_future_code(self, exchange, fut_code):
        """获取指定品种的所有未到期合约"""
        query = """
        SELECT 
            ts_code,
            name,
            exchange,
            fut_code,
            delist_date
        FROM futures_basic
        WHERE exchange = %s 
        AND fut_code = %s
        AND delist_date > CURDATE()
        ORDER BY ts_code
        """
        try:
            # 使用原生MySQL查询
            with self.connection.cursor() as cursor:
                cursor.execute(query, (exchange, fut_code))
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                
            # 手动创建DataFrame
            df = pd.DataFrame(data, columns=columns)
            
            # 转换日期格式
            if not df.empty and 'delist_date' in df.columns:
                df['delist_date'] = pd.to_datetime(df['delist_date']).dt.strftime('%Y-%m-%d')
                
            return df
            
        except Exception as e:
            logging.error(f"获取种合约数据失败: {str(e)}")
            return None
    
    def get_last_trade_date(self):
        """获取最后有效交易日"""
        try:
            current_time = datetime.now()
            current_date = current_time.date()
            
            # 如果是周末（周六或周日）
            if current_time.weekday() >= 5:
                # 获取上周五的期
                days_to_friday = current_time.weekday() - 4
                last_trade_date = current_date - timedelta(days=days_to_friday)
                print(f"当前是周末，返回上周五日期: {last_trade_date}")
                return last_trade_date
                
            # 如果是工作日（周一到周五）
            else:
                # 如果在17:00之后，使用当前日期
                if current_time.hour >= 17:
                    print(f"当前时间在17:00之后，返回当天日期: {current_date}")
                    return current_date
                    
                # 如果在15:00之前，获取上一个工作日
                elif current_time.hour < 15:
                    # 如果是周一
                    if current_time.weekday() == 0:
                        # 返上周五
                        last_trade_date = current_date - timedelta(days=3)
                    else:
                        # 返回前一天
                        last_trade_date = current_date - timedelta(days=1)
                    print(f"当前时间在15:00之前，返回上一个交易日: {last_trade_date}")
                    return last_trade_date
                    
                # 15:00-17:00之间，使用当前日期
                else:
                    print(f"当前时间在15:00-17:00之间，返回当天日期: {current_date}")
                    return current_date
                    
        except Exception as e:
            error_msg = f"获取最后交易日失败: {str(e)}"
            print(error_msg)
            logging.error(error_msg)
            return None
    
    def check_quote_exists(self, ts_code, trade_date):
        """检查某个合约的行情数据是否存在"""
        try:
            # 首先确保表存在
            create_table_query = """
            CREATE TABLE IF NOT EXISTS futures_daily_quotes (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                ts_code VARCHAR(20),
                trade_date DATE,
                open DECIMAL(20,4),
                high DECIMAL(20,4),
                low DECIMAL(20,4),
                close DECIMAL(20,4),
                pre_close DECIMAL(20,4),
                change_rate DECIMAL(20,4),
                vol DECIMAL(20,4),
                amount DECIMAL(20,4),
                oi DECIMAL(20,4),
                oi_chg DECIMAL(20,4),
                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY idx_ts_trade (ts_code, trade_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
            
            with self.connection.cursor() as cursor:
                # 创建表
                cursor.execute(create_table_query)
                self.connection.commit()
                
                # 检查数据是否存在
                query = """
                SELECT COUNT(*) 
                FROM futures_daily_quotes 
                WHERE ts_code = %s AND trade_date = %s
                """
                cursor.execute(query, (ts_code, trade_date))
                count = cursor.fetchone()[0]
                return count > 0
                
        except Exception as e:
            error_msg = f"检查行情数据失败: {str(e)}"
            print(error_msg)
            logging.error(error_msg)
            return False
    
    @error_handler(logger=logging)
    def get_main_contracts(self, exchange, fut_code):
        """获取主力合约"""
        try:
            # 1. 先获取该品种的最新交易日期
            date_query = """
            SELECT DATE_FORMAT(MAX(trade_date), '%Y-%m-%d') as latest_date
            FROM futures_daily_quotes
            """
            
            with self.connection.cursor() as cursor:
                cursor.execute(date_query)
                latest_date = cursor.fetchone()[0]
                
                if not latest_date:
                    logging.warning("未找到任何行情数据")
                    return None
                
                logging.debug(f"最新交易日期: {latest_date}")
                
                # 2. 获取该品种所有未到期合约在最新交易日的数据
                query = """
                SELECT 
                    q.ts_code,
                    CAST(COALESCE(q.vol, 0) AS DECIMAL(20,4)) as volume,
                    CAST(COALESCE(q.oi, 0) AS DECIMAL(20,4)) as position,
                    q.trade_date
                FROM futures_basic b
                LEFT JOIN futures_daily_quotes q 
                    ON b.ts_code = q.ts_code 
                    AND DATE(q.trade_date) = DATE(%s)
                WHERE b.exchange = %s 
                AND b.fut_code = %s
                AND b.delist_date >= %s
                HAVING volume > 0 OR position > 0
                ORDER BY (volume * 0.4 + position * 0.6) DESC
                """
                
                logging.debug(f"执行查询: {query}")
                logging.debug(f"参数: {latest_date}, {exchange}, {fut_code}, {latest_date}")
                
                cursor.execute(query, (latest_date, exchange, fut_code, latest_date))
                contracts_data = cursor.fetchall()
                
                if not contracts_data:
                    # 如果没有找到有成交的合约，使用最近到期的合约作为备选
                    backup_query = """
                    SELECT ts_code
                    FROM futures_basic
                    WHERE exchange = %s
                    AND fut_code = %s
                    AND delist_date >= %s
                    ORDER BY delist_date ASC
                    LIMIT 1
                    """
                    cursor.execute(backup_query, (exchange, fut_code, latest_date))
                    backup_result = cursor.fetchone()
                    if backup_result:
                        logging.info(f"使用备选合约: {backup_result[0]} (无成交量数据)")
                        return backup_result[0]
                    
                    logging.warning(f"未找到任何有效合约: {exchange}.{fut_code}")
                    return None
                
                # 3. 找出成交量和持仓量最大的合约
                max_score = 0
                main_contract = None
                main_volume = 0
                main_position = 0
                
                for ts_code, volume, position, trade_date in contracts_data:
                    # 计算得分 (成交量40%，持仓量60%)
                    score = float(volume) * 0.4 + float(position) * 0.6
                    
                    logging.info(
                        f"合约: {ts_code}\n"
                        f"  交易日期: {trade_date}\n"
                        f"  成交量: {volume}\n"
                        f"  持仓量: {position}\n"
                        f"  得分: {score}"
                    )
                    
                    if score > max_score:
                        max_score = score
                        main_contract = ts_code
                        main_volume = volume
                        main_position = position
                
                if main_contract:
                    logging.info(
                        f"\n选定主力合约: {main_contract}\n"
                        f"交易日期: {latest_date}\n"
                        f"成交量: {main_volume}\n"
                        f"持仓量: {main_position}\n"
                        f"最终得分: {max_score}"
                    )
                    return main_contract
                else:
                    logging.warning(f"未找到 {exchange}.{fut_code} 的主力合约")
                    return None
                
        except Exception as e:
            error_msg = f"获取主力合约失败: {str(e)}"
            logging.error(f"{error_msg}\n{traceback.format_exc()}")
            return None
    
    def get_contract_quotes(self, ts_code, days=1):
        """获取合约行情数据"""
        query = """
        SELECT ts_code, trade_date, open, high, low, close, 
               vol, amount, oi
        FROM futures_daily_quotes
        WHERE ts_code = %s
        AND trade_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
        ORDER BY trade_date DESC
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (ts_code, days))
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=columns)
                if not df.empty:
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
                return df
        except Exception as e:
            logging.error(f"获取合约行失败: {str(e)}")
            return None
    
    @error_handler(logger=logging)
    def save_main_contract(self, trade_date, exchange, fut_code, ts_code, vol, amount, oi):
        """保存主力合约信息"""
        try:
            # 确保据类型正确，处理 None 值
            def safe_float(value, default=0.0):
                """安全地转换为浮点数"""
                try:
                    if value is None or pd.isna(value):
                        return default
                    return float(value)
                except (TypeError, ValueError):
                    return default

            # 使用安全转换
            vol = safe_float(vol)
            amount = safe_float(amount)
            oi = safe_float(oi)
            
            # 格式化日期
            if isinstance(trade_date, str):
                try:
                    trade_date = datetime.strptime(trade_date, '%Y-%m-%d').date()
                except ValueError as e:
                    logging.error(f"日期格式错误: {trade_date}, {str(e)}")
                    return False
            
            # 验证必要字段
            if not all([trade_date, exchange, fut_code, ts_code]):
                logging.error(f"缺少必要字段: trade_date={trade_date}, exchange={exchange}, "
                             f"fut_code={fut_code}, ts_code={ts_code}")
                return False
            
            query = """
            INSERT INTO futures_main_contract 
                (trade_date, exchange, fut_code, ts_code, vol, amount, oi)
            VALUES 
                (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                ts_code = VALUES(ts_code),
                vol = VALUES(vol),
                amount = VALUES(amount),
                oi = VALUES(oi)
            """
            
            with self.transaction() as cursor:
                cursor.execute(query, (
                    trade_date,
                    exchange,
                    fut_code,
                    ts_code,
                    vol,
                    amount,
                    oi
                ))
                
            logging.info(f"保存主力合约信息成功: {exchange}.{fut_code} -> {ts_code} "
                        f"(vol={vol}, amount={amount}, oi={oi})")
            return True
                
        except Exception as e:
            error_msg = f"保存主力合约信息失败: {str(e)}"
            logging.error(f"{error_msg}\n{traceback.format_exc()}")
            return False

    def create_main_contract_table(self):
        """创建主力合约表"""
        try:
            # 先删除旧表（如果存在）
            drop_query = """
            DROP TABLE IF EXISTS futures_main_contract
            """
            
            # 创建新表
            create_query = """
            CREATE TABLE futures_main_contract (
                trade_date DATE NOT NULL,
                exchange VARCHAR(20) NOT NULL,
                fut_code VARCHAR(20) NOT NULL,
                ts_code VARCHAR(20) NOT NULL,
                vol DECIMAL(20,4) DEFAULT 0,
                amount DECIMAL(20,4) DEFAULT 0,
                oi DECIMAL(20,4) DEFAULT 0,
                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (trade_date, exchange, fut_code)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            
            with self.transaction() as cursor:
                cursor.execute(drop_query)
                cursor.execute(create_query)
                logging.info("主力合约表重建成功")
            return True
            
        except Exception as e:
            error_msg = f"创建主力合约表失败: {str(e)}"
            logging.error(f"{error_msg}\n{traceback.format_exc()}")
            return False
    
    def get_main_contract(self, exchange, fut_code, trade_date=None):
        """获取指定日期的主力合约"""
        try:
            query = """
            SELECT ts_code
            FROM futures_main_contract
            WHERE exchange = %s 
            AND fut_code = %s
            AND trade_date = %s
            """
            
            if trade_date is None:
                trade_date = datetime.now().strftime('%Y-%m-%d')
            
            with self.connection.cursor() as cursor:
                cursor.execute(query, (exchange, fut_code, trade_date))
                result = cursor.fetchone()
                return result[0] if result else None
                
        except Exception as e:
            logging.error(f"获取主力合约失败: {str(e)}")
            return None
    
    def get_valid_contracts(self):
        """获取所有有效合约（未到期的合约）"""
        try:
            if not self.ensure_connected():
                return None
                
            # 获取当前日期
            today = datetime.now().strftime('%Y-%m-%d')
            
            query = """
            SELECT DISTINCT
                ts_code,
                name,
                exchange,
                fut_code,
                delist_date as last_ddate
            FROM futures_basic
            WHERE delist_date > %s  # 使用参数化查询
            ORDER BY exchange, fut_code, ts_code
            """
            
            # 使用原生MySQL查询
            with self.connection.cursor() as cursor:
                cursor.execute(query, (today,))
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                
            # 手动创建DataFrame
            df = pd.DataFrame(data, columns=columns)
            
            # 转换日期格式
            if not df.empty and 'last_ddate' in df.columns:
                df['last_ddate'] = pd.to_datetime(df['last_ddate']).dt.strftime('%Y-%m-%d')
                
                # 再次过滤，确日期比较正确
                df = df[pd.to_datetime(df['last_ddate']) > pd.to_datetime(today)]
                
            valid_count = len(df) if df is not None else 0
            print(f"获取到{valid_count}个有效合约")
            
            return df
            
        except Exception as e:
            error_msg = f"获取有效合约数据失败: {str(e)}"
            print(error_msg)
            logging.error(error_msg)
            return None
    
    @error_handler(logger=logging)
    def save_quotes(self, df):
        """保存货行情数据"""
        if df is None or df.empty:
            return False
            
        with self.transaction() as cursor:
            # 使用QueryBuilder构建SQL
            fields = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 
                     'pre_close', 'change_rate', 'vol', 'amount', 'oi']
            insert_query = QueryBuilder.build_insert('futures_daily_quotes', fields)
            
            for _, row in df.iterrows():
                # 检查数据是否存在
                check_query = QueryBuilder.build_select(
                    'futures_daily_quotes',
                    'COUNT(*)',
                    'ts_code = %s AND trade_date = %s'
                )
                cursor.execute(check_query, (row['ts_code'], row['trade_date']))
                if cursor.fetchone()[0] > 0:
                    continue
                
                # 准备数据并插入
                data = self._prepare_quote_data(row)
                cursor.execute(insert_query, [data[field] for field in fields])
                
        return True

    def _prepare_quote_data(self, row):
        """准备行情数据"""
        return {
            'ts_code': str(row['ts_code'])[:20],
            'trade_date': row['trade_date'],
            'open': float(row['open']) if pd.notnull(row['open']) else None,
            'high': float(row['high']) if pd.notnull(row['high']) else None,
            'low': float(row['low']) if pd.notnull(row['low']) else None,
            'close': float(row['close']) if pd.notnull(row['close']) else None,
            'pre_close': float(row['pre_close']) if pd.notnull(row['pre_close']) else None,
            'change_rate': self._calculate_change_rate(row),
            'vol': float(row['vol']) if pd.notnull(row['vol']) else None,
            'amount': float(row['amount']) if pd.notnull(row['amount']) else None,
            'oi': float(row['oi']) if pd.notnull(row['oi']) else None
        }

    @staticmethod
    def _calculate_change_rate(row):
        """计算涨跌幅"""
        if pd.notnull(row['close']) and pd.notnull(row['pre_close']) and row['pre_close'] != 0:
            return ((row['close'] - row['pre_close']) / row['pre_close'] * 100)
        return None
    
    @error_handler(logger=logging)
    def update_main_contracts(self):
        """更新主力合约信息"""
        try:
            # 1. 获取最新交易日期的所有行情数据
            query = """
            SELECT 
                q.ts_code COLLATE utf8mb4_unicode_ci,
                b.exchange,
                b.fut_code,
                q.vol,
                q.oi,
                q.amount,
                q.trade_date
            FROM futures_daily_quotes q
            JOIN futures_basic b ON q.ts_code COLLATE utf8mb4_unicode_ci = b.ts_code
            WHERE q.trade_date = (
                SELECT MAX(trade_date) 
                FROM futures_daily_quotes
            )
            AND b.delist_date > DATE_FORMAT(NOW(), '%Y-%m-%d')
            """
            
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                all_data = cursor.fetchall()
                
                if not all_data:
                    logging.warning("未找到任何行情数据")
                    return 0, 0
                
                # 2. 按交易所和品种分组数据
                grouped_data = {}
                for row in all_data:
                    ts_code, exchange, fut_code, vol, oi, amount, trade_date = row
                    key = (exchange, fut_code)
                    if key not in grouped_data:
                        grouped_data[key] = []
                    grouped_data[key].append({
                        'ts_code': ts_code,
                        'vol': float(vol) if vol is not None else 0,
                        'oi': float(oi) if oi is not None else 0,
                        'amount': float(amount) if amount is not None else 0,
                        'trade_date': trade_date
                    })
                
                # 3. 处理每个品种的数据
                success_count = 0
                fail_count = 0
                
                for (exchange, fut_code), contracts in grouped_data.items():
                    try:
                        logging.info(f"\n处理 {exchange} {fut_code}")
                        
                        # 计算每个合约的得分
                        max_score = 0
                        main_contract = None
                        main_data = None
                        
                        for contract in contracts:
                            score = contract['vol'] * 0.4 + contract['oi'] * 0.6
                            logging.debug(
                                f"合约: {contract['ts_code']}\n"
                                f"  成交量: {contract['vol']}\n"
                                f"  持仓量: {contract['oi']}\n"
                                f"  得分: {score}"
                            )
                            
                            if score > max_score:
                                max_score = score
                                main_contract = contract['ts_code']
                                main_data = contract
                        
                        if main_contract and main_data:
                            # 保存主力合约信息
                            if self.save_main_contract(
                                trade_date=main_data['trade_date'],
                                exchange=exchange,
                                fut_code=fut_code,
                                ts_code=main_contract,
                                vol=main_data['vol'],
                                amount=main_data['amount'],
                                oi=main_data['oi']
                            ):
                                success_count += 1
                                logging.info(f"更新{exchange} {fut_code}主力合约成功: {main_contract}")
                            else:
                                fail_count += 1
                                logging.error(f"保存{exchange} {fut_code}主力合约失败")
                        else:
                            fail_count += 1
                            logging.warning(f"未找到{exchange} {fut_code}的主力合约")
                            
                    except Exception as e:
                        fail_count += 1
                        error_msg = f"更新{exchange} {fut_code}主力合约失败: {str(e)}"
                        logging.error(error_msg)
                        continue
                
                summary = (
                    f"\n{'='*50}\n"
                    f"主力合约更新完成\n"
                    f"成功: {success_count}\n"
                    f"失败: {fail_count}\n"
                    f"{'='*50}"
                )
                logging.info(summary)
                return success_count, fail_count
                
        except Exception as e:
            error_msg = f"更新主力合约失败: {str(e)}"
            logging.error(f"{error_msg}\n{traceback.format_exc()}")
            raise