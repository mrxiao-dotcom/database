我想要设计一个windows界面程序，来维护数据库，其中数据库表格的信息，由database.md文件提供，数据库连接信息在：
    def __init__(self):
        self.host = "10.17.31.47"
        self.user = "root"
        self.password = "fsR6Hf$"
        self.port = 3306
        self.database = "tbauto"
        self.table = "tbPriceData"
数据获取的方法是调用tushare接口：
@Web @https://tushare.pro/document/2?doc_id=134 
在这个网站内

请帮我架构这个程序，并提供代码。

功能：
1、从tushare获取数据到数据库，包括：
    1.1 期货品种信息，即各个市场的期货品种信息，市场与品种的对应关系，期货合约名称、合约代码等，具体参考database.md文件
    1.2 期货历史数据，包括：主力合约最近30个交易日的行情
    1.3 每个交易日机构成交的持仓数据
2、从数据库中读取数据，并进行可视化，包括：
    2.1 期货品种信息，即各个市场的期货品种信息，市场与品种的对应关系，期货合约名称、合约代码等，具体参考database.md文件
    2.2 期货历史数据，包括：主力合约最近30个交易日的行情
    2.3 每个交易日机构成交的持仓数据

3、定时任务
    3.1 每天定时从tushare获取数据到数据库，下午17：00 自动执行
    3.2 执行遇到故障，要进行通知，并进行邮件报警
