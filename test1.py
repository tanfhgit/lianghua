import akshare as ak
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# ---------------------- 数据库配置（utf8mb4编码） ----------------------
DB_CONFIG = {
    "host": "localhost",       # 本地数据库地址
    "port": 3306,              # MySQL默认端口
    "user": "root",            # 你的MySQL用户名
    "password": "Tanfh12#$",  # 替换为你的MySQL密码
    "database": "future_market_data",  # 数据库名
    "charset": "utf8mb4"       # 统一用utf8mb4编码
}

# ---------------------- 1. 获取螺纹钢主力合约数据 ----------------------
def get_rb_main_data():
    # 获取当前主力合约代码
    main_symbol = "RB2605"

    
    # 下载日线行情数据
    df = ak.futures_zh_daily_sina(symbol=main_symbol)
    
    # 数据清洗与格式转换
    df = df.rename(columns={
        "date": "trade_date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
        "hold":"hold",
        "settle": "settle"
    })
    # 日期格式转换为MySQL的DATE类型
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    # 添加合约代码列
    df["symbol"] = main_symbol
    
    # 保留需要的字段
    df = df[["trade_date", "symbol", "open", "high", "low", "close", "volume", "hold","settle"]]
    return df

# ---------------------- 2. 数据插入MySQL数据库 ----------------------
def insert_data_to_mysql(df):
    try:
        # 创建SQLAlchemy连接引擎（指定utf8mb4编码）
        engine = create_engine(
            f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset=utf8mb4"
        )
        
        # 插入数据（重复主键自动更新）
        df.to_sql(
            name="rb_main_contract",  # 表名
            con=engine,
            if_exists="append",       # 追加模式（已有数据则跳过重复）
            index=False,              # 不插入DataFrame的索引列
            chunksize=1000,           # 批量插入大小（优化性能）
            method="multi"            # 多值插入（提高效率）
        )
        print(f"✅ 成功插入/更新 {len(df)} 条行情数据")

    except Exception as e:
        print(f"❌ 数据插入失败：{str(e)}")
    finally:
        engine.dispose()  # 关闭数据库连接

# ---------------------- 3. 主函数执行 ----------------------
if __name__ == "__main__":
    print(f"[{datetime.now()}] 开始获取螺纹钢行情数据...")
    rb_data = get_rb_main_data()
    insert_data_to_mysql(rb_data)