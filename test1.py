import akshare as ak
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import mplfinance as mpf

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
def get_rb_main_data(symbol="RB2605"):
    # 下载日线行情数据
    df = ak.futures_zh_daily_sina(symbol=symbol)
    
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
    df["symbol"] = symbol
    
    # 保留需要的字段
    df = df[["trade_date", "symbol", "open", "high", "low", "close", "volume", "hold","settle"]]
    return df

# ---------------------- 2. 数据插入MySQL数据库（修复主键重复问题） ----------------------
def insert_data_to_mysql(df, symbol="RB2605"):
    try:
        # 创建SQLAlchemy连接引擎（指定utf8mb4编码）
        engine = create_engine(
            f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset=utf8mb4"
        )
        
        # ✅ 修复点1：查询数据库中已存在的日期，过滤重复数据
        with engine.connect() as conn:
            # 查询表中已有的(trade_date, symbol)组合
            existing_dates = pd.read_sql(
                f"SELECT trade_date FROM rb_main_contract WHERE symbol = '{symbol}'",
                conn
            )
            # 转换为日期格式，方便对比
            existing_dates["trade_date"] = pd.to_datetime(existing_dates["trade_date"]).dt.date
            # 过滤掉已存在的数据，只保留新数据
            new_data = df[~df["trade_date"].isin(existing_dates["trade_date"])]
        
        if len(new_data) == 0:
            print(" 没有新的行情数据需要插入")
            return
        
        # 插入新数据（无重复，避免主键冲突）
        new_data.to_sql(
            name="rb_main_contract",  # 表名
            con=engine,
            if_exists="append",       # 追加模式（仅插入新数据）
            index=False,              # 不插入DataFrame的索引列
            chunksize=1000,           # 批量插入大小（优化性能）
            method="multi"            # 多值插入（提高效率）
        )
        print(f" 成功插入 {len(new_data)} 条新行情数据")

    except Exception as e:
        print(f"数据插入失败：{str(e)}")
    finally:
        engine.dispose()  # 关闭数据库连接

# ---------------------- 3. 主函数执行 ----------------------
if __name__ == "__main__":
    print(f"[{datetime.now()}] 开始获取螺纹钢行情数据...")
    rb_data = get_rb_main_data("RB2605")
    insert_data_to_mysql(rb_data, "RB2605")
    
    rb_df = rb_data
    
    # ✅ 修复点2：设置日期为DatetimeIndex（解决K线图TypeError）
    rb_df["trade_date"] = pd.to_datetime(rb_df["trade_date"])  # 转换为datetime类型
    rb_df.set_index("trade_date", inplace=True)  # 设置为索引
    
    # 计算均线（5日、20日）
    rb_df["ma5"] = rb_df["close"].rolling(window=5).mean()
    rb_df["ma20"] = rb_df["close"].rolling(window=20).mean()
    
    # 配置绘图样式
    my_style = mpf.make_mpf_style(
        base_mpf_style="yahoo",
        rc={"font.family": "SimHei"},  # 解决中文显示问题
        y_on_right=True
    )
    
    # 绘制K线图+均线
    mpf.plot(
        rb_df.tail(60),  # 显示最近60天数据
        type="candle",  # K线类型
        title=f"RB2605 日线K线图（近60天）",
        ylabel="价格（元/吨）",
        ylabel_lower="成交量",
        volume=True,  # 显示成交量
        mav=(5, 20),  # 显示5日、20日均线
        style=my_style,
        figratio=(16, 9),  # 图片比例
        savefig=f"RB2605_kline.png"  # 保存图片
    )
    
    print(f"\nK线图已保存到：RB2605_kline.png")