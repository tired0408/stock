"""
使用akshar获取股票数据的API接口
"""
import os
import datetime
import pandas as pd
import numpy as np
import akshare as ak
from typing import List

data_path = r"E:\py-workspace\stock\data"
def trade_date():
    """获取交易日期"""
    path = os.path.join(data_path, "trade_date.csv")
    if os.path.exists(path):
        df = pd.read_csv(path)
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    else:
        df = ak.tool_trade_date_hist_sina()
        df.to_csv(path, index=False)
    return df

def cache(name):
    """单个文件读取的装饰器"""
    def decorator(func):
        def wrapper(date_str, *args, **kwargs):
            folder = os.path.join(data_path, date_str)
            if not os.path.exists(folder):
                os.makedirs(folder)
            path = os.path.join(folder, f"{name}.csv")
            if os.path.exists(path):
                df = pd.read_csv(path)
            else:
                df: pd.DataFrame = func(date_str, *args, **kwargs)
                df.to_csv(path, index=False)
            return df
        return wrapper
    return decorator

@cache("szse_summary")
def szse_summary(date_str):
    """获取深证交易所的成交数据
    
    Args:
        date_str (str): 日期,例如20251011
    """
    return ak.stock_szse_summary(date=date_str)

@cache("shse_summary")
def shse_summary(date_str):
    """获取上证交易所的成交数据"""
    return ak.stock_sse_deal_daily(date=date_str)


def concept_summary(date_range: List[datetime.time], name):
    """获取概念板块的成交数据"""
    df = __concept_summary_by_local(date_range, name)
    if df is not None:
        return df
    st = date_range[0].strftime('%Y%m%d')
    ed = date_range[-1].strftime('%Y%m%d')
    df = ak.stock_board_concept_hist_em(symbol=name, period="daily", start_date=st, end_date=ed)
    for _, row in df.iterrows():
        each_date: datetime.time = datetime.datetime.strptime(row["日期"], "%Y-%m-%d")
        cache_path = os.path.join(data_path, each_date.strftime('%Y%m%d'), "concept_summary.csv")
        row = row[row.index != "日期"]
        row = pd.concat([pd.Series([name], index=['名称']), row])
        if os.path.exists(cache_path):
            df = pd.read_csv(cache_path)
            index_bool = df["名称"] == name
            if index_bool.any():
                df.loc[index_bool, row.index] = row.values
            else:
                df = pd.concat([df, row.to_frame().T], ignore_index=True)
        else:
            df = row.to_frame().T
        df.to_csv(cache_path, index=False)
    return df


@cache("sh_a_spot")
def sh_a_spot(date_str):
    """获取上证A股的成分股数据"""
    return __market_classify(date_str, "sh_a_spot", ak.stock_sh_a_spot_em)

@cache("sz_a_spot")
def sz_a_spot(date_str):
    """获取深证A股的成分股数据"""
    return __market_classify(date_str, "sz_a_spot", ak.stock_sz_a_spot_em)

@cache("cy_a_spot")
def cy_a_spot(date_str):
    """获取创业板A股的成分股数据"""
    return __market_classify(date_str, "cy_a_spot", ak.stock_cy_a_spot_em)

@cache("kc_a_spot")
def kc_a_spot(date_str):
    """获取科创板的成分股数据"""
    return __market_classify(date_str, "kc_a_spot", ak.stock_kc_a_spot_em)

def __concept_summary_by_local(date_range: List[datetime.time], symbol):
    """从本地文件中读取概念板块的数据"""
    rd = []
    for search_date in date_range:
        date_str = search_date.strftime('%Y%m%d')
        folder = os.path.join(data_path, date_str)
        if not os.path.exists(folder):
            return None
        path = os.path.join(folder, f"concept_summary.csv")
        if not os.path.exists(path):
            return None
        df: pd.DataFrame = pd.read_csv(path)
        df = df[df["名称"] == symbol]
        if len(df) == 0:
            return None
        df = df.drop("名称", axis=1)
        df.insert(0, "日期", search_date)
        rd.append(df)
    return pd.concat(rd, axis=0, ignore_index=True)

def __market_classify(date_str, name, method):
    """获取根据市场分类的股票数据"""
    # 直接读取当天数据
    today = datetime.datetime.now()
    if today.strftime('%Y%m%d') == date_str:
        df = __realtime2history(method())
        return df
    # 读取历史数据
    tmp_path = os.path.join(data_path, date_str, f"{name}_tmp.csv")
    if os.path.exists(tmp_path):
        datas = pd.read_csv(tmp_path)
    else:
        datas: pd.DataFrame = __realtime2history(method())
        datas.loc[:, ~datas.columns.isin(["代码", "名称"])] = np.nan
    try:
        for index, row in datas.iterrows():
            if not pd.isnull(row["换手率"]):
                continue
            code = str(row["代码"])
            df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=date_str, end_date=date_str)
            df = df.drop(["日期", "股票代码"], axis=1)
            series = df.iloc[0]
            datas.loc[index, series.index] = series.values
    finally:
        count = datas["换手率"].isna().any(axis=1).sum()
        print(f"数据缺失行数:{count}")
        datas.to_csv(tmp_path, index=False)
    return datas

def __realtime2history(datas: pd.DataFrame):
    """将实时数据转换为历史数据"""
    datas = datas.rename(columns={'今开': '开盘', '最新价': '收盘'})
    datas = datas[["代码", "名称", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]]
    return datas

if __name__ == "__main__":
    pass
