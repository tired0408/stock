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
    else:
        df = ak.tool_trade_date_hist_sina()
        df.to_csv(path, index=False)
    return df

def szse_summary(date_str):
    """获取深证交易所的成交数据
    
    Args:
        date_str (str): 日期,例如20251011
    """
    folder = os.path.join(data_path, date_str)
    if not os.path.exists(folder):
        os.makedirs(folder)
    path = os.path.join(folder, "szse_summary.csv")
    if os.path.exists(path):
        df = pd.read_csv(path)
    else:
        df = ak.stock_szse_summary(date=date_str)
        df.to_csv(path, index=False)
    return df

def shse_summary(date_str):
    """获取上证交易所的成交数据
    
    Args:
        date_str (str): 日期,例如20251011
    """
    folder = os.path.join(data_path, date_str)
    if not os.path.exists(folder):
        os.makedirs(folder)
    path = os.path.join(folder, f"shse_summary.csv")
    if os.path.exists(path):
        df = pd.read_csv(path)
    else:
        df = ak.stock_sse_deal_daily(date=date_str)
        df.to_csv(path, index=False)
    return df


def concept_summary(date_str, name):
    """获取东方财富概念板块的成交数据"""
    folder = os.path.join(data_path, date_str)
    if not os.path.exists(folder):
        os.makedirs(folder)
    path = os.path.join(folder, f"concept_summary_{name}.csv")
    if os.path.exists(path):
        df = pd.read_csv(path)
    else:
        df = ak.stock_board_concept_hist_em(symbol=name, period="daily", start_date=date_str, end_date=date_str)
        df.to_csv(path, index=False)
    return df

def sh_a_spot(date_str):
    """获取上证A股的成分股数据"""
    folder = os.path.join(data_path, date_str)
    if not os.path.exists(folder):
        os.makedirs(folder)
    path = os.path.join(folder, f"sh_a_spot.csv")
    if os.path.exists(path):
        df = pd.read_csv(path)
        return df
    else:
        df = __sh_a_spot_by_api(date_str)
        df.to_csv(path, index=False)
    return df

def sh_a_spot_realtime():
    date_str = datetime.datetime.now().strftime("%Y%m%d")
    folder = os.path.join(data_path, date_str)
    if not os.path.exists(folder):
        os.makedirs(folder)
    path = os.path.join(folder, f"sh_a_spot.csv")
    if os.path.exists(path):
        df = pd.read_csv(path)
    else:
        df = ak.stock_sh_a_spot_em()
        df.to_csv(path, index=False)
    return df

def sz_a_spot(date_str):
    """获取深证A股的成分股数据"""
    

def __sh_a_spot_by_api(date_str):
    """通过API获取上证A股的成分数据"""
    today_str = datetime.datetime.now().strftime("%Y%m%d")
    df = sh_a_spot_realtime()
    if date_str == today_str:
        return df    
    columns_to_clear = df.columns[~df.columns.isin(["序号", "代码", "名称"])].tolist()
    df.loc[:, columns_to_clear] = np.nan
    return df


def get_top20_summary(date_range: List[datetime.time]) -> List[float]:
    """获取沪深每日成交额前20的总计额度"""
    st = date_range[0].strftime('%Y%m%d')
    ed = date_range[-1].strftime('%Y%m%d')
    code1 = ak.stock_sh_a_spot_em()["代码"].tolist()  # 所有上证A股代码
    code2 = ak.stock_sz_a_spot_em()["代码"].tolist()  # 所有深证A股代码
    code3 = ak.stock_cy_a_spot_em()["代码"].tolist()  # 所有创业板A股代码
    code4 = ak.stock_kc_a_spot_em()["代码"].tolist()  # 所有科创板代码
    code_list = code1 + code2 + code3 + code4
    df_total = pd.DataFrame(columns=["日期"])
    for code in code_list:
        df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=st, end_date=ed, adjust="qfq")
        df = df[["日期", "成交额"]]
        df = df.rename(columns={'成交额': code})
        df_total = pd.merge(df_total, df, on='日期', how='outer').fillna(0)
    df.drop(columns='日期')
    rd = []
    for _, row in df_total.iterrows():
        rd.append(row.nlargest(20).sum())
    return rd
    
if __name__ == "__main__":
    sh_a_spot("20250903")
