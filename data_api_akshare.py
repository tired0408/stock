"""
使用akshar获取股票数据的API接口
"""
import os
import datetime
import pandas as pd
import akshare as ak
from typing import List

def get_trade_date() -> List[datetime.time]:
    """获取交易日历"""
    today = datetime.date.today()
    df_trade_date = ak.tool_trade_date_hist_sina()
    df_trade_date = df_trade_date[df_trade_date['trade_date'] < today]
    df_trade_date = df_trade_date.tail(10)
    df_trade_date = df_trade_date['trade_date'].tolist()
    return df_trade_date

def get_szse_summary(date_range: List[datetime.time]) -> List[float]:
    """获取深圳证券交易所的成交数据"""
    rd = []
    for search_date in date_range:
        search_date = search_date.strftime('%Y%m%d')
        df = ak.stock_szse_summary(date=search_date)
        df = df.loc[df["证券类别"].isin(["主板A股", "创业板A股"]), ["成交金额"]]
        rd.append(df.sum())
    return rd

def get_shse_summary(date_range: List[datetime.time]) -> List[float]:
    """获取上证证券交易所的成交数据"""
    rd = []
    for search_date in date_range:
        search_date = search_date.strftime('%Y%m%d')
        df = ak.stock_sse_deal_daily(date=search_date)
        df = df.loc[df["单日情况"] == "成交金额", ["主板A", "科创板"]]
        rd.append(df.sum())
    return rd

def get_dfcf_concept_summary(date_range: List[datetime.time], name) -> List[float]:
    """获取东方财富概念板块的历史成交额数据"""
    st = date_range[0].strftime('%Y%m%d')
    ed = date_range[-1].strftime('%Y%m%d')
    df = ak.stock_board_concept_hist_em(symbol=name, period="daily", start_date=st, end_date=ed, adjust="qfq")
    return df["成交额"].tolist()

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
    

def main():
    """主函数"""
    trade_date = get_trade_date()
    sz_amount = get_szse_summary(trade_date)
    sh_amount = get_shse_summary(trade_date)
    hs300_amount = get_dfcf_concept_summary(trade_date, "HS300_")
    micro_amount = get_dfcf_concept_summary(trade_date, "微盘股")
    hundred_amount = get_dfcf_concept_summary(trade_date, "百元股")
    top20_amount = get_top20_summary(trade_date)



if __name__ == "__main__":
    main()
