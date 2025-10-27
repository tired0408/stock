"""
使用akshar获取股票数据的API接口
"""
import os
import datetime
import pandas as pd
import numpy as np
import akshare as ak
from typing import List
from tqdm import tqdm
from typing import List
from mootdx.reader import Reader
from show_result import create_styled_table
from requests.exceptions import ConnectionError

data_path = r"E:\py-workspace\stock\data"
tdx_path = r'D:\new_tdx'
class CustomException(Exception):
    """自定义错误"""

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
    rd = ak.stock_szse_summary(date=date_str)
    if len(rd) < 14:
        raise CustomException("深圳交易所的每日概况数据还没有更新")
    return rd

@cache("shse_summary")
def shse_summary(date_str):
    """获取上证交易所的成交数据"""
    return ak.stock_sse_deal_daily(date=date_str)

def shsz_amount(date_range: List[datetime.time]) -> List[pd.DataFrame]:
    """获取沪深两市的成交额数据"""
    reader = Reader.factory(market="std", tdxdir=tdx_path)
    rd = []
    file2pattern = {
        "sh": ("sh60", "sh688"),
        "sz": ("sz000", "sz300")
    }    
    for key, value in file2pattern.items():
        spot_list = os.listdir(os.path.join(tdx_path, "vipdoc", key, "lday"))
        spot_list = [name[2:-4] for name in spot_list if name.startswith(value)]
        for symbol in tqdm(spot_list):
            df = reader.daily(symbol=symbol)
            series = df["amount"].reindex(date_range)
            rd.append(pd.DataFrame([series.values], columns=date_range, index=[symbol]))
    rd = pd.concat(rd)
    return rd

def get_trade_date() -> List[datetime.time]:
    """获取交易日历"""
    today = datetime.datetime.now()
    if today.hour >= 15:
        today = today + datetime.timedelta(days=1)
    today = today.date()
    start_date = datetime.datetime.strptime('20251015', '%Y%m%d').date()
    df_trade_date = trade_date()
    df_trade_date = df_trade_date[df_trade_date['trade_date'] < today]
    df_trade_date = df_trade_date[df_trade_date['trade_date'] >= start_date]
    df_trade_date = df_trade_date.tail(28)
    df_trade_date = df_trade_date['trade_date'].tolist()
    return df_trade_date

def get_szse_summary(date_range: List[datetime.time]) -> List[float]:
    """获取深圳证券交易所的成交数据,单位(亿元)"""
    rd = []
    for search_date in date_range:
        search_date = search_date.strftime('%Y%m%d')
        df = szse_summary(search_date)
        df = df.loc[df["证券类别"].isin(["主板A股", "创业板A股"])]
        rd.append(df["成交金额"].sum() / 100000000)
    return rd

def get_shse_summary(date_range: List[datetime.time]) -> List[float]:
    """获取上证证券交易所的成交数据,单位(亿元)"""
    rd = []
    for search_date in date_range:
        search_date = search_date.strftime('%Y%m%d')
        df = shse_summary(search_date)
        df = df.loc[df["单日情况"] == "成交金额", ["主板A", "科创板"]]
        rd.append(df.iloc[0].sum())
    return rd

def get_top20_summary(date_range: List[datetime.time]) -> List[float]:
    """获取沪深每日成交额前20的总计额度,单位(亿元)"""
    df: pd.DataFrame = shsz_amount(date_range)
    rd = []
    for col in df.columns:
        top20_amount = df[col].nlargest(20).sum()
        rd.append(top20_amount / 100000000)
    return rd

def get_index_summary(date_range: List[datetime.time], symbol) -> List[float]:
    """获取指数概念的历史成交额数据"""
    reader = Reader.factory(market='ext', tdxdir=tdx_path)
    df = reader.daily(symbol=symbol)
    series = df["hk_stock_amount"].reindex(date_range)
    series = series / 100
    return series.tolist()


def get_concept_summary(date_range: List[datetime.time], symbol) -> List[float]:
    """获取概念板块的历史成交额数据,单位(亿元)"""
    reader = Reader.factory(market='std', tdxdir=tdx_path)
    df = reader.daily(symbol=symbol)
    series = df["amount"].reindex(date_range)
    series = series / 100000000
    return series.tolist()
    

def data2html(dates, total, hs300, micro, hundred, top20):
    df_source = pd.DataFrame({
        "沪深两市": total,
        "沪深300": hs300,
        "微盘股": micro,
        "百元股": hundred,
        "沪深前20": top20
    }, index=dates)
    # 计算占比和涨幅
    df_result = pd.DataFrame(index=dates)
    names = df_source.columns[df_source.columns != "沪深两市"].tolist()
    for col in names:
        df_result[col+'_占比'] = df_source[col] / df_source['沪深两市'] * 100
        df_result[col+'_占比涨幅'] = df_result[f"{col}_占比"].pct_change(fill_method=None) * 100
    df_result = df_result.round(2)
    df_result = df_result.reset_index(names='日期')
    # 输出展示图表
    create_styled_table(df_result, [name for name in df_result.columns if "涨幅" in name])
    # 创建图表
    # fig = make_subplots(
    #     rows=2, cols=1,
    #     shared_xaxes=True,
    #     vertical_spacing=0.15,
    #     subplot_titles=("微盘股、百元股等占比", "微盘股、百元股等占比涨幅")
    # )
    # # 添加占比柱状图
    # for _, col in enumerate(names):
    #     fig.add_trace(go.Bar(
    #         x=df_result["日期"].astype(str),  # X轴偏移，使柱并列
    #         y=df_result[col+'_占比'],
    #         name=f'{col}占比',
    #         text=[f'{v:.2%}' for v in df_result[col+'_占比']],  # 鼠标提示显示百分比
    #         hoverinfo='text',
    #     ), row=1, col=1)
    # # 添加涨幅折线
    # for col, color in zip(names, ['blue','orange','green','red']):
    #     fig.add_trace(go.Scatter(
    #         x=df_result["日期"].astype(str),
    #         y=df_result[col+'_占比涨幅'],
    #         mode='lines+markers',
    #         name=f'{col}占比涨幅',
    #         text=[f'{v:.1f}%' if not pd.isna(v) else '' for v in df_result[col+'_占比涨幅']],
    #         marker=dict(color=color),
    #         line=dict(color=color)
    #     ), row=2, col=1)
    # fig.update_layout(
    #     barmode='group',
    #     showlegend=True,
    #     title_text="市场情况分析图表",
    #     margin=dict(l=80, r=80, t=100, b=80)
    # )
    # # X轴设置为分类轴
    # fig.update_xaxes(type='category', row=1, col=1)
    # fig.update_xaxes(type='category', row=2, col=1)
    # # 保存为离线 HTML 文件
    # fig.write_html('股市大盘分析图表.html', auto_open=False)
    # print("已保存为离线html文件")
def main():
    """主函数"""
    trade_date = get_trade_date()
    print("获取股市成交额前20的合计成交额")
    top20_amount = get_top20_summary(trade_date)
    print("获取深圳交易所成交数据")
    sz_amount = get_szse_summary(trade_date)
    print("获取上海交易所成交数据")
    sh_amount = get_shse_summary(trade_date)
    total_amout = (np.array(sz_amount) + np.array(sh_amount)).tolist()
    print("获取沪深300成交数据")
    hs300_amount = get_index_summary(trade_date, '62#000300')
    print("获取微盘股成交数据")
    micro_amount = get_concept_summary(trade_date, "880823")
    print("获取百元股成交数据")
    hundred_amount = get_concept_summary(trade_date, "880878")
    print("将数据转化成所需图表")
    data2html(trade_date, total_amout, hs300_amount, micro_amount, hundred_amount, top20_amount)

if __name__ == "__main__":
    try:
        main()
    except ConnectionError as e:
        print("访问接口地址连接失败,请稍后重试")
