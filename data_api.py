"""
使用akshar获取股票数据的API接口
"""
import datetime
import pandas as pd
import numpy as np
import utils_akshare as utils
from show_result import create_styled_table
from typing import List
from requests.exceptions import ConnectionError

def get_trade_date() -> List[datetime.time]:
    """获取交易日历"""
    today = datetime.datetime.now()
    if today.hour >= 15:
        today = today + datetime.timedelta(days=1)
    today = today.date()
    start_date = datetime.datetime.strptime('20251013', '%Y%m%d').date()
    df_trade_date = utils.trade_date()
    df_trade_date = df_trade_date[df_trade_date['trade_date'] < today]
    df_trade_date = df_trade_date[df_trade_date['trade_date'] >= start_date]
    df_trade_date = df_trade_date.tail(28)
    df_trade_date = df_trade_date['trade_date'].tolist()
    return df_trade_date

def get_szse_summary(date_range: List[datetime.time]) -> List[float]:
    """获取深圳证券交易所的成交数据:(亿元)"""
    rd = []
    for search_date in date_range:
        search_date = search_date.strftime('%Y%m%d')
        df = utils.szse_summary(search_date)
        df = df.loc[df["证券类别"].isin(["主板A股", "创业板A股"])]
        rd.append(df["成交金额"].sum() / 100000000)
    return rd

def get_shse_summary(date_range: List[datetime.time]) -> List[float]:
    """获取上证证券交易所的成交数据(亿元)"""
    rd = []
    for search_date in date_range:
        search_date = search_date.strftime('%Y%m%d')
        df = utils.shse_summary(search_date)
        df = df.loc[df["单日情况"] == "成交金额", ["主板A", "科创板"]]
        rd.append(df.iloc[0].sum())
    return rd

def get_dfcf_concept_summary(date_range: List[datetime.time], name) -> List[float]:
    """获取东方财富概念板块的历史成交额数据"""
    df: pd.DataFrame = utils.concept_summary(date_range, name)
    df["成交额"] = df["成交额"] / 100000000
    return df["成交额"].tolist()

def get_top20_summary(date_range: List[datetime.time]) -> List[float]:
    """获取沪深每日成交额前20的总计额度"""
    rd = []
    for search_date in date_range:
        search_date = search_date.strftime('%Y%m%d')
        data1 = utils.sh_a_spot(search_date)
        data2 = utils.sz_a_spot(search_date)
        data3 = utils.cy_a_spot(search_date)
        data4 = utils.kc_a_spot(search_date)
        df = pd.concat([data1, data2, data3, data4], axis=0)
        top20_amount = df["成交额"].nlargest(20).sum()
        rd.append(top20_amount / 100000000)
    return rd    

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
    hs300_amount = get_dfcf_concept_summary(trade_date, "HS300_")
    print("获取微盘股成交数据")
    micro_amount = get_dfcf_concept_summary(trade_date, "微盘股")
    print("获取百元股成交数据")
    hundred_amount = get_dfcf_concept_summary(trade_date, "百元股")
    print("将数据转化成所需图表")
    data2html(trade_date, total_amout, hs300_amount, micro_amount, hundred_amount, top20_amount)

if __name__ == "__main__":
    try:
        main()
    except ConnectionError as e:
        print("访问接口地址连接失败,请稍后重试")
