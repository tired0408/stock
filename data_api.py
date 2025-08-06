"""
股票获取数据的API接口
"""
import os
import pandas as pd
import akshare as ak


class DataAPI:

    def __init__(self):
        base_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")
        self.a_share_list = os.path.join(base_path, "a_share_list.csv")

    def get_a_share_list(self, update=False):
        if not update and os.path.exists(self.a_share_list):
            return pd.read_csv(self.a_share_list)
        print("通过接口更新A股上市公司列表")
        stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
        stock_zh_a_spot_em_df = stock_zh_a_spot_em_df[["代码", "名称"]]
        stock_zh_a_spot_em_df.to_csv(self.a_share_list, index=False)
        return stock_zh_a_spot_em_df


data = DataAPI()
a_share_list = data.get_a_share_list()
print(a_share_list)
# stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
# print(stock_zh_a_spot_em_df)
# stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="688399", period="daily", start_date="20170301", end_date='20240528', adjust="qfq")
# print(stock_zh_a_hist_df)