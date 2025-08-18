"""
整理期货的交易数据
"""
import os
import re
import collections
import xlsxwriter
import pandas as pd


output_data = []
open_position_list = collections.deque()  # 开仓的数据信息
path = r"E:\NewFolder\gushi\期货成交明细"
for file_name in os.listdir(path):
    trade_date = re.search(r"\d{4}-\d{2}-\d{2}", file_name)
    if trade_date is not None:
        trade_date = trade_date.group()
        print(f"当前文件为交易明细日报:{file_name}")
    else:
        print(f"当前文件为成交明细月报:{file_name}")
    excel_path = os.path.join(path, file_name)
    df = pd.read_excel(excel_path, sheet_name="成交明细", header=9)
    df = df.iloc[:-1]
    for _, row in df.iterrows():
        if "IM" not in row["合约"]:
            continue
        num = row["手数"]
        each_trade_date = row["交易日期"] if trade_date is None else trade_date
        each_status = str.strip(row["开/平"])
        each_trade_type = str.strip(row["买/卖"])
        each_data = [row["合约"], each_trade_date, row["成交时间"], each_trade_type, row["成交价"], each_status, row["手续费"]/num]
        # 进行开仓操作
        for _ in range(num):
            if len(open_position_list) == 0 or open_position_list[-1][3] == each_trade_type:
                open_position_list.append(each_data.copy())
            else:
                each_full_data: list = open_position_list.pop()
                each_full_data.extend(each_data[1:])
                # 计算盈亏情况
                each_full_data.append(each_full_data[4] - row["成交价"] if each_trade_type == "买" else row["成交价"] - each_full_data[4])
                output_data.append(each_full_data)
print("数据已整理完毕")
if len(open_position_list) == 0:
    print(f"截至{output_data[-1][1]},账户状态为平仓状态")
else:
    print(f"截至{open_position_list[-1][1]},账户状态为开{open_position_list[-1][3]}仓状态")
print("输出整理后的成交明细表")
wb = xlsxwriter.Workbook(os.path.join(os.path.dirname(path), "整理成交明细.xlsx"))
ws = wb.add_worksheet()
# 构造标题
merge_format = wb.add_format({
    'align': 'center',
    'valign': 'vcenter',
})
ws.merge_range(0, 0, 1, 0, "合约", merge_format)
ws.merge_range(0, 1, 0, 6, "开仓操作", merge_format)
ws.write(1, 1, "交易日期")
ws.write(1, 2, "成交时间")
ws.write(1, 3, "买/卖")
ws.write(1, 4, "成交价")
ws.write(1, 5, "开/平")
ws.write(1, 6, "手续费")
ws.merge_range(0, 7, 0, 12, "平仓操作", merge_format)
ws.write(1, 7, "交易日期")
ws.write(1, 8, "成交时间")
ws.write(1, 9, "买/卖")
ws.write(1, 10, "成交价")
ws.write(1, 11, "开/平")
ws.write(1, 12, "手续费")
ws.merge_range(0, 13, 1, 13, "盈亏", merge_format)
# 写入数据
for row_i, data in enumerate(output_data):
    for col_i, value in enumerate(data):
        ws.write(row_i+2, col_i, value)
# 保存文件
wb.close()
print("输出完成")