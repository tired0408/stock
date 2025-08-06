"""
整理期货的交易数据
"""
import os
import collections
import xlsxwriter
import pandas as pd


output_data = []
open_position_list = collections.deque()
path = r"E:\NewFolder\gushi\期货成交明细"
for excel_path in os.listdir(path):
    excel_path = os.path.join(path, excel_path)
    df = pd.read_excel(excel_path, sheet_name="成交明细", header=9)
    df = df.iloc[:-1]
    for _, row in df.iterrows():
        if "IM" not in row["合约"]:
            continue
        num = row["手数"]
        if len(open_position_list) != 0 and open_position_list[-1][3] != row["买/卖"]:
            for _ in range(num):
                each_output: list = open_position_list.pop()
                each_output.extend([row["交易日期"], row["成交时间"], row["买/卖"], row["成交价"], row["开/平"], row["手续费"]/num])
                if each_output[3] == row["买/卖"]:
                    raise Exception("异常数据")
                each_output.append(each_output[4] - row["成交价"] if each_output[3].strip() == "卖" else row["成交价"] - each_output[4])
                output_data.append(each_output)
            continue
        for i in range(num):
            open_position_list.append([row["合约"], row["交易日期"], row["成交时间"], row["买/卖"], row["成交价"], row["开/平"], row["手续费"]/num])
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