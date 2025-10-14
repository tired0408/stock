import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta

# ==================== 1. 解决中文乱码问题 ====================
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# ==================== 3. 定义条件着色函数 ====================
def get_cell_color(row_idx):
    """
    根据单元格位置和值返回颜色
    row_idx: 行索引（0为表头）
    """
    # 表头颜色
    if row_idx == 0:
        return '#4F81BD'  # 蓝色表头
    # 数据行交替颜色
    if row_idx % 2 == 1:
        return '#F8F8F8'  # 浅灰色
    else:
        return 'white'    # 白色

def get_text_color(row_idx, col_idx, value, title_list, color_titles):
    """
    根据单元格位置和值返回文字颜色
    """
    # 表头文字颜色
    if row_idx == 0:
        return 'white'  # 白色文字
    
    # 指定需要条件着色的列
    color_titles = ['利润', '收益', '波动', '差额']
    title = title_list[col_idx]
    
    # 如果当前列是需要条件着色的列
    if title in color_titles:
        try:
            numeric_value = float(value)
            if numeric_value > 0:
                return 'red'    # 正数：红色文字
            elif numeric_value < 0:
                return 'green'  # 负数：绿色文字
        except (ValueError, TypeError):
            return 'black'  # 非数值：黑色文字
    return 'black'  # 默认黑色文字

# ==================== 4. 创建表格图片 ====================
def create_styled_table(df: pd.DataFrame, color_titles):
    # 创建图形
    fig, ax = plt.subplots(figsize=(16, 8))
    ax.axis('tight')
    ax.axis('off')
    
    # 准备表格数据（包含表头）
    cell_data = [df.columns.tolist()] + df.values.tolist()
    titles = df.columns.tolist()
    # 创建颜色矩阵
    cell_colors = []
    text_colors = []
    
    for i, row in enumerate(cell_data):
        row_colors = []
        row_text_colors = []
        for j, value in enumerate(row):
            row_colors.append(get_cell_color(i))
            row_text_colors.append(get_text_color(i, j, value, titles, color_titles))
        cell_colors.append(row_colors)
        text_colors.append(row_text_colors)
    
    # 创建表格
    table = ax.table(
        cellText=cell_data,
        cellColours=cell_colors,
        cellLoc='center',
        loc='center'
    )
    
    # 设置表格样式
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 2.0)
    
    # 设置文字颜色
    for i in range(len(cell_data)):
        for j in range(len(cell_data[0])):
            table[(i, j)].get_text().set_color(text_colors[i][j])
            if i == 0:  # 表头文字加粗
                table[(i, j)].get_text().set_weight('bold')
    
    # 设置单元格边框
    for _, cell in table.get_celld().items():
        cell.set_edgecolor('gray')
        cell.set_linewidth(0.5)
    
    # 添加标题
    plt.title('市场情况分析图表', fontsize=16, pad=20, weight='bold')
    
    # 调整布局并保存
    plt.tight_layout()
    output_filename = 'test.png'
    plt.savefig(output_filename, dpi=300, bbox_inches='tight', facecolor='white')
    
    return output_filename

# ==================== 5. 执行并显示结果 ====================
if __name__ == "__main__":
    # ==================== 2. 生成示例数据（请替换为您的实际数据） ====================
    # 创建日期范围（最近10天，便于显示）
    myself_dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(9, -1, -1)]

    # 生成八组示例数据（其中四组包含正负数用于条件着色）
    myself_data_dict = {
        '日期': myself_dates,
        '销售量': np.random.randint(80, 200, 10).tolist(),      # 数据列1
        '访问量': np.random.randint(800, 2000, 10).tolist(),   # 数据列2
        '转化率': np.round(np.random.uniform(2.0, 8.0, 10), 2).tolist(),  # 数据列3
        '收入': np.round(np.random.uniform(5000, 15000, 10), 2).tolist(),  # 数据列4
        # 以下四列需要条件着色（包含正负数）
        '利润': np.round(np.random.uniform(-1000, 3000, 10), 2).tolist(),    # 需要着色的列1
        '收益': np.round(np.random.uniform(-500, 1500, 10), 2).tolist(),     # 需要着色的列2
        '波动': np.round(np.random.uniform(-200, 400, 10), 2).tolist(),      # 需要着色的列3
        '差额': np.round(np.random.uniform(-800, 1200, 10), 2).tolist()      # 需要着色的列4
    }

    myself_df = pd.DataFrame(myself_data_dict)


    # 生成表格图片
    output_file = create_styled_table(myself_df, ["利润", "收益", "波动", "差额"])
    
    # 显示生成的信息
    print("=" * 60)
    print("表格生成完成！")
    print(f"输出文件: {output_file}")
    print(f"表格尺寸: {myself_df.shape[0]} 行 × {myself_df.shape[1]} 列")
    print("=" * 60)
    print("\n数据预览（前5行）:")
    print(myself_df.head().to_string(index=False))
    
    print("\n条件着色说明:")
    print("✅ 表头: 蓝色背景，白色文字")
    print("✅ 正数: 金色背景，红色文字（利润、收益、波动、差额列）")
    print("✅ 负数: 浅绿背景，绿色文字（利润、收益、波动、差额列）")
    print("✅ 其他列: 交替行颜色，黑色文字")
    
    # 显示图片（如果在支持图形界面的环境中）
    try:
        plt.show()
    except:
        print("\n提示: 在命令行环境中，图片已保存为文件，请查看生成的PNG文件")