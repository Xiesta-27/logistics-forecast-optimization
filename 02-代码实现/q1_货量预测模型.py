import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 设置 pandas 输出格式（防止打印省略）
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# 参数设定
N_DAYS = 7  # 滑动窗口天数
ALPHA = 0.5  # 趋势项权重
PREDICT_DATE = '2024/12/16'
START_TIME = datetime.strptime('2024/12/15 14:00:00', '%Y/%m/%d %H:%M:%S')
END_TIME = datetime.strptime('2024/12/16 14:00:00', '%Y/%m/%d %H:%M:%S')

# 1. 读取数据
daily_df = pd.read_excel('../03-数据文件/原始数据集/附件3.xlsx')  # 日货量数据
minute_df = pd.read_excel('../03-数据文件/原始数据集/附件2.xlsx')  # 分钟级数据
result_template_1 = pd.read_excel('../03-数据文件/原始数据集/结果表1.xlsx')  # 模板
result_template_2 = pd.read_excel('../03-数据文件/原始数据集/结果表2.xlsx')  # 模板

# 2. 数据预处理
daily_df['日期'] = pd.to_datetime(daily_df['日期'])
minute_df['日期'] = pd.to_datetime(minute_df['日期'])
minute_df['分钟起始'] = pd.to_timedelta(minute_df['分钟起始'].astype(str))

# 3. 定义函数：获取每条线路的时间分布向量（144个10分钟比例）
def get_time_distribution(line_code, df):
    df_line = df[df['线路编码'] == line_code]
    if df_line.empty:
        return None
    grouped = df_line.groupby(['日期'])
    ratio_matrix = []
    for date, group in grouped:
        group_sorted = group.sort_values(by='分钟起始')
        total = group_sorted['包裹量'].sum()
        if total > 0:
            ratio = group_sorted['包裹量'].values / total
            if len(ratio) == 144:
                ratio_matrix.append(ratio)
    if ratio_matrix:
        return np.mean(ratio_matrix, axis=0)
    else:
        return None

# 4. 定义函数：根据最近N天进行滑动预测
def predict_daily_quantity(line_code, df):
    df_line = df[df['线路编码'] == line_code].sort_values(by='日期')
    recent = df_line.tail(N_DAYS + 1)
    if len(recent) < N_DAYS + 1:
        return None
    values = recent['包裹量'].values
    moving_avg = np.mean(values[-N_DAYS:])
    trend = values[-1] - values[0]
    return moving_avg + ALPHA * trend

# 5. 执行预测主逻辑
results_1 = []
results_2 = []

for line_code in result_template_1['线路编码'].unique():
    predicted_qty = predict_daily_quantity(line_code, daily_df)
    if predicted_qty is None:
        continue

    results_1.append({
        '线路编码': line_code,
        '日期': PREDICT_DATE,
        '货量': round(predicted_qty)
    })

    r_vector = get_time_distribution(line_code, minute_df)

    if r_vector is None:
        origin = line_code.split(' - ')[0]
        similar_lines = [lc for lc in minute_df['线路编码'].unique() if lc.startswith(origin)]
        avg_vectors = [get_time_distribution(lc, minute_df) for lc in similar_lines]
        avg_vectors = [v for v in avg_vectors if v is not None]
        if avg_vectors:
            r_vector = np.mean(avg_vectors, axis=0)
        else:
            r_vector = np.ones(144) / 144  # 均匀分布兜底

    # 分解预测总量到每10分钟
    quantities_10min = predicted_qty * r_vector

    current_time = START_TIME
    for q in quantities_10min:
        results_2.append({
            '线路编码': line_code,
            '日期': current_time.strftime('%Y/%m/%d'),
            '分钟起始': current_time.strftime('%H:%M:%S'),
            '包裹量': round(q)
        })
        current_time += timedelta(minutes=10)

# 6. 保存结果表
result_df1 = pd.DataFrame(results_1)
result_df2 = pd.DataFrame(results_2)

result_df1.to_excel('../03-数据文件/结果表1_预测结果.xlsx', index=False)
result_df2.to_excel('../03-数据文件/结果表2_分钟预测.xlsx', index=False)
print("已保存：结果表1_预测结果.xlsx 和 结果表2_分钟预测.xlsx")
