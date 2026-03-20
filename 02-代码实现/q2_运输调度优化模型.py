import pandas as pd
import numpy as np
from math import ceil
from datetime import datetime, timedelta
from collections import defaultdict


df_lineinfo = pd.read_excel('./rawData/附件1.xlsx')
df_ownercount = pd.read_excel('./rawData/附件5.xlsx')
df_route = pd.read_excel('./rawData/附件4.xlsx')  # 可串点站点
df_day_pred = pd.read_excel('./processedData/结果表1_预测结果.xlsx')


# 构造可串点对
route_pairs = set()
for _, row in df_route.iterrows():
    route_pairs.add((row['站点编号1'], row['站点编号2']))
    route_pairs.add((row['站点编号2'], row['站点编号1']))  # 双向


# 格式化时间
df_lineinfo['发运时间'] = pd.to_datetime(df_lineinfo['发运节点'], format='%H:%M:%S').dt.time
df_lineinfo['线路日期'] = '2024/12/16'  # 或根据需求动态设置

# 加载预测结果（字段名：线路编码，日期，货量）
df_day_pred['日期'] = pd.to_datetime(df_day_pred['日期']).dt.strftime('%Y/%m/%d')

# 添加预测货量列
def get_predicted_volume(row):
    key = row['线路编码']
    date = row['线路日期']
    match = df_day_pred[(df_day_pred['线路编码'] == key) & (df_day_pred['日期'] == date)]
    if not match.empty:
        return match.iloc[0]['货量']
    return 0

df_lineinfo['预测包裹量'] = df_lineinfo.apply(get_predicted_volume, axis=1)


# 贪心策略（使用预测包裹量）
C = 500  # 每辆车容量

def can_chain(row_i, row_j):
    if row_i['起始场地'] != row_j['起始场地']:
        return False
    if row_i['车队编码'] != row_j['车队编码']:
        return False
    time_diff = abs(datetime.combine(datetime.today(), row_i['发运时间']) -
                    datetime.combine(datetime.today(), row_j['发运时间']))
    if time_diff.total_seconds() > 1800:
        return False
    if row_i['目的场地'] != row_j['目的场地'] and (row_i['目的场地'], row_j['目的场地']) not in route_pairs:
        return False
    return True

def greedy_chain(df):
    used = set()
    chains = []

    df_sorted = df.sort_values(by='发运时间')
    for i, row_i in df_sorted.iterrows():
        if row_i['线路编码'] in used:
            continue
        group = [row_i]
        used.add(row_i['线路编码'])
        current_load = row_i['预测包裹量']

        for j, row_j in df_sorted.iterrows():
            if row_j['线路编码'] in used or row_j['线路编码'] == row_i['线路编码']:
                continue
            if can_chain(row_i, row_j):
                if current_load + row_j['预测包裹量'] <= C:
                    group.append(row_j)
                    current_load += row_j['预测包裹量']
                    used.add(row_j['线路编码'])

        chains.append(group)
    return chains


# 调度逻辑
def assign_vehicles(chain_list, df_ownercount, C=500):
    vehicle_id = 0
    result = []

    owner_pool = {k: v for k, v in zip(df_ownercount['车队编码'], df_ownercount['自有车数量'])}
    chain_list.sort(key=lambda x: -len(x))

    for chain in chain_list:
        team = chain[0]['车队编码']
        total_load = sum(r['预测包裹量'] for r in chain)
        load_rate = total_load / C

        if owner_pool.get(team, 0) > 0:
            vehicle_type = '自有'
            owner_pool[team] -= 1
        else:
            vehicle_type = '外部'

        vehicle_id += 1
        for route in chain:
            result.append({
                '线路编码': route['线路编码'],
                '日期': '2023-12-15',
                '预计发运时间': route['发运时间'].strftime('%H:%M:%S'),
                '发运车辆': f'{vehicle_type}-V{vehicle_id}',
                '车辆类型': vehicle_type,
                '装载率': load_rate
            })
    return pd.DataFrame(result)


# 目标函数评估
def evaluate(df_result, df_lineinfo):
    merged = df_result.merge(df_lineinfo[['线路编码', '自有变动成本', '外部承运商成本']], on='线路编码')

    Z1 = sum(
        row['自有变动成本'] if '自有' in row['发运车辆'] else row['外部承运商成本']
        for _, row in merged.iterrows()
    )
    Z2 = df_result[df_result['车辆类型'] == '自有']['发运车辆'].nunique()
    Z3 = df_result['装载率'].mean()

    # 权重
    w1, w2, w3 = 0.6, 0.3, 0.1
    Z = w1 * Z1 - w2 * Z2 - w3 * Z3

    return {'Z': Z, 'Z1_总成本': Z1, 'Z2_自有车使用数': Z2, 'Z3_平均装载率': Z3}


# 主函数入口
def main():
    chains = greedy_chain(df_lineinfo)
    df_result = assign_vehicles(chains, df_ownercount)
    metrics = evaluate(df_result, df_lineinfo)

    df_result.to_excel("./processedData/调度结果表3.xlsx", index=False)
    print("指标评估：", metrics)

    df_specific = df_result[df_result['线路编码'].isin([
        '场地3 - 站点83 - 0600', '场地3 - 站点83 - 1400'
    ])]
    print("\n 特定线路调度明细：")
    print(df_specific)

main()
