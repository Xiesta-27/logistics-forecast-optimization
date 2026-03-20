import pandas as pd
import numpy as np
from math import ceil
from datetime import datetime, timedelta

# 读取数据
df_lineinfo = pd.read_excel('./rawData/附件1.xlsx')
df_ownercount = pd.read_excel('./rawData/附件5.xlsx')
df_route = pd.read_excel('./rawData/附件4.xlsx')
df_pred = pd.read_excel('./processedData/结果表1_预测结果.xlsx')

# 预处理
df_lineinfo['发运时间'] = pd.to_datetime(df_lineinfo['发运节点'], format='%H:%M:%S')
df_lineinfo['线路日期'] = '2024/12/16'
df_pred['日期'] = pd.to_datetime(df_pred['日期']).dt.strftime('%Y/%m/%d')
df_lineinfo['线路编码'] = df_lineinfo['线路编码'].str.strip()
df_pred['线路编码'] = df_pred['线路编码'].str.strip()

# 添加预测货量
def get_predicted_volume(row):
    match = df_pred[(df_pred['线路编码'] == row['线路编码']) & (df_pred['日期'] == row['线路日期'])]
    return match['货量'].values[0] if not match.empty else 0
df_lineinfo['预测货量'] = df_lineinfo.apply(get_predicted_volume, axis=1)

# 可串点
route_pairs = set()
for _, row in df_route.iterrows():
    route_pairs.add((row['站点编号1'], row['站点编号2']))
    route_pairs.add((row['站点编号2'], row['站点编号1']))

# 容器决策
C_0 = 1000  # 非容器容量
C_1 = 800   # 容器容量

def container_decision(row):
    load = row['预测货量']
    time = row['发运时间'].hour
    if load <= C_1:
        return 1
    if row['车队编码'].startswith("Z") or time <= 6:
        return 1
    return 0

df_lineinfo['使用容器'] = df_lineinfo.apply(container_decision, axis=1)
df_lineinfo['单车容量'] = df_lineinfo['使用容器'].map(lambda x: C_1 if x == 1 else C_0)
df_lineinfo['车辆需求'] = df_lineinfo.apply(lambda r: ceil(r['预测货量'] / r['单车容量']), axis=1)

# 串点逻辑
def can_chain(row_i, row_j):
    if row_i['起始场地'] != row_j['起始场地']:
        return False
    if row_i['车队编码'] != row_j['车队编码']:
        return False
    if abs((row_i['发运时间'] - row_j['发运时间']).total_seconds()) > 1800:
        return False
    if row_i['目的场地'] != row_j['目的场地'] and (row_i['目的场地'], row_j['目的场地']) not in route_pairs:
        return False
    if row_i['使用容器'] != row_j['使用容器']:
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
        total_volume = row_i['预测货量']
        capacity = row_i['单车容量']
        used.add(row_i['线路编码'])
        for j, row_j in df_sorted.iterrows():
            if row_j['线路编码'] in used or row_j['线路编码'] == row_i['线路编码']:
                continue
            if can_chain(row_i, row_j):
                if total_volume + row_j['预测货量'] <= capacity:
                    group.append(row_j)
                    total_volume += row_j['预测货量']
                    used.add(row_j['线路编码'])
        chains.append(group)
    return chains

# 分配车辆
def assign_vehicles(chain_list, df_ownercount):
    vehicle_id = 0
    result = []

    owner_pool = {k: v for k, v in zip(df_ownercount['车队编码'], df_ownercount['自有车数量'])}
    chain_list.sort(key=lambda x: -len(x))

    for chain in chain_list:
        team = chain[0]['车队编码']
        container_flag = chain[0]['使用容器']
        total_volume = sum(r['预测货量'] for r in chain)
        capacity = chain[0]['单车容量']
        load_rate = total_volume / capacity

        if owner_pool.get(team, 0) > 0:
            vehicle_type = '自有'
            owner_pool[team] -= 1
        else:
            vehicle_type = '外部'

        vehicle_id += 1
        for route in chain:
            result.append({
                '线路编码': route['线路编码'],
                '日期': route['线路日期'],
                '预计发运时间': route['发运时间'].strftime('%H:%M:%S'),
                '是否使用容器': '是' if container_flag else '否',
                '发运车辆': f'{vehicle_type}-V{vehicle_id}'
            })
    return pd.DataFrame(result)

# 主程序
def main():
    chains = greedy_chain(df_lineinfo)
    df_result = assign_vehicles(chains, df_ownercount)
    df_result.to_excel('./processedData/结果表4.xlsx', index=False)
    print("问题三调度结果已写入：结果表4.xlsx")

    # 输出指定线路
    targets = ['场地3 - 站点83 - 0600', '场地3 - 站点83 - 1400']
    df_specific = df_result[df_result['线路编码'].isin(targets)]
    print("\n指定线路调度结果：")
    print(df_specific)

main()
