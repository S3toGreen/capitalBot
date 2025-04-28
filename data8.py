import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import webbrowser
import os

# 載入資料
DATA3 = pd.read_csv("txfp.csv")
DATA3['Time'] = pd.to_datetime(DATA3['Time'])

# 建立基本欄位
DATA3['Delta'] = DATA3['aggBuy'] - DATA3['aggSell']
DATA3['Volume'] = DATA3['aggBuy'] + DATA3['aggSell']
DATA3['Minute'] = DATA3['Time'].dt.floor('min')

# 建立價格區間格線（控制解析度）
price_tick = 10
DATA3['Price_bin'] = (DATA3['Price'] // price_tick * price_tick).astype(int)

# 限縮時間範圍（近 10 分鐘）
time_bins = DATA3['Minute'].sort_values().unique()[-10:]
DATA3 = DATA3[DATA3['Minute'].isin(time_bins)]

# 建立 Bid / Ask Volume 分布表格
volmap = DATA3.groupby(['Minute', 'Price_bin'])[['aggBuy', 'aggSell']].sum().reset_index()
volume_profile_bid = DATA3.groupby('Price_bin')['aggBuy'].sum()
volume_profile_ask = DATA3.groupby('Price_bin')['aggSell'].sum()
volume_profile = volume_profile_bid + volume_profile_ask
volume_profile = volume_profile.reindex(sorted(DATA3['Price_bin'].unique(), reverse=True)).fillna(0)
poc_price = volume_profile.idxmax()

# 建立 Delta 分布表格與 Volume、VWAP、CVD
minute_groups = DATA3.groupby('Minute')
delta_map = minute_groups['Delta'].sum()
volume_map = minute_groups['Volume'].sum()
vwap_map = minute_groups.apply(lambda x: np.sum(x['Price'] * x['Volume']) / np.sum(x['Volume']))

# VWAP 平滑線
vwap_line = pd.Series([vwap_map[t] for t in sorted(vwap_map.index)], index=sorted(vwap_map.index))

# 建立時間價格座標軸
times = sorted(volmap['Minute'].unique())
prices = sorted(volmap['Price_bin'].unique(), reverse=True)

# 建立 subplot 架構
cols = len(times) + 1
fig = make_subplots(
    rows=3, cols=cols, shared_yaxes=True, vertical_spacing=0.02, horizontal_spacing=0.005,
    specs=[[{"type": "bar"}] * cols] * 2 + [[{"type": "scatter"}] * cols],
    subplot_titles=['🕒 ' + str(t)[11:16] for t in times] + ['VolProf'] + [''] * cols * 2
)

# 每分鐘建立 Bid / Ask 條狀圖
for idx, t in enumerate(times):
    df_t = volmap[volmap['Minute'] == t].set_index('Price_bin').reindex(prices).fillna(0)
    bids = df_t['aggBuy'].values * -1
    asks = df_t['aggSell'].values
    bid_labels = [str(int(v)) if v > 0 else "" for v in df_t['aggBuy'].values]
    ask_labels = [str(int(v)) if v > 0 else "" for v in df_t['aggSell'].values]

    max_vol = max(df_t['aggBuy'].max(), df_t['aggSell'].max(), 1)
    cap_ratio = 0.85
    bid_colors = ['rgba(0,150,255,{:.2f})'.format(min(v / max_vol, cap_ratio)) for v in df_t['aggBuy'].values]
    ask_colors = ['rgba(255,100,100,{:.2f})'.format(min(v / max_vol, cap_ratio)) for v in df_t['aggSell'].values]

    fig.add_trace(go.Bar(
        x=bids, y=prices, orientation='h', name='Bid',
        marker=dict(color=bid_colors, line=dict(color='rgba(0,150,255,1)', width=1)),
        text=bid_labels, textposition='auto', insidetextanchor='start',
        textfont=dict(size=10, color='white'), hovertemplate='價格: %{y}<br>Bid: %{text}<extra></extra>',
        showlegend=False
    ), row=1, col=idx+1)

    fig.add_trace(go.Bar(
        x=asks, y=prices, orientation='h', name='Ask',
        marker=dict(color=ask_colors, line=dict(color='rgba(255,100,100,1)', width=1)),
        text=ask_labels, textposition='auto', insidetextanchor='end',
        textfont=dict(size=10, color='white'), hovertemplate='價格: %{y}<br>Ask: %{text}<extra></extra>',
        showlegend=False
    ), row=1, col=idx+1)

    fig.add_trace(go.Bar(
        x=[str(t)[11:16]], y=[delta_map[t]],
        marker_color='green' if delta_map[t] >= 0 else 'red',
        text=str(delta_map[t]), textposition='inside',
        textfont=dict(size=11, color='white'), showlegend=False
    ), row=2, col=idx+1)

    fig.add_trace(go.Bar(
        x=[str(t)[11:16]], y=[volume_map[t]],
        marker_color='rgba(100,200,255,0.8)',
        text=str(volume_map[t]), textposition='inside',
        textfont=dict(size=11, color='white'), showlegend=False
    ), row=3, col=idx+1)

    for r in range(1, 4):
        fig.update_xaxes(showticklabels=False, showgrid=False, zeroline=False, row=r, col=idx+1)

# Volume Profile 雙色堆疊圖（右側）
vp_bids = volume_profile_bid.reindex(prices).fillna(0)
vp_asks = volume_profile_ask.reindex(prices).fillna(0)

fig.add_trace(go.Bar(
    x=-vp_bids.values, y=vp_bids.index,
    orientation='h', marker=dict(color='rgba(0,150,255,0.4)')
), row=1, col=cols)

fig.add_trace(go.Bar(
    x=vp_asks.values, y=vp_asks.index,
    orientation='h', marker=dict(color='rgba(255,100,100,0.4)')
), row=1, col=cols)

# 加入 POC 高亮區（擴及所有欄位）
fig.add_shape(
    type="line", x0=0, x1=1.005, y0=poc_price, y1=poc_price, xref='paper', yref='y1',
    line=dict(color="yellow", width=2, dash="dot")
)
fig.add_annotation(
    x=1.007, y=poc_price, xref='paper', yref='y1',
    text="<b>POC</b>", showarrow=False, font=dict(color="yellow", size=11)
)

# 加上 VWAP 平滑線
fig.add_trace(go.Scatter(
    x=[str(t)[11:16] for t in times],
    y=[vwap_map[t] for t in times],
    mode='lines',
    line=dict(width=2.5, color='lime'),
    name='VWAP Line', showlegend=False,
    xaxis='x', yaxis='y3'
), row=3, col=cols-1)

fig.update_xaxes(range=[-vp_bids.max()*1.1, vp_asks.max()*1.1], row=1, col=cols)
for r in range(1, 4):
    fig.update_xaxes(showticklabels=False, showgrid=False, zeroline=False, row=r, col=cols)

fig.add_annotation(
    text="VolProf", xref="paper", yref="paper",
    x=1.0, y=1.05, showarrow=False,
    font=dict(size=14, color="white"), align="right"
)

fig.add_annotation(
    text="<b>Delta (Buy - Sell)</b>", xref="paper", yref="paper",
    x=0, y=0.67, showarrow=False,
    font=dict(size=12, color="lightgray"), align="left"
)

fig.update_layout(
    title='📊 Orderflow 每根K棒 Bid / Ask 成交視覺圖（分離柱狀 + VolProf）',
    barmode='relative', template='plotly_dark', height=720,
    plot_bgcolor='black', paper_bgcolor='black', bargap=0.1,
    showlegend=False, margin=dict(t=70, l=40, r=40, b=60),
)

fig.update_yaxes(
    title='價格', autorange='reversed',
    showgrid=True, gridcolor='rgba(255,255,255,0.05)',
    tickmode='array', tickvals=prices, ticks="outside",
    row=1, col=1
)

fig.add_hline(y=0, line_color='gray', row=2, col="all")
fig.add_hline(y=0, line_color='gray', row=3, col="all")

for y in range(-400, 401, 200):
    fig.add_hline(y=y, line_color='rgba(255,255,255,0.03)', line_dash='dot', row=2, col=1)
for y in range(0, int(volume_map.max()*1.2), 1000):
    fig.add_hline(y=y, line_color='rgba(255,255,255,0.03)', line_dash='dot', row=3, col=1)

output_file = "output_orderflow_bar_split.html"
fig.write_html(output_file)
webbrowser.open('file://' + os.path.realpath(output_file))
