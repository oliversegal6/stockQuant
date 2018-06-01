import matplotlib
matplotlib.use('Agg')
import tushare as ts
from matplotlib.pylab import date2num
import datetime
import matplotlib.pyplot as plt
import mpl_finance as mpf

wdyx = ts.get_k_data('300253','2018-01-01')
wdyx.info()

def date_to_num(dates):
    num_time = []
    for date in dates:
        date_time = datetime.datetime.strptime(date,'%Y-%m-%d')
        num_date = date2num(date_time)
        num_time.append(num_date)
    return num_time
# dataframe转换为二维数组
mat_wdyx = wdyx.as_matrix()
num_time = date_to_num(mat_wdyx[:,0])
mat_wdyx[:,0] = num_time
#         日期,   开盘,     收盘,    最高,      最低,   成交量,    代码
mat_wdyx[:3]
fig, ax = plt.subplots(figsize=(15,5))
fig.subplots_adjust(bottom=0.5)
mpf.candlestick_ochl(ax, mat_wdyx, width=1, colorup='g', colordown='r', alpha=1.0)
plt.grid(True)
# 设置日期刻度旋转的角度 
plt.xticks(rotation=30)
plt.title('wanda yuanxian 17')
plt.xlabel('Date')
plt.ylabel('Price')
# x轴的刻度为日期
ax.xaxis_date ()
###candlestick_ochl()函数的参数
# ax 绘图Axes的实例
# mat_wdyx 价格历史数据
# width    图像中红绿矩形的宽度,代表天数
# colorup  收盘价格大于开盘价格时的颜色
# colordown   低于开盘价格时矩形的颜色
# alpha      矩形的颜色的透明度
plt.show()
plt.savefig('result/kline.png')

fig, ax = plt.subplots(figsize=(15,5))
mpf.plot_day_summary_oclh(ax, mat_wdyx,colorup='g', colordown='r')
plt.grid(True)
ax.xaxis_date()
plt.title('wandayuanxian 17')
plt.ylabel('Price')
plt.show()
plt.savefig('result/open&closeprice.png')

fig, (ax1, ax2) = plt.subplots(2, sharex=True, figsize=(15,8))
mpf.candlestick_ochl(ax1, mat_wdyx, width=1.0, colorup = 'g', colordown = 'r')
ax1.set_title('wandayuanxian')
ax1.set_ylabel('Price')
ax1.grid(True)
ax1.xaxis_date()
plt.bar(mat_wdyx[:,0]-0.25, mat_wdyx[:,5], width= 0.5)
ax2.set_ylabel('Volume')
ax2.grid(True)
plt.show()
plt.savefig('result/kline&volumn.png')