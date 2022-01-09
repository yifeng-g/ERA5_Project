from multiprocessing import Pool
import netCDF4 as nc
import pandas as pd
import datetime
from tqdm import tqdm
import time
import os

def csv_make(year, longitude, latitude,):
    # 时间字符串转换为时间戳
    def time_reswitch(Y, M, D, h):
        ts = datetime.datetime.timestamp(datetime.datetime(Y, M, D, h, 0, 0))
        data_out = int((ts / 3600) + 613608 + 8)  # 1900年1月1日零时距离1970年1月1日零时有613608个小时,采用默认东八时区还原时间戳与格林尼治时间差8小时
        return data_out

    # 根据输入日期返回dataset中时间的序列范围
    def time_range(dataset, y, m, d, h):
        timestamp = time_reswitch(y, m, d, h)
        time = getdct((dataset['time']))[timestamp]
        return time

    # 摄氏度和开尔文温度转换，暂未使用
    def tem_switch(k):
        c = k - 273.15
        return c

    # 时间戳转换为时间字符串
    def time_switch(time_stamp):
        ts = (time_stamp - 613608) * 3600  # 1900年1月1日零时距离1970年1月1日零时有613608个小时
        date = datetime.datetime.utcfromtimestamp(ts)
        return date

    # 将dataset格式数据转化为字典
    def getdct(varibles):
        var_varibles = varibles
        dct_varibles = {}
        for i in range(0, len(var_varibles)):
            a = round(float(var_varibles[i]), 1)
            dct_varibles[a] = i
        return dct_varibles

    #获取指定变量在指定时间，经纬度的值
    def get_attr(dataset, attr, time):
        y = dataset[str(attr)][time, latitude_value, longitude_value]
        return (y)

    # 为dataset格式数据中的指定变量创建索引字典
    def getdct(varibles):
        var_varibles = varibles
        dct_varibles = {}
        for i in range(0, len(var_varibles)):
            a = round(float(var_varibles[i]), 1)
            dct_varibles[a] = i
        return dct_varibles


    #导入cn文件
    dataset1 = nc.Dataset(str(data_address) + str(year) + '_1' + '.nc')
    dataset2 = nc.Dataset(str(data_address) + str(year) + '_2' + '.nc')
    dataset3 = nc.Dataset(str(data_address) + str(year) + '_3' + '.nc')
    dataset4 = nc.Dataset(str(data_address) + str(year) + '_4' + '.nc')
    df = pd.DataFrame()

    #设定时间范围
    time_start = time_range(dataset1, year, 1, 1, 0)
    time_end = time_range(dataset1, year, 12, 31, 23)

    #制作longitude/latitude对应的索引
    longitude_value = (getdct(dataset1['longitude']))[longitude]
    latitude_value = (getdct(dataset1['latitude']))[latitude]

    #选取需要使用的变量从nc文件取出，放入df
    for ts in tqdm(range(time_start, time_end + 1)):
        a = {'longitude': longitude,
             'latitude': latitude,
             'time': time_switch(dataset1['time'][ts]),
             'd2m': get_attr(dataset1, 'd2m', ts),
             't2m': get_attr(dataset1, 't2m', ts),
             'skt': get_attr(dataset1, 'skt', ts),
             'stl1': get_attr(dataset1, 'stl1', ts),
             'stl2': get_attr(dataset1, 'stl2', ts),
             'stl3': get_attr(dataset1, 'stl3', ts),
             'stl4': get_attr(dataset1, 'stl4', ts),
             'asn': get_attr(dataset1, 'asn', ts),
             'snowc': get_attr(dataset1, 'snowc', ts),
             'rsn': get_attr(dataset1, 'rsn', ts),
             'sde': get_attr(dataset1, 'sde', ts),
             'fal': get_attr(dataset2, 'fal', ts),
             'src': get_attr(dataset2, 'src', ts),
             'sd': get_attr(dataset2, 'sd', ts),
             'smlt': get_attr(dataset2, 'smlt', ts),
             'slhf': get_attr(dataset2, 'slhf', ts),
             'tsn': get_attr(dataset2, 'tsn', ts),
             'swvl1': get_attr(dataset2, 'swvl1', ts),
             'swvl2': get_attr(dataset2, 'swvl2', ts),
             'swvl3': get_attr(dataset2, 'swvl3', ts),
             'swvl4': get_attr(dataset2, 'swvl4', ts),
             'evabs': get_attr(dataset3, 'evabs', ts),
             'evaow': get_attr(dataset3, 'evaow', ts),
             'evatc': get_attr(dataset3, 'evatc', ts),
             'evavt': get_attr(dataset3, 'evavt', ts),
             'pev': get_attr(dataset3, 'pev', ts),
             'ro': get_attr(dataset3, 'ro', ts),
             'ssr': get_attr(dataset3, 'ssr', ts),
             'str': get_attr(dataset3, 'str', ts),
             'sshf': get_attr(dataset3, 'sshf', ts),
             'ssrd': get_attr(dataset3, 'ssrd', ts),
             'strd': get_attr(dataset3, 'strd', ts),
             'u10': get_attr(dataset4, 'u10', ts),
             'v10': get_attr(dataset4, 'v10', ts),
             'lai_hv': get_attr(dataset4, 'lai_hv', ts),
             'lai_lv': get_attr(dataset4, 'lai_lv', ts),
             'es': get_attr(dataset4, 'es', ts),
             'ssro': get_attr(dataset4, 'ssro', ts),
             'sp': get_attr(dataset4, 'sp', ts),
             'sro': get_attr(dataset4, 'sro', ts),
             'e': get_attr(dataset4, 'e', ts),
             'tp': get_attr(dataset4, 'tp', ts)}
        df = df.append(a, ignore_index=True)
        del(a)
    df.to_csv('data_demo.csv',
              index_label="index_label")
    print('processing_compeleted')


if __name__ == '__main__':
    data_address = 'F:/New folder/'
    longitude = 100.5
    latitude = 30.5
    year = 2006
    csv_make(year, longitude, latitude,)
