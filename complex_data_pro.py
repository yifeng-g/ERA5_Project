from multiprocessing import Pool
import netCDF4 as nc
import pandas as pd
import datetime
import numpy as np
from tqdm import tqdm
import time
import gc
import os

def main(i):
    def csv_make(year, city, longitude, latitude, ):
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

        # 摄氏度和开尔文温度转换
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
                gc.disable()
                a = round(float(var_varibles[i]), 1)
                dct_varibles[a] = i
                gc.enable()
            return dct_varibles


        dataset1 = nc.Dataset('F:/era5/' + str(year) + '_1' + '.nc')
        dataset2 = nc.Dataset('F:/era5/' + str(year) + '_2' + '.nc')
        dataset3 = nc.Dataset('F:/era5/' + str(year) + '_3' + '.nc')
        dataset4 = nc.Dataset('F:/era5/' + str(year) + '_4' + '.nc')
        longitude_value = (getdct(dataset1['longitude']))[longitude]
        latitude_value = (getdct(dataset1['latitude']))[latitude]

        time_start = time_range(dataset1, year, 1, 1, 0)
        time_end = time_range(dataset1, year, 1, 1, 23)
        time_list = list(range(time_start, time_end + 1))
        b = [[0]*(time_end - time_start+1) for _ in range(55)]
        print(b)
        # dic = {}
        # for i in time_range:
        #     dic[i] = [longitude,
        #                 latitude,
        #                 time_switch(dataset1['time'][ts]),
        #                 get_attr(dataset1, 'd2m', ts),
        #                 get_attr(dataset1, 't2m', ts),]
        # dict_all = [[0] for _ in range(time_end - time_start)]

        # for i in range (time_end - time_start):
        #     for t in time_list:
        #         a = dic[t]
        #         dict_all[t] = a
        name = ['longitude', 'latitude','time','d2m','t2m']
        # df_all = pd.DataFrame(columns=name, data=dict_all)
        # df_all.to_csv('D:/python/pythonProject1/era5/1.csv')
        # b=pd.DataFrame()
        # print(str(year) + 'kaishi')


        # for ts in tqdm(range(time_start, time_end + 1)):
        #     # print(str(year) + 'xunhuan' +'kaishi')
        #     # gc.disable()
        #     a={'longitude': longitude,
        #          'latitude': latitude,
        #          'time': time_switch(dataset1['time'][ts]),
        #          'd2m': get_attr(dataset1, 'd2m', ts),
        #          't2m': get_attr(dataset1, 't2m', ts),
        #          'skt': get_attr(dataset1, 'skt', ts),
        #          'asn': get_attr(dataset1, 'asn', ts),
        #          'snowc': get_attr(dataset1, 'snowc', ts),
        #          'rsn': get_attr(dataset1, 'rsn', ts),
        #          'sde': get_attr(dataset1, 'sde', ts),
        #          'stl1': get_attr(dataset1, 'stl1', ts),
        #          'stl2': get_attr(dataset1, 'stl2', ts),
        #          'stl3': get_attr(dataset1, 'stl3', ts),
        #          'stl4': get_attr(dataset1, 'stl4', ts),
        #          'fal': get_attr(dataset2, 'fal', ts),
        #          'src': get_attr(dataset2, 'src', ts),
        #          'sd': get_attr(dataset2, 'sd', ts),
        #          'smlt': get_attr(dataset2, 'smlt', ts),
        #          'slhf': get_attr(dataset2, 'slhf', ts),
        #          'tsn': get_attr(dataset2, 'tsn', ts),
        #          'swvl1': get_attr(dataset2, 'swvl1', ts),
        #          'swvl2': get_attr(dataset2, 'swvl2', ts),
        #          'swvl3': get_attr(dataset2, 'swvl3', ts),
        #          'swvl4': get_attr(dataset2, 'swvl4', ts),
        #          'evabs': get_attr(dataset3, 'evabs', ts),
        #          'evaow': get_attr(dataset3, 'evaow', ts),
        #          'evatc': get_attr(dataset3, 'evatc', ts),
        #          'evavt': get_attr(dataset3, 'evavt', ts),
        #          'pev': get_attr(dataset3, 'pev', ts),
        #          'ro': get_attr(dataset3, 'ro', ts),
        #          'ssr': get_attr(dataset3, 'ssr', ts),
        #          'str': get_attr(dataset3, 'str', ts),
        #          'sshf': get_attr(dataset3, 'sshf', ts),
        #          'ssrd': get_attr(dataset3, 'ssrd', ts),
        #          'strd': get_attr(dataset3, 'strd', ts),
        #          'u10': get_attr(dataset4, 'u10', ts),
        #          'v10': get_attr(dataset4, 'v10', ts),
        #          'lai_hv': get_attr(dataset4, 'lai_hv', ts),
        #          'lai_lv': get_attr(dataset4, 'lai_lv', ts),
        #          'es': get_attr(dataset4, 'es', ts),
        #          'ssro': get_attr(dataset4, 'ssro', ts),
        #          'sp': get_attr(dataset4, 'sp', ts),
        #          'sro': get_attr(dataset4, 'sro', ts),
        #          'e': get_attr(dataset4, 'e', ts),
        #          'tp': get_attr(dataset4, 'tp', ts)}
        #     b = pd.concat([b, a], axis=0, ignore_index=True)
            # print(type(get_attr(dataset1, 't2m', ts)))
            # d2m_list.extend(a)
            # t2m_list.extend(n)
            # t2m_list.extend(get_attr(dataset1, 't2m', ts))
            # print(b)
            #
            # # print(a)
            # # print(str(year) + 'xunhuan' + 'zhong
            # df = df.append(a, ignore_index=True)
            # gc.enable()
            # print(str(year) + 'xunhuan' + 'jieshu')
            # time.sleep(0.01)
            # return df
        # df.to_csv('F:/era5/' + str(city) + '_' + str(year) + '_' + str(longitude) + '-' + str(latitude) + '.csv',
        #           index_label="index_label")
        # print(str(city) + str(year) + 'compelete')
    local_df = pd.DataFrame(pd.read_csv('localnew.csv'))
    city = local_df['city'][i]
    longitude = local_df['longitude'][i]
    latitude = local_df['latitude'][i]


    for year in range(2008, 2013):
        csv_make(year, city, longitude, latitude,)
    print(city, longitude, latitude)



if __name__ == '__main__':
    for i in range(0, 32):
         print(i)
         main(i)
    # p = Pool(6)
    # for i in range(0, 32):
    #     p.apply_async(main, args=(i,))
    #     print(str(i)+'over')
    # p.close()
    # p.join()
    # help(p.apply_async(i))
    # print('Waiting for all subprocesses done...')
    # p.close()
    # p.join()
    # print('All subprocesses done.')