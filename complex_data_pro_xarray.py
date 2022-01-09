from multiprocessing import Pool
import netCDF4 as nc
import xarray as xr
import pandas as pd
import datetime
import numpy as np
from tqdm import tqdm
import time
import gc
import os
def main(i):
    def csv_make(year, city, lon, lat):
        def time_switch(time_stamp):
            ts = (time_stamp - 613608) * 3600  # 1900年1月1日零时距离1970年1月1日零时有613608个小时
            date = datetime.datetime.utcfromtimestamp(ts)
            return date

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


        def get_attr(dataset,attr):
            y = dataset[str(attr)].sel(time=slice(start_date, end_date), longitude=lon, latitude=lat).values
            # print(str(attr)+'over')
            return y
        def get_time(dataset,attr):
            y = dataset[str(attr)].sel(time=slice(start_date, end_date)).values
            return y

        dataset1 = xr.open_dataset('F:/era5/' + str(year) + '_1' + '.nc')
        dataset2 = xr.open_dataset('F:/era5/' + str(year) + '_2' + '.nc')
        dataset3 = xr.open_dataset('F:/era5/' + str(year) + '_3' + '.nc')
        dataset4 = xr.open_dataset('F:/era5/' + str(year) + '_4' + '.nc')

        # dataset11 = nc.Dataset('F:/era5/' + str(year) + '_1' + '.nc')
        # dataset2 = nc.Dataset('F:/era5/' + str(year) + '_2' + '.nc')
        # dataset3 = nc.Dataset('F:/era5/' + str(year) + '_3' + '.nc')
        # dataset4 = nc.Dataset('F:/era5/' + str(year) + '_4' + '.nc')


        start_date = str(year) + "-01-01-00"
        end_date = str(year) + "-12-31-23"


        b = [[0] * (len(get_time(dataset1,'time'))) for _ in range(5)]
        print(b)
        # num =0
        allname = ['longitude',
                   'latitude',
             'time',
             'd2m',
             't2m',
             'skt',
             'asn',
             'snowc',
             'rsn',
             'sde',
             'stl1',
             'stl2',
             'stl3',
             'stl4',
             'fal',
             'src',
             'sd',
             'smlt',
             'slhf',
             'tsn',
             'swvl1',
             'swvl2',
             'swvl3',
             'swvl4',
             'evabs',
             'evaow',
             'evatc',
             'evavt',
             'pev',
             'ro',
             'ssr',
             'str',
             'sshf',
             'ssrd',
             'strd',
             'u10',
             'v10',
             'lai_hv',
             'lai_lv',
             'es',
             'ssro',
             'sp',
             'sro',
             'e',
             'tp']
        b[0] = get_time(dataset1,'time')
        b[1] = [lon]* len(get_time(dataset1,'time'))
        b[2] = [lat]* len(get_time(dataset1,'time'))
        # b[3] = get_attr(dataset1, str(allname[3]))
        # b[4] = get_attr(dataset1, str(allname[4]))
        b[5] = get_attr(dataset1, str(allname[5]))
        # b[6] = get_attr(dataset1, str(allname[6]))
        # b[7] = get_attr(dataset1, str(allname[7]))
        # b[8] = get_attr(dataset1, str(allname[8]))
        # b[9] = get_attr(dataset1, str(allname[9]))
        # b[10] = get_attr(dataset1, str(allname[10]))
        # print(10)
        # b[11] = get_attr(dataset1, str(allname[11]))
        # b[12] = get_attr(dataset1, str(allname[12]))
        # b[13] = get_attr(dataset1, str(allname[12]))
        # b[14] = get_attr(dataset1, str(allname[14]))
        # b[15] = get_attr(dataset1, str(allname[15]))
        # b[16] = get_attr(dataset1, str(allname[16]))
        # b[17] = get_attr(dataset1, str(allname[17]))
        # b[18] = get_attr(dataset1, str(allname[18]))
        # b[19] = get_attr(dataset1, str(allname[19]))
        # b[20] = get_attr(dataset1, str(allname[20]))
        # print(20)
        # b[21] = get_attr(dataset1, str(allname[21]))
        # b[22] = get_attr(dataset1, str(allname[22]))
        # b[23] = get_attr(dataset1, str(allname[23]))
        # b[24] = get_attr(dataset1, str(allname[24]))
        # b[25] = get_attr(dataset1, str(allname[25]))
        # b[26] = get_attr(dataset1, str(allname[26]))
        # b[27] = get_attr(dataset1, str(allname[27]))
        # b[28] = get_attr(dataset1, str(allname[28]))
        # b[29] = get_attr(dataset1, str(allname[29]))
        # b[30] = get_attr(dataset1, str(allname[30]))
        # print(30)
        # b[31] = get_attr(dataset1, str(allname[31]))
        # b[32] = get_attr(dataset1, str(allname[32]))
        # b[33] = get_attr(dataset1, str(allname[33]))
        # b[34] = get_attr(dataset1, str(allname[34]))
        # b[35] = get_attr(dataset1, str(allname[35]))
        # b[36] = get_attr(dataset1, str(allname[36]))
        # b[37] = get_attr(dataset1, str(allname[37]))
        # b[38] = get_attr(dataset1, str(allname[38]))
        # b[39] = get_attr(dataset1, str(allname[39]))
        # b[40] = get_attr(dataset1, str(allname[40]))
        # b[41] = get_attr(dataset1, str(allname[41]))
        # b[42] = get_attr(dataset1, str(allname[42]))
        # b[43] = get_attr(dataset1, str(allname[43]))
        # b[44] = get_attr(dataset1, str(allname[44]))
        b[45] = get_attr(dataset1, str(allname[45]))
        # print(45)



        # for name in tqdm(allname):
        #     b[num]=get_attr(dataset1, str(name))
        #     num = num + 1
        #     print(num)


        # print(b)
        # print(b)
        # c = get_attr(dataset1, 'd2m',)
        # # print(type(c))
        # # print(c)
        # b[0]=c
        # print(type(c))
        #
        # print(b)
        # # print(b)

        # a = {'longitude': lon,
        #      'latitude': lat,
        #      'time': get_time(dataset1,'time'),
        #      'd2m': get_attr(dataset1, 'd2m'),
        #      't2m': get_attr(dataset1, 't2m'),b
        #      'asn': get_attr(dataset1, 'asn'),
        #      'snowc': get_attr(dataset1, 'snowc'),
        #      'rsn': get_attr(dataset1, 'rsn'),
        #      'sde': get_attr(dataset1, 'sde'),
        #      'stl1': get_attr(dataset1, 'stl1'),
        #      'stl2': get_attr(dataset1, 'stl2'),
        #      'stl3': get_attr(dataset1, 'stl3'),
        #      'stl4': get_attr(dataset1, 'stl4'),
        #      'fal': get_attr(dataset2, 'fal'),
        #      'src': get_attr(dataset2, 'src'),
        #      'sd': get_attr(dataset2, 'sd'),
        #      'smlt': get_attr(dataset2, 'smlt'),
        #      'slhf': get_attr(dataset2, 'slhf'),
        #      'tsn': get_attr(dataset2, 'tsn'),
        #      'swvl1': get_attr(dataset2, 'swvl1'),
        #      'swvl2': get_attr(dataset2, 'swvl2'),
        #      'swvl3': get_attr(dataset2, 'swvl3'),
        #      'swvl4': get_attr(dataset2, 'swvl4'),
        #      'evabs': get_attr(dataset3, 'evabs'),
        #      'evaow': get_attr(dataset3, 'evaow'),
        #      'evatc': get_attr(dataset3, 'evatc'),
        #      'evavt': get_attr(dataset3, 'evavt'),
        #      'pev': get_attr(dataset3, 'pev'),
        #      'ro': get_attr(dataset3, 'ro'),
        #      'ssr': get_attr(dataset3, 'ssr'),
        #      'str': get_attr(dataset3, 'str'),
        #      'sshf': get_attr(dataset3, 'sshf'),
        #      'ssrd': get_attr(dataset3, 'ssrd'),
        #      'strd': get_attr(dataset3, 'strd'),
        #      'u10': get_attr(dataset4, 'u10'),
        #      'v10': get_attr(dataset4, 'v10'),
        #      'lai_hv': get_attr(dataset4, 'lai_hv'),
        #      'lai_lv': get_attr(dataset4, 'lai_lv'),
        #      'es': get_attr(dataset4, 'es'),
        #      'ssro': get_attr(dataset4, 'ssro'),
        #      'sp': get_attr(dataset4, 'sp'),
        #      'sro': get_attr(dataset4, 'sro'),
        #      'e': get_attr(dataset4, 'e'),
        #      'tp': get_attr(dataset4, 'tp')}
        df_all = pd.DataFrame(b,index=allname)
        df2 = pd.DataFrame(df_all.values.T, index=df_all.columns, columns=df_all.index)
        print(df2)
        df2.to_csv('1.csv',)

    local_df = pd.DataFrame(pd.read_csv('localnew1.csv'))

    city = local_df['city'][i]
    lon = local_df['longitude'][i]
    lat = local_df['latitude'][i]
    print(str(city) + 'compelete')

    # for year in tqdm(range(2008,2013)):
    year = 2011
    csv_make(year, city, lon, lat,)


if __name__ == '__main__':
    main(1)

    # p = Pool(32)
    # for i in range(0, 32):
    #     # main(i)
    #     p.apply_async(main, args=(i,))
    # p.close()
    # p.join()
