import netCDF4 as nc
from netCDF4 import Dataset, num2date
import pandas as pd
import datetime
import numpy as np
import time

def main():
    def time_switch(time_stamp):
        ts = list((time_stamp - 613608) * 3600)  # 1900年1月1日 零时距离1970年1月1日零时有613608个小时
        for d in range(len(ts)):
            ts[d] = datetime.datetime.utcfromtimestamp(ts[d])
        return ts

    def getdct(varibles):
        var_varibles = varibles
        dct_varibles = {}
        for i in range(0, len(var_varibles)):
            a = round(float(var_varibles[i]), 1)
            dct_varibles[a] = i
        return dct_varibles

    for year in range (2013,2017):

        dataset1 = nc.Dataset('D:/era5data/1/' + str(year) +'_1.nc')
        dataset2 = nc.Dataset('D:/era5data/4/' + str(year) +'_4.nc')

        time_array = time_switch(np.asarray(dataset1['time'][:]))
        dic_lon = getdct(dataset1['longitude'])
        dic_lat = getdct(dataset1['latitude'])

        local_df = pd.DataFrame(pd.read_csv('finall1.csv'))

        for i in range(len(local_df)):
            tic1 = time.perf_counter()
            city = local_df['xian'][i]
            lon = np.around(local_df['lon'][i], 1)
            lat = np.around(local_df['lat'][i], 1)
            longitude_value = dic_lon[lon]
            latitude_value = dic_lat[lat]

            b = [[0] * (len(time_array)) for _ in range(6)]
            b[0] = time_array
            b[1] = [city] * (len(time_array))
            b[2] = [lat] * (len(time_array))
            b[3] = [lon] * (len(time_array))
            b[4] = dataset1['t2m'][:,latitude_value,longitude_value]
            b[5] = dataset2['tp'][:,latitude_value,longitude_value]

            df_all = pd.DataFrame(b)
            df2 = pd.DataFrame(df_all.values.T, index=df_all.columns, columns=['time','county','lat','lon','t2m','tp'])
            df2.to_csv('data/'+ str(city)+str(year)+'.csv', index=None)
            toc1 = time.perf_counter()
            shijian1 = toc1 - tic1
            print('第'+str(i)+'ci' + str(shijian1))
            del(b,df2,df_all)

    del (dataset1,dataset2)

if __name__ == '__main__':
    main()



###缩写
 # 'd2m': get_attr(dataset1, 'd2m'),
 #         't2m': get_attr(dataset1, 't2m'),
         # 'skt': get_attr(dataset1, 'skt'),
         # 'asn': get_attr(dataset1, 'asn'),
         # 'snowc': get_attr(dataset1, 'snowc'),
         # 'rsn': get_attr(dataset1, 'rsn'),
         # 'sde': get_attr(dataset1, 'sde'),
         # 'stl1': get_attr(dataset1, 'stl1'),
         # 'stl2': get_attr(dataset1, 'stl2'),
         # 'stl3': get_attr(dataset1, 'stl3'),
         # 'stl4': get_attr(dataset1, 'stl4'),
         # 'fal': get_attr(dataset2, 'fal'),
         # 'src': get_attr(dataset2, 'src'),
         # 'sd': get_attr(dataset2, 'sd'),
         # 'smlt': get_attr(dataset2, 'smlt'),
         # 'slhf': get_attr(dataset2, 'slhf'),
         # 'tsn': get_attr(dataset2, 'tsn'),
         # 'swvl1': get_attr(dataset2, 'swvl1'),
         # 'swvl2': get_attr(dataset2, 'swvl2'),
         # 'swvl3': get_attr(dataset2, 'swvl3'),
         # 'swvl4': get_attr(dataset2, 'swvl4'),
         # 'evabs': get_attr(dataset3, 'evabs'),
         # 'evaow': get_attr(dataset3, 'evaow'),
         # 'evatc': get_attr(dataset3, 'evatc'),
         # 'evavt': get_attr(dataset3, 'evavt'),
         # 'pev': get_attr(dataset3, 'pev'),
         # 'ro': get_attr(dataset3, 'ro'),
         # 'ssr': get_attr(dataset3, 'ssr'),
         # 'str': get_attr(dataset3, 'str'),
         # 'sshf': get_attr(dataset3, 'sshf'),
         # 'ssrd': get_attr(dataset3, 'ssrd'),
         # 'strd': get_attr(dataset3, 'strd'),
         # 'u10': get_attr(dataset4, 'u10'),
         # 'v10': get_attr(dataset4, 'v10'),
         # 'lai_hv': get_attr(dataset4, 'lai_hv'),
         # 'lai_lv': get_attr(dataset4, 'lai_lv'),
         # 'es': get_attr(dataset4, 'es'),
         # 'ssro': get_attr(dataset4, 'ssro'),
         # 'sp': get_attr(dataset4, 'sp'),
         # 'sro': get_attr(dataset4, 'sro'),
         # 'e': get_attr(dataset4, 'e'),
         # 'tp': get_attr(dataset4, 'tp')}

