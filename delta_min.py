# collect data from database ic
import pymysql
import numpy as np
import datetime
import os
import pandas as pd
import copy
import matplotlib.pyplot as plt    
from matplotlib.pyplot import MultipleLocator

data = np.load('ic_ok.npy',allow_pickle = True).item()

trade_time = list(data.keys())
trade_time.sort()

years = [2017,2018,2019,2020,2021,2022]

for t in trade_time:
    if(t.year not in years):
        data.pop(t)

trade_time = list(data.keys())
trade_time.sort()




def plot_delta(delta_symbol = delta_symbol):

    delta_list = {2017:[],2018:[],2019:[],2020:[],2021:[],2022:[]}
    # 06-09  len(trade_time)/241 = 544 
    for t in trade_time:
        price0 = 0
        price1 = 0
        for i in data[t]:
            if(i[0][-2:] == delta_symbol[0]):
                price0 = i[3] 
            if(i[0][-2:] == delta_symbol[1]):
                price1 = i[3]
        if(price0 != 0 and price1 != 0):
            delta = price0 - price1
            delta_list[t.year].append(delta)


    panzhong_list = {2017:[],2018:[],2019:[],2020:[],2021:[],2022:[]}
    for y in years:
        all_min = delta_list[y]
        panzhong_list[y].append([all_min[i] for i in range(len(all_min)) if(i%241 > 5 and (i%241 - 241) < -5) ])

    ''' 
    panzhong_list_2 = {2017:[],2018:[],2019:[],2020:[],2021:[],2022:[]}
    for y in years:
        all_min = delta_list[y]
        panzhong_list_2[y].append([all_min[i] for i in range(len(all_min)) if(i%241 > 10 and (i%241 - 241) < -10) ])
    '''

    mean_dict = {2017:[],2018:[],2019:[],2020:[],2021:[],2022:[]}
    # 22895/241 = 95
    for y in years:
        all_min = delta_list[y]
        inter = 241
        total_length = len(all_min)
        n_days = total_length/inter
        n_days = int(n_days)
        for i in range(inter):
            same_min = all_min[i:total_length:inter] 
            same_min = np.array(same_min) 
            mean = same_min.mean()
            mean_dict[y].append(mean) 

    #plot
    #run configuration 
    plt.rcParams['font.size']=12
    #plt.rcParams['font.family'] = 'Arial'
    plt.rcParams['axes.linewidth']=0.5
    plt.rcParams['axes.grid']=True
    plt.rcParams['grid.linestyle']='--'
    plt.rcParams['grid.linewidth']=0.2
    plt.rcParams["savefig.transparent"]='True'  
    plt.rcParams['lines.linewidth']=0.8
    plt.rcParams['lines.markersize'] = 1

    # min close price
    '''
    #subplot
    fig,ax = plt.subplots(1,1)

    for i in years:
        ax.plot(delta_list[i],label='%s'%i)

    plt.legend()

    #  there are some extra point in upper graph
    list_2017 = delta_list[2017]
    where_list = [i for i in range(10,len(list_2017)) if (abs(list_2017[i] - np.mean(list_2017[i-10:i+10])) > 30)]
    [trade_time[i] for i in where_list]
    '''


    # remove near kaipan shoupan less than 5min
    fig,ax = plt.subplots(1,2)

    for i in years:
        ax[0].plot(panzhong_list[i][0],label='%s'%i)

    # mean price every min

    for i in years:
        ax[1].plot(mean_dict[i],label='%s'%i)
    ax[0].set_title(delta_symbol)
    plt.legend()
    
 
 
 
 plot_delta(['01','02'])
