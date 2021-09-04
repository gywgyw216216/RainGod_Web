#!usr/bin/env python
# -*- coding: utf-8 -*-

"""
main.py
"""

from multiprocessing import Pool, Manager
from os import cpu_count
from time import sleep
from random import uniform
from datetime import datetime

from crawler import get_province_data, get_city_data, get_weather_data
from mysql import (
    delete_all_provinces, update_province, delete_all_city, update_city, delete_all_weathers_data,
    update_all_weathers_data,
)
from tools import print_log, print_traceback_error



# Define Global Variables.
DEFAULT_URL = 'http://www.nmc.cn/publish/forecast/ASH/xujiahui.html'  # Default URL
FIRST_CITY_URLS = ['beijing', 'tianjin', 'shijiazhuang', 'taiyuan', 'huhehaote', 'shenyang', 'changchun', 'haerbin',
                   'xujiahui', 'nanjing', 'hangzhou', 'hefei', 'fuzhou', 'nanchang', 'jinan', 'zhengzhou', 'wuhan',
                   'changshashi', 'guangzhou', 'nanning', 'haikou', 'shapingba', 'chengdu', 'guiyang', 'kunming',
                   'lasa', 'xian', 'lanzhou', 'xining', 'yinchuan', 'wulumuqi', 'xianggang', 'aomen',
                   'taibei']  # First City URLs



def get_all_cities_data(province_id, province, first_city_url, all_city_urls, all_cities, lock):
    """
    获取所有城市数据

    :parameter province_id: 省份代号
    :type province_id: <class 'str'>

    :parameter province: 省份名
    :type province: <class 'str'>

    :parameter first_city_url: 第一个城市的url
    :type first_city_url: <class 'str'>

    :parameter all_city_urls: 所有城市url的字典列表
    :type all_city_urls: <class 'multiprocessing.managers.ListProxy'>

    :parameter all_cities: 所有城市的字典列表
    :type all_cities: <class 'multiprocessing.managers.ListProxy'>

    :parameter lock: 多进程管理锁
    :type lock: <class 'multiprocessing.managers.AcquirerProxy'>
    """
    
    city_urls, cities = get_city_data(
        'http://www.nmc.cn/publish/forecast/' + province_id + '/' + first_city_url + '.html')
    
    # 操作共享变量的临界区代码
    lock.acquire()  # 子进程获取多进程管理锁
    all_city_urls.append({province_id: city_urls})
    all_cities.append({province: cities})
    lock.release()  # 子进程释放多进程管理锁



def get_all_weathers_data(province_id, province, city_url, city, all_real_time_weathers, all_date_weathers, lock):
    """
    获取所有气象数据

    :parameter province_id: 省份代号
    :type province_id: <class 'str'>

    :parameter province: 省份名
    :type province: <class 'str'>

    :parameter city_url: 城市url
    :type city_url: <class 'str'>

    :parameter city: 城市名
    :type city: <class 'str'>

    :parameter all_real_time_weathers: 所有实时天气元组列表
    :type all_real_time_weathers: <class 'multiprocessing.managers.ListProxy'>

    :parameter all_date_weathers: 所有日期天气元组列表
    :type all_date_weathers: <class 'multiprocessing.managers.ListProxy'>

    :parameter lock: 多进程管理锁
    :type lock: <class 'multiprocessing.managers.AcquirerProxy'>
    """
    
    real_time_weather, date_weathers = get_weather_data(
        'http://www.nmc.cn/publish/forecast/' + province_id + '/' + city_url + '.html')
    
    lock.acquire()
    all_real_time_weathers.append((province, city, real_time_weather))
    all_date_weathers.append((province, city, date_weathers))
    lock.release()



def main():
    """
    Python气象数据爬取服务器端主函数
    """
    
    print_log('RainGod Initializing...... \n')
    
    manager = Manager()  # 创建多进程管理对象
    lock = manager.Lock()  # 创建多进程管理锁
    # 初始化共享变量
    all_city_urls = manager.list()  # 所有城市url的字典列表
    all_cities = manager.list()  # 所有城市的字典列表
    all_real_time_weathers = manager.list()  # 所有实时天气元组列表
    all_date_weathers = manager.list()  # 所有日期天气元组列表
    
    print_log('Getting Province Data...... \n')
    province_ids, provinces = get_province_data(DEFAULT_URL)
    delete_all_provinces()
    
    for i in range(len(province_ids)):
        update_province(provinces[i])
    
    print_log('Get Province Data Successfully! \n')
    
    while True:
        try:
            print_log('Getting City Data...... \n')
            
            pool = Pool(cpu_count())  # 创建多进程池，最大进程容量为当前机器上的CPU处理器核心数
            
            for i in range(len(FIRST_CITY_URLS)):
                pool.apply_async(func=get_all_cities_data, args=(
                    province_ids[i], provinces[i], FIRST_CITY_URLS[i], all_city_urls, all_cities,
                    lock,))  # 循环创建获取所有城市数据异步子进程
                sleep(uniform(0, 5))
            
            pool.close()  # 关闭多进程池
            pool.join()  # 主进程阻塞，等待子进程退出
            
            delete_all_city()
            
            pool = Pool(cpu_count())
            
            for i in range(len(all_city_urls)):
                province_id = list(all_city_urls[i].keys())[0]
                province = list(all_cities[i].keys())[0]
                city_urls = list(all_city_urls[i].values())[0]
                cities = list(all_cities[i].values())[0]
                
                for j in range(len(city_urls)):
                    pool.apply_async(func=update_city, args=(province, cities[j],))  # 循环创建更新城市数据异步子进程
            
            pool.close()
            pool.join()
            
            print_log('Get City Data Successfully! \n')
            print_log('RainGod Initialization Successfully! \n')
            
            break
        except Exception as exception:
            print_traceback_error(exception, 'RainGod Initialization')
            all_city_urls[:] = []
            all_cities[:] = []
    
    # 主循环
    while True:
        hour = datetime.now().hour  # 获取当前日期时间的小时数
        minute = datetime.now().minute  # 获取当前日期时间的分钟数
        
        if (not minute) and (not hour % 24):  # 每天0点0分、24点0分进行一次气象数据爬取与写入MySQL数据库表操作
            try:
                print_log('RainGod Running...... \n')
                print_log('Getting All Weather Data...... \n')
                
                all_real_time_weathers[:] = []
                all_date_weathers[:] = []
                
                pool = Pool(cpu_count())
                
                for i in range(len(all_city_urls)):
                    province_id = list(all_city_urls[i].keys())[0]
                    province = list(all_cities[i].keys())[0]
                    city_urls = list(all_city_urls[i].values())[0]
                    cities = list(all_cities[i].values())[0]
                    
                    for j in range(len(city_urls)):
                        pool.apply_async(func=get_all_weathers_data, args=(
                            province_id, province, city_urls[j], cities[j], all_real_time_weathers, all_date_weathers,
                            lock,))  # 循环创建获取所有气象数据异步子进程
                        sleep(uniform(5 * cpu_count(), 15 * cpu_count()))
                
                pool.close()
                pool.join()
                
                print_log('Get All Weather Data Successfully! ')
                print_log('Updating MySQL Database "raingod"...... ')
                
                delete_all_weathers_data()
                
                pool = Pool(cpu_count())
                
                for i in range(len(all_real_time_weathers)):
                    pool.apply_async(func=update_all_weathers_data, args=(
                        all_real_time_weathers[i][0], all_real_time_weathers[i][1], all_real_time_weathers[i][2],
                        all_date_weathers[i][2],))  # 循环创建更新所有气象数据异步子进程
                
                pool.close()
                pool.join()
                
                print_log('Update MySQL Database "raingod" Successfully! \n')
                print_log('RainGod Finish! \n')
                sleep(55)
                print_log('RainGod New Round. \n')
            except Exception as exception:
                print_traceback_error(exception, 'RainGod Runtime')
                sleep(5)
        else:
            sleep(55)



if __name__ == '__main__':
    main()
