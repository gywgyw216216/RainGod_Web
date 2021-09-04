#!usr/bin/env python
# -*- coding: utf-8 -*-

"""
crawler.py
Define Some Crawler Functions and Some Parser Functions.
1. selenium_initialization()
2. crawler()
3. parser()
4. get_province_data()
5. get_city_data()
6. get_weather_data()
"""

from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.ie.options import Options as IeOptions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
from sys import platform
from time import sleep
from random import uniform
from datetime import datetime, timedelta

from weather import RealTimeWeather, DateWeather
from tools import print_log, print_traceback_error



# Define Some Global Variables.
if platform == 'win32':
    CHROME_WEB_DRIVER_PATH = './resources/chromedriver.exe'  # Windows Chrome Web Driver Path
    FIREFOX_WEB_DRIVER_PATH = './resources/geckodriver.exe'  # Windows Firefox Web Driver Path
else:
    CHROME_WEB_DRIVER_PATH = './resources/chromedriver'  # Linux Chrome Web Driver Path
    FIREFOX_WEB_DRIVER_PATH = './resources/geckodriver'  # Linux Firefox Web Driver Path



def selenium_initialization(browser_webdriver_path, browser_type, hide=True):
    """
    初始化Selenium Web Driver

    :parameter browser_webdriver_path: Browser Web Driver路径
    :type browser_webdriver_path: <class 'str'>

    :parameter browser_type: 浏览器类型
    :type browser_type: <class 'str'>

    :parameter hide: 是否不显示浏览器，默认为True
    :type hide: <class 'bool'>

    :return: webdriver: Selenium Web Driver
    :rtype: <class 'selenium.webdriver'>

    :raise: exception: 浏览器类型错误
    :type: <class 'Exception'>

    :exception: exception: Selenium Web Driver初始化错误
    :type: <class 'Exception'>
    """
    
    try:
        print_log('Selenium Web Driver Initializing...... ')
        
        if browser_type == 'Firefox' or browser_type == 'firefox':
            option = FirefoxOptions()  # 设置Firefox浏览器选项
            option.add_argument('--incognito')  # 以隐私窗口访问
            option.add_argument('--disable-gpu')  # 禁用GPU加速
            option.add_argument('--no-sandbox')  # 以root权限访问
            
            if hide:
                option.add_argument('--headless')  # 不显示浏览器
            
            print_log('Selenium Web Driver Initialization Successfully! ')
            
            return webdriver.Firefox(executable_path=browser_webdriver_path, options=option)
        elif browser_type == 'Chrome' or browser_type == 'chrome':
            option = ChromeOptions()  # 设置Chrome浏览器选项
            option.add_argument('--incognito')  # 以无痕窗口访问
            option.add_argument('--disable-gpu')  # 禁用GPU加速
            option.add_argument('--no-sandbox')  # 以root权限访问
            
            if hide:
                option.add_argument('--headless')  # 不显示浏览器
            
            print_log('Selenium Web Driver Initialization Successfully! ')
            
            return webdriver.Chrome(executable_path=browser_webdriver_path, options=option)
        elif browser_type == 'Safari' or browser_type == 'safari':
            print_log('Selenium Web Driver Initialization Successfully! ')
            
            return webdriver.Safari(executable_path=browser_webdriver_path)
        elif browser_type == 'IE' or browser_type == 'ie' or browser_type == 'Ie':
            option = IeOptions()  # 设置IE浏览器选项
            option.add_argument('--incognito')  # 以无痕窗口访问
            option.add_argument('--disable-gpu')  # 禁用GPU加速
            option.add_argument('--no-sandbox')  # 以root权限访问
            
            if hide:
                option.add_argument('--headless')  # 不显示浏览器
            
            print_log('Selenium Web Driver Initialization Successfully! ')
            
            return webdriver.Ie(executable_path=browser_webdriver_path, options=option)
        elif browser_type == 'Edge' or browser_type == 'edge':
            print_log('Selenium Web Driver Initialization Successfully! ')
            
            return webdriver.Edge(executable_path=browser_webdriver_path)
        else:
            raise Exception('Browser Type ERROR! ')
    except Exception as exception:
        print_traceback_error(exception, 'Selenium Web Driver Initialization')
        
        return None



def crawler(web_driver, url):
    """
    爬虫
    
    :parameter web_driver: Selenium Web Driver
    :type web_driver: <class 'selenium.webdriver'>
    
    :parameter url: 爬虫目标地址
    :type url: <class 'str'>
    
    :return: result: 爬虫目标地址HTML源代码
    :rtype: <class 'str'>
    
    :exception: exception: 爬虫错误
    :type: <class 'Exception'>
    """
    
    print_log('Crawling...... ')
    
    while True:
        try:
            '''
            每隔0.5秒进行HTML页面是否动态加载结束的判断，超时等待时间为5秒，最多检查10次。
            若在5秒内HTML页面动态加载结束，则进行下一步操作；
            否则重新访问HTML页面，进行是否动态加载结束判断。
            '''
            while True:
                try:
                    web_driver.get(url)
                    WebDriverWait(web_driver, 5).until(
                        expected_conditions.text_to_be_present_in_element((By.CSS_SELECTOR, 'option[value=""]'),
                                                                          '请选择市区'), 'Timeout Exception')
                    
                    break
                except TimeoutException as exception:
                    print_traceback_error(TimeoutException, exception.msg)
                    sleep(uniform(10, 300))
            
            result = web_driver.page_source
            web_driver.close()
            web_driver.quit()
            print_log('Crawler Successfully! ')
            
            return result
        except Exception as exception:
            print_traceback_error(exception, 'Crawler')
            sleep(300)



def parser(html):
    """
    Beautiful Soup HTML源代码解析
    
    :parameter html: 爬虫目标地址HTML源代码
    :type html: <class 'str'>
    
    :return: soup: 爬虫目标地址HTML源代码解析结果
    :rtype: <class 'bs4.BeautifulSoup'>
    """
    
    return BeautifulSoup(html, 'lxml')



def get_province_data(url):
    """
    获取省份数据
    
    :parameter url: 爬虫目标地址
    :type url: <class 'str'>
    
    :returns
    
    :return: province_ids: 省份id字符串列表
    :rtype: <class 'list'>
    
    :return: provinces: 省份字符串列表
    :rtype: <class 'list'>
    
    :exception: exception: 爬虫目标地址HTML源代码解析错误
    :type: <class 'Exception'>
    """
    
    try:
        # 使用Firefox浏览器进行爬虫
        web_driver = selenium_initialization(FIREFOX_WEB_DRIVER_PATH, 'Firefox')
        html = crawler(web_driver, url)
        print_log('Parsing...... ')
        soup = parser(html)
        # 使用BeautifulSoup进行解析
        options = soup.find_all('option')
        province_ids = []
        provinces = []
        
        # 遍历列表提取HTML标签内的属性值和文本
        for item in options:
            if len(list(item.attrs.keys())) == 1:
                if item.attrs['value']:
                    province_ids.append(item.attrs['value'])
                    provinces.append(item.get_text())
        
        print_log('Parser Successfully! ')
        
        return province_ids, provinces
    except Exception as exception:
        print_traceback_error(exception, 'Parser')
        
        return None, None



def get_city_data(url):
    """
    获取城市数据
    
    :parameter url: 爬虫目标地址
    :type url: <class 'str'>
    
    :returns
    
    :return: city_urls: 城市url字符串列表
    :rtype: <class 'list'>
    
    :return: cities: 城市字符串列表
    :rtype: <class 'list'>
    
    :exception: exception: 爬虫目标地址HTML源代码解析错误
    :type: <class 'Exception'>
    """
    
    try:
        # 使用Firefox浏览器进行爬虫
        web_driver = selenium_initialization(FIREFOX_WEB_DRIVER_PATH, 'Firefox')
        html = crawler(web_driver, url)
        print_log('Parsing...... ')
        soup = parser(html)
        # 使用BeautifulSoup进行解析
        options = soup.find_all('option')
        city_urls = []
        cities = []
        
        # 遍历列表提取HTML标签内的属性值和文本
        for item in options:
            if len(list(item.attrs.keys())) == 2:
                city_urls.append(item.attrs['url'][22:-5])
                cities.append(item.get_text())
        
        print_log('Parser Successfully! ')
        
        return city_urls, cities
    except Exception as exception:
        print_traceback_error(exception, 'Parser')
        
        return None, None



def get_weather_data(url):
    """
    获取气象数据

    :parameter url: 爬虫目标地址
    :type url: <class 'str'>

    :returns

    :return: real_time_weather: 实时天气类对象
    :rtype: <class 'weather.RealTimeWeather'>

    :return: date_weathers: 日期天气类对象列表
    :rtype: <class 'list'>

    :exception: exception: 爬虫目标地址HTML源代码解析错误
    :type: <class 'Exception'>
    """
    
    try:
        # 使用Firefox浏览器进行爬虫
        web_driver = selenium_initialization(FIREFOX_WEB_DRIVER_PATH, 'Firefox')
        html = crawler(web_driver, url)
        print_log('Parsing...... ')
        soup = parser(html)
        # 使用BeautifulSoup进行解析
        real_time_publish_time = soup.find(id='realPublishTime').get_text()
        real_time_temperature = soup.find(id='realTemperature').get_text()
        real_time_precipitation = soup.find(id='realRain').get_text()
        real_time_wind_direction = soup.find(id='realWindDirect').get_text()
        real_time_wind_power = soup.find(id='realWindPower').get_text()
        real_time_relative_humidity = soup.find(id='realHumidity').get_text()
        real_time_sensible_temperature = soup.find(id='realFeelst').get_text()
        real_time_aqi = soup.find(id='aqi').get_text().strip()
        real_time_comfort = soup.find(id='realIcomfort').get_text()
        # 创建RealTimeWeather类对象real_time_weather
        real_time_weather = RealTimeWeather(real_time_publish_time, real_time_temperature, real_time_precipitation,
                                            real_time_wind_direction, real_time_wind_power, real_time_relative_humidity,
                                            real_time_sensible_temperature, real_time_aqi, real_time_comfort)
        # 使用BeautifulSoup进行解析
        temperatures = soup.find_all('div', attrs={'class': 'tmp'})
        weather_descriptions = soup.find_all('div', attrs={'class': 'desc'})
        wind_directions = soup.find_all('div', attrs={'class': 'windd'})
        wind_levels = soup.find_all('div', attrs={'class': 'winds'})
        # 创建date_weathers列表
        date_weathers = []
        # 获取当前日期时间
        date = datetime.today()
        
        # 遍历各列表提取HTML标签内文本
        for i in range(7):
            highest_temperature = temperatures[2 * i].get_text().strip()
            lowest_temperature = temperatures[2 * i + 1].get_text().strip()
            weather_description1 = weather_descriptions[2 * i].get_text().strip()
            weather_description2 = weather_descriptions[2 * i + 1].get_text().strip()
            wind_direction1 = wind_directions[2 * i].get_text().strip()
            wind_direction2 = wind_directions[2 * i + 1].get_text().strip()
            wind_level1 = wind_levels[2 * i].get_text().strip()
            wind_level2 = wind_levels[2 * i + 1].get_text().strip()
            # 创建DateWeather类对象并添加进DateWeather类对象列表date_weathers
            date_weathers.append(
                DateWeather(date, highest_temperature, lowest_temperature, weather_description1, weather_description2,
                            wind_direction1, wind_direction2, wind_level1, wind_level2))
            # 当前日期时间值＋1天
            date += timedelta(days=1)
        
        print_log('Parser Successfully! ')
        
        return real_time_weather, date_weathers
    except Exception as exception:
        print_traceback_error(exception, 'Parser')
        
        return None, None



if __name__ == '__main__':
    province_ids, provinces = get_province_data('http://www.nmc.cn/publish/forecast/ASH/xujiahui.html')
    city_urls, cities = get_city_data('http://www.nmc.cn/publish/forecast/ASH/xujiahui.html')
    real_time_weather, date_weathers = get_weather_data('http://www.nmc.cn/publish/forecast/ASH/xujiahui.html')
    
    print(province_ids)
    print(provinces)
    print(city_urls)
    print(cities)
    print(real_time_weather)
    
    for i in date_weathers:
        print(i)
