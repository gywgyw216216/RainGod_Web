#!usr/bin/env python
# -*- coding: utf-8 -*-

"""
weather.py
Define Some Weather Classes.
1. RealTimeWeather
2. DateWeather
"""



class RealTimeWeather:
    """
    实时天气类
    """
    
    
    
    def __init__(self, publish_time, temperature, precipitation, wind_direction, wind_power, relative_humidity,
                 sensible_temperature, aqi, comfort):
        """
        实时天气类初始化
        
        :parameter publish_time: 实时发布/更新时间
        :type publish_time: <class 'str'>
        
        :parameter temperature: 实时气温
        :type temperature: <class 'str'>
        
        :parameter precipitation: 实时降水量
        :type precipitation: <class 'str'>
        
        :parameter wind_direction: 实时风向
        :type wind_direction: <class 'str'>
        
        :parameter wind_power: 实时风力
        :type wind_power: <class 'str'>
        
        :parameter relative_humidity: 实时相对湿度
        :type relative_humidity: <class 'str'>
        
        :parameter sensible_temperature: 实时体感温度
        :type sensible_temperature: <class 'str'>
        
        :parameter aqi: 实时空气质量
        :type aqi: <class 'str'>
        
        :parameter comfort: 实时舒适度
        :type comfort: <class 'str'>
        """
        
        if publish_time == '':
            self.__publish_time = '-'
        else:
            self.__publish_time = publish_time
        
        if temperature == '':
            self.__temperature = '-'
        else:
            self.__temperature = temperature
        
        if precipitation == '':
            self.__precipitation = '-'
        else:
            self.__precipitation = precipitation
        
        if wind_direction == '':
            self.__wind_direction = '-'
        else:
            self.__wind_direction = wind_direction
        
        if wind_power == '':
            self.__wind_power = '-'
        else:
            self.__wind_power = wind_power
        
        if relative_humidity == '':
            self.__relative_humidity = '-'
        else:
            self.__relative_humidity = relative_humidity
        
        if sensible_temperature == '':
            self.__sensible_temperature = '-'
        else:
            self.__sensible_temperature = sensible_temperature
        
        if aqi == '':
            self.__aqi = '-'
        else:
            self.__aqi = aqi
        
        if comfort == '':
            self.__comfort = '-'
        else:
            self.__comfort = comfort
    
    
    
    def get_publish_time(self):
        """
        获取实时发布/更新时间
        
        :return: self.__publish_time: 实时发布/更新时间
        :rtype: <class 'str'>
        """
        
        return self.__publish_time
    
    
    
    def get_temperature(self):
        """
        获取实时气温
        
        :return: self.__temperature: 实时气温
        :rtype: <class 'str'>
        """
        
        return self.__temperature
    
    
    
    def get_precipitation(self):
        """
        获取实时降水量
        
        :return: self.__precipitation: 实时降水量
        :rtype: <class 'str'>
        """
        
        return self.__precipitation
    
    
    
    def get_wind_direction(self):
        """
        获取实时风向
        
        :return: self.__wind_direction: 实时风向
        :rtype: <class 'str'>
        """
        
        return self.__wind_direction
    
    
    
    def get_wind_power(self):
        """
        获取实时风力
        
        :return: self.__wind_power: 实时风力
        :rtype: <class 'str'>
        """
        
        return self.__wind_power
    
    
    
    def get_relative_humidity(self):
        """
        获取实时相对湿度
        
        :return: self.__relative_humidity: 实时相对湿度
        :rtype: <class 'str'>
        """
        
        return self.__relative_humidity
    
    
    
    def get_sensible_temperature(self):
        """
        获取实时体感温度
        
        :return: self.__sensible_temperature: 实时体感温度
        :rtype: <class 'str'>
        """
        
        return self.__sensible_temperature
    
    
    
    def get_aqi(self):
        """
        获取实时空气质量
        
        :return: self.__aqi: 实时空气质量
        :rtype: <class 'str'>
        """
        
        return self.__aqi
    
    
    
    def get_comfort(self):
        """
        获取实时舒适度
        
        :return: self.__comfort: 实时舒适度
        :rtype: <class 'str'>
        """
        
        return self.__comfort
    
    
    
    def __str__(self) -> str:
        """
        重写__str__()方法
        
        :return: string: 实时天气类属性字符串
        :rtype: <class 'str'>
        """
        
        return self.get_publish_time() + "，实时温度：" + self.get_temperature() + "，实时降水量：" + self.get_precipitation() + \
               "，实时风向：" + self.get_wind_direction() + "，实时风力：" + self.get_wind_power() + "，实时相对湿度：" + \
               self.get_relative_humidity() + "，实时体感温度：" + self.get_sensible_temperature() + "，实时" + self.get_aqi() + \
               "，实时" + self.get_comfort()



class DateWeather:
    """
    日期天气类
    """
    
    
    
    def __init__(self, date, highest_temperature, lowest_temperature, weather_description1, weather_description2,
                 wind_direction1, wind_direction2, wind_level1, wind_level2):
        """
        日期天气类初始化
        
        :parameter date: 日期
        :type date: class 'datetime.datetime'>
        
        :parameter highest_temperature: 最高温度
        :type highest_temperature: <class 'str'>
        
        :parameter lowest_temperature: 最低温度
        :type lowest_temperature: <class 'str'>
        
        :parameter weather_description1: 最高温度天气
        :type weather_description1: <class 'str'>
        
        :parameter weather_description2: 最低温度天气
        :type weather_description2: <class 'str'>
        
        :parameter wind_direction1: 最高温度风向
        :type wind_direction1: <class 'str'>
        
        :parameter wind_direction2: 最低温度风向
        :type wind_direction2: <class 'str'>
        
        :parameter wind_level1: 最高温度风力
        :type wind_level1: <class 'str'>
        
        :parameter wind_level2: 最低温度风力
        :type wind_level2: <class 'str'>
        """
        
        self.__date = date
        
        if highest_temperature == '':
            self.__highest_temperature = '-'
        else:
            self.__highest_temperature = highest_temperature
        
        if lowest_temperature == '':
            self.__lowest_temperature = '-'
        else:
            self.__lowest_temperature = lowest_temperature
        
        if weather_description1 == '':
            self.__weather_description1 = '-'
        else:
            self.__weather_description1 = weather_description1
        
        if weather_description2 == '':
            self.__weather_description2 = '-'
        else:
            self.__weather_description2 = weather_description2
        
        if wind_direction1 == '':
            self.__wind_direction1 = '-'
        else:
            self.__wind_direction1 = wind_direction1
        
        if wind_direction2 == '':
            self.__wind_direction2 = '-'
        else:
            self.__wind_direction2 = wind_direction2
        
        if wind_level1 == '':
            self.__wind_level1 = '-'
        else:
            self.__wind_level1 = wind_level1
        
        if wind_level2 == '':
            self.__wind_level2 = '-'
        else:
            self.__wind_level2 = wind_level2
    
    
    
    def get_date(self):
        """
        获取日期
        
        :return: self.__date: 日期
        :rtype: <class 'datetime.datetime'>
        """
        
        return self.__date
    
    
    
    def get_highest_temperature(self):
        """
        获取最高温度
        
        :return: self.__highest_temperature: 最高温度
        :rtype: <class 'str'>
        """
        
        return self.__highest_temperature
    
    
    
    def get_lowest_temperature(self):
        """
        获取最低温度
        
        :return: self.__lowest_temperature: 最低温度
        :rtype: <class 'str'>
        """
        
        return self.__lowest_temperature
    
    
    
    def get_weather_description1(self):
        """
        获取最高温度天气
        
        :return: self.__weather_description1: 最高温度天气
        :rtype: <class 'str'>
        """
        
        return self.__weather_description1
    
    
    
    def get_weather_description2(self):
        """
        获取最低温度天气
        
        :return: self.__weather_description2: 最低温度天气
        :rtype: <class 'str'>
        """
        
        return self.__weather_description2
    
    
    
    def get_wind_direction1(self):
        """
        获取最高温度风向
        
        :return: self.__wind_direction1: 最高温度风向
        :rtype: <class 'str'>
        """
        
        return self.__wind_direction1
    
    
    
    def get_wind_direction2(self):
        """
        获取最低温度风向
        
        :return: self.__wind_direction2: 最低温度风向
        :rtype: <class 'str'>
        """
        
        return self.__wind_direction2
    
    
    
    def get_wind_level1(self):
        """
        获取最高温度风力
        
        :return: self.__wind_level1: 最高温度风力
        :rtype: <class 'str'>
        """
        
        return self.__wind_level1
    
    
    
    def get_wind_level2(self):
        """
        获取最低温度风力
        
        :return: self.__wind_level2: 最低温度风力
        :rtype: <class 'str'>
        """
        
        return self.__wind_level2
    
    
    
    def __str__(self) -> str:
        """
        重写__str__()方法

        :return: string: 日期天气类属性字符串
        :rtype: <class 'str'>
        """
        
        return self.get_date().strftime(
            "%Y-%m-%d, %A") + "：最高温度：" + self.get_highest_temperature() + "，天气：" + self.get_weather_description1() + \
               "，风向：" + self.get_wind_direction1() + "，风力：" + self.get_wind_level1() + "；最低温度：" + \
               self.get_lowest_temperature() + "，天气：" + self.get_weather_description2() + "，风向：" + \
               self.get_wind_direction2() + "，风力：" + self.get_wind_level2()
