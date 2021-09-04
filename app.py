#!usr/bin/env python
# -*- coding: utf-8 -*-

"""
app.py
"""

from flask import Flask, render_template, request, redirect, url_for
from mysql import query_all_provinces, query_city, query_all_weathers_data



app = Flask(__name__)



@app.route('/')
@app.route('/index/')
def select():
    query_provinces = query_all_provinces()
    provinces = []
    cities = []
    
    for i in query_provinces:
        provinces.append(i['province'])
    
    for i in provinces:
        query_cities = query_city(i)
        city = []
        
        if query_cities:
            for j in query_cities:
                city.append(j['city'])
        
        cities.append({i: city})
    
    return render_template('index.html', provinces=provinces, cities=cities)



@app.route('/deal/')
def deal():
    province = request.args.get('province')
    city = request.args.get('city')
    
    return redirect(url_for('weather', province=province, city=city))



@app.route('/weather/')
def weather():
    province = request.args.get('province')
    city = request.args.get('city')
    real_time_weather, date_weathers = query_all_weathers_data(province, city)
    
    context = {
        'province': province, 'city': city, 'real_time_weather': real_time_weather, 'date_weathers': date_weathers
    }
    
    return render_template('weather.html', **context)



if __name__ == '__main__':
    """
    Python气象数据可视化Web前端Flask启动
    """
    
    app.run()  # 运行Flask app
