#!usr/bin/env python
# -*- coding: utf-8 -*-

"""
mysql.py
Define Some MySQL Functions.
1. mysql_initialization()
2. create()
3. update()
4. retrieve()
5. delete()
6. mysql_close()
7. delete_all_provinces()
8. update_province()
9. query_all_provinces()
10. delete_all_city()
11. update_city()
12. query_city()
13. delete_all_real_time_weathers()
14. update_real_time_weather()
15. query_real_time_weather()
16. delete_all_date_weathers()
17. update_date_weather()
18. query_date_weather()
19. delete_all_weathers_data()
20. update_all_weathers_data()
21. query_all_weathers_data()
"""

from pymysql import connect
from pymysql.cursors import DictCursor

from tools import print_log, print_traceback_error



# Define Global Variables.
HOST = 'localhost'  # MySQL Host
PORT = 3306  # MySQL Port
USERNAME = 'root'  # MySQL Username



def mysql_initialization(database, host='localhost', port=3306, username='root', password=None, charset='utf8mb4'):
    """
    初始化MySQL
    
    :parameter database: MySQL数据库名称
    :type database: <class 'str'>
    
    :parameter host: MySQL Host，默认为localhost
    :type host: <class 'str'>
    
    :parameter port: MySQL端口号，默认为3306
    :type port: <class 'int'>
    
    :parameter username: MySQL用户名，默认为root
    :type username: <class 'str'>
    
    :parameter password: MySQL密码，默认为None
    :type password: <class 'str'>
    
    :parameter charset: MySQL字符集，默认为utf8mb4
    :type charset: <class 'str'>
    
    :returns
    
    :return: connection: MySQL连接对象
    :rtype: <class 'pymysql.connections.Connection'>
    
    :return: cursor: MySQL游标对象
    :rtype: <class 'pymysql.cursors.DictCursor'>
    
    :exception: exception: MySQL初始化错误
    :type: <class 'Exception'>
    """
    
    try:
        connection = connect(host=host, port=port, user=username, password=password, database=database,
                             charset=charset)  # 创建MySQL连接
        cursor = connection.cursor(cursor=DictCursor)  # 创建MySQL游标
        print_log('MySQL Initialization Successfully! ')
        print_log('Connect MySQL Database "' + database + '" Successfully! ')
        
        return connection, cursor
    except Exception as exception:
        print_log('Fail to Connect MySQL Database "' + database + '"! ')
        print_traceback_error(exception, 'MySQL Initialization')
        
        return None, None



def create(connection, cursor, sql):
    """
    MySQL创建
    
    :parameter connection: MySQL连接对象
    :type connection: <class 'pymysql.connections.Connection'>
    
    :parameter cursor: MySQL游标对象
    :type cursor: <class 'pymysql.cursors.DictCursor'>
    
    :parameter sql: SQL语句
    :type sql: <class 'str'>
    
    :exception: exception: 创建错误
    :type: <class 'Exception'>
    """
    
    try:
        cursor.execute(sql)  # 执行SQL语句进行创建操作
        print_log('Create Successfully! ')
    except Exception as exception:
        print_traceback_error(exception, 'Create')



def update(connection, cursor, operation, sql, value=None):
    """
    MySQL更新数据
    
    :parameter connection: MySQL连接对象
    :type connection: <class 'pymysql.connections.Connection'>
    
    :parameter cursor: MySQL游标对象
    :type cursor: <class 'pymysql.cursors.DictCursor'>
    
    :parameter operation: MySQL更新操作类型
    :type operation: <class ‘str'>
    
    :parameter sql: SQL语句
    :type sql: <class 'str'>
    
    :parameter value: MySQL更新数据列表，默认为None
    :type value: <class 'list'>
    
    :raise: exception: 更新操作类型错误
    :type: <class 'Exception'>
    
    :exception: exception: 更新错误
    :type: <class 'Exception'>
    """
    
    try:
        # 判断更新操作类型
        if operation == 'update' or operation == 'Update' or operation == 'UPDATE':
            cursor.execute(sql, value)  # 执行SQL语句进行更新操作
            connection.commit()  # 提交操作更改
            print_log('Update Data Successfully! ')
        elif operation == 'insert' or operation == 'Insert' or operation == 'INSERT':
            cursor.executemany(sql, value)  # 执行SQL语句进行插入多条数据操作
            connection.commit()
            print_log('Update Data Successfully! ')
        else:
            raise Exception('Update Operation ERROR! ')
    except Exception as exception:
        print_traceback_error(exception, 'Update')
        connection.rollback()  # 回滚操作



def retrieve(connection, cursor, query):
    """
    MySQL查询
    
    :parameter connection: MySQL连接对象
    :type connection: <class 'pymysql.connections.Connection'>
    
    :parameter cursor: MySQL游标对象
    :type cursor: <class 'pymysql.cursors.DictCursor'>
    
    :parameter query: SQL查询语句
    :type query: <class 'str'>
    
    :return: results: MySQL查询结果字典列表
    :rtype: <class 'list'>
    
    :exception: exception: 查询错误
    :type: <class 'Exception'>
    """
    
    try:
        cursor.execute(query)  # 执行SQL语句进行查询操作
        results = cursor.fetchall()  # 获取游标所有结果
        print_log('Retrieve Data Successfully! ')
        
        return results
    except Exception as exception:
        print_traceback_error(exception, 'Retrieve')
        
        return None



def delete(connection, cursor, operation, sql, value=None):
    """
    MySQL删除
    
    :parameter connection: MySQL连接对象
    :type connection: <class 'pymysql.connections.Connection'>
    
    :parameter cursor: MySQL游标对象
    :type cursor: <class 'pymysql.cursors.DictCursor'>
    
    :parameter operation: MySQL删除操作类型
    :type operation: <class ‘str'>
    
    :parameter sql: SQL删除语句
    :type sql: <class 'str'>
    
    :parameter value: MySQL删除数据列表，默认为None
    :type value: <class 'list'>
    
    :raise: exception: 删除操作类型错误
    :type: <class 'Exception'>
    
    :exception: exception: 删除错误
    :type: <class 'Exception'>
    """
    
    try:
        # 判断删除操作类型
        if operation == 'drop' or operation == 'Drop' or operation == 'DROP':
            cursor.execute(sql)  # 执行SQL语句进行（结构性）删除操作
            connection.commit()
            print_log('Delete Successfully! ')
        elif operation == 'delete' or operation == 'Delete' or operation == 'DELETE':
            cursor.execute(sql, value)  # 执行SQL语句进行（内容性）删除操作
            connection.commit()
            print_log('Delete Data Successfully! ')
        else:
            raise Exception('Delete Operation ERROR! ')
    except Exception as exception:
        print_traceback_error(exception, 'Delete')
        connection.rollback()



def mysql_close(connection, cursor):
    """
    MySQL关闭连接
    
    :parameter connection: MySQL连接对象
    :type connection: <class 'pymysql.connections.Connection'>
    
    :parameter cursor: MySQL游标对象
    :type cursor: <class 'pymysql.cursors.DictCursor'>
    
    :exception: exception: MySQL连接关闭错误
    :type: <class 'Exception'>
    """
    
    try:
        if connection:
            if cursor:
                cursor.close()  # 关闭MySQL游标
                connection.close()  # 关闭MySQL连接
                print_log('MySQL Connection Close Successfully! ')
            else:
                connection.close()
                print_log('MySQL Connection Close Successfully! ')
    except Exception as exception:
        print_traceback_error(exception, 'MySQL Connection Close')



def delete_all_provinces():
    """
    删除所有省份数据
    """
    
    connection, cursor = mysql_initialization(host=HOST, port=PORT, username=USERNAME, password='qwaszx123',
                                              database='raingod')
    print_log('Deleting All Provinces...... ')
    sql = 'DELETE FROM Province;'
    delete(connection, cursor, 'delete', sql)
    print_log('Delete All Provinces Successfully! ')
    mysql_close(connection, cursor)



def update_province(province):
    """
    更新省份数据
    
    :parameter province: 省份名
    :type province: <class 'str'>
    """
    
    connection, cursor = mysql_initialization(host=HOST, port=PORT, username=USERNAME, password='qwaszx123',
                                              database='raingod')
    print_log('Updating Province Data...... ')
    sql = 'INSERT INTO Province(province) VALUES (%s);'
    insert_data = [(province)]
    update(connection, cursor, 'insert', sql, insert_data)
    print_log('Update Province Data Successfully! ')
    mysql_close(connection, cursor)



def query_all_provinces():
    """
    查询所有省份数据
    
    :return: results: MySQL查询结果字典列表
    :rtype: <class 'list'>
    """
    
    connection, cursor = mysql_initialization(host=HOST, port=PORT, username=USERNAME, password='qwaszx123',
                                              database='raingod')
    print_log('Retrieving All Provinces...... ')
    query = 'SELECT * FROM Province;'
    results = retrieve(connection, cursor, query)
    print_log('Retrieve ALl Provinces Successfully! ')
    mysql_close(connection, cursor)
    
    return results



def delete_all_city():
    """
    删除所有城市数据
    """
    
    connection, cursor = mysql_initialization(host=HOST, port=PORT, username=USERNAME, password='qwaszx123',
                                              database='raingod')
    print_log('Deleting All Cities...... ')
    sql = 'DELETE FROM City;'
    delete(connection, cursor, 'delete', sql)
    print_log('Delete All Cities Successfully! ')
    mysql_close(connection, cursor)



def update_city(province, city):
    """
    更新城市数据
    
    :parameter province: 省份名
    :type province: <class 'str'>
    
    :parameter city: 城市名
    :type city: <class 'str'>
    """
    
    connection, cursor = mysql_initialization(host=HOST, port=PORT, username=USERNAME, password='qwaszx123',
                                              database='raingod')
    print_log('Updating City Data...... ')
    sql = 'INSERT INTO City(province,city) VALUES (%s,%s);'
    insert_data = [(province, city)]
    update(connection, cursor, 'insert', sql, insert_data)
    print_log('Update City Data Successfully! ')
    mysql_close(connection, cursor)



def query_city(province):
    """
    查询城市数据
    
    :parameter province: 省份名
    :type province: <class 'str'>
    
    :return: results: MySQL查询结果字典列表
    :rtype: <class 'list'>
    """
    
    connection, cursor = mysql_initialization(host=HOST, port=PORT, username=USERNAME, password='qwaszx123',
                                              database='raingod')
    print_log('Retrieving City Data......')
    query = 'SELECT * FROM City WHERE province="%s";' % province
    results = retrieve(connection, cursor, query)
    print_log('Retrieve City Data Successfully! ')
    mysql_close(connection, cursor)
    
    return results



def delete_all_real_time_weathers():
    """
    删除所有实时天气数据
    """
    
    connection, cursor = mysql_initialization(host=HOST, port=PORT, username=USERNAME, password='qwaszx123',
                                              database='raingod')
    print_log('Deleting All Real Time Weathers...... ')
    sql = 'DELETE FROM RealTimeWeather;'
    delete(connection, cursor, 'delete', sql)
    print_log('Delete All Real Time Weathers Successfully! ')
    mysql_close(connection, cursor)



def update_real_time_weather(province, city, real_time_weather):
    """
    更新实时天气数据
    
    :parameter province: 省份名
    :type province: <class 'str'>
    
    :parameter city: 城市名
    :type city: <class 'str'>
    
    :parameter real_time_weather: 实时天气类对象
    :type real_time_weather: <class 'weather.RealTimeWeather'>
    """
    
    connection, cursor = mysql_initialization(host=HOST, port=PORT, username=USERNAME, password='qwaszx123',
                                              database='raingod')
    print_log('Updating Real Time Weather Data...... ')
    sql = 'INSERT INTO RealTimeWeather(province,city,publish_time,temperature,precipitation,wind_direction,' \
          'wind_power,relative_humidity,sensible_temperature,aqi,comfort) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
    insert_data = [(province, city, real_time_weather.get_publish_time(), real_time_weather.get_temperature(),
                    real_time_weather.get_precipitation(), real_time_weather.get_wind_direction(),
                    real_time_weather.get_wind_power(), real_time_weather.get_relative_humidity(),
                    real_time_weather.get_sensible_temperature(), real_time_weather.get_aqi(),
                    real_time_weather.get_comfort())]
    update(connection, cursor, 'insert', sql, insert_data)
    print_log('Update Real Time Weather Data Successfully! ')
    mysql_close(connection, cursor)



def query_real_time_weather(province, city):
    """
    查询实时天气数据
    
    :parameter province: 省份名
    :type province: <class 'str'>
    
    :parameter city: 城市名
    :type city: <class 'str'>
    
    :return: results: MySQL查询结果字典列表
    :rtype: <class 'list'>
    """
    
    connection, cursor = mysql_initialization(host=HOST, port=PORT, username=USERNAME, password='qwaszx123',
                                              database='raingod')
    print_log('Retrieving Real Time Weather Data...... ')
    query = 'SELECT * FROM RealTimeWeather WHERE province="%s" AND city="%s";' % (province, city)
    results = retrieve(connection, cursor, query)
    print_log('Retrieve Real Time Weather Data Successfully! ')
    mysql_close(connection, cursor)
    
    return results



def delete_all_date_weathers():
    """
    删除所有日期天气数据
    """
    
    connection, cursor = mysql_initialization(host=HOST, port=PORT, username=USERNAME, password='qwaszx123',
                                              database='raingod')
    print_log('Deleting All Date Weathers...... ')
    sql = 'DELETE FROM DateWeather;'
    delete(connection, cursor, 'delete', sql)
    print_log('Delete All Date Weathers Successfully! ')
    mysql_close(connection, cursor)



def update_date_weather(province, city, date_weathers):
    """
    更新日期天气数据
    
    :parameter province: 省份名
    :type province: <class 'str'>
    
    :parameter city: 城市名
    :type city: <class 'str'>
    
    :parameter date_weathers: 日期天气类对象列表
    :type date_weathers: <class 'list'>
    """
    
    connection, cursor = mysql_initialization(host=HOST, port=PORT, username=USERNAME, password='qwaszx123',
                                              database='raingod')
    print_log('Updating Date Weather Data...... ')
    sql = 'INSERT INTO DateWeather(province,city,date_time,highest_temperature,lowest_temperature,' \
          'weather_description1,weather_description2,wind_direction1,wind_direction2,wind_level1,wind_level2) VALUES ' \
          '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
    insert_data = []
    
    for i in date_weathers:
        insert_data.append((province, city, i.get_date(), i.get_highest_temperature(), i.get_lowest_temperature(),
                            i.get_weather_description1(), i.get_weather_description2(), i.get_wind_direction1(),
                            i.get_wind_direction2(), i.get_wind_level1(), i.get_wind_level2()))
    
    update(connection, cursor, 'insert', sql, insert_data)
    print_log('Update Date Weather Data Successfully! ')
    mysql_close(connection, cursor)



def query_date_weather(province, city):
    """
    查询日期天气数据
    
    :parameter province: 省份名
    :type province: <class 'str'>
    
    :parameter city: 城市名
    :type city: <class 'str'>
    
    :return: results: MySQL查询结果字典列表
    :rtype: <class 'list'>
    """
    
    connection, cursor = mysql_initialization(host=HOST, port=PORT, username=USERNAME, password='qwaszx123',
                                              database='raingod')
    print_log('Retrieving Date Weather Data...... ')
    query = 'SELECT * FROM DateWeather WHERE province="%s" AND city="%s" ORDER BY date_time;' % (province, city)
    results = retrieve(connection, cursor, query)
    print_log('Retrieve Date Weather Data Successfully! ')
    mysql_close(connection, cursor)
    
    return results



def delete_all_weathers_data():
    """
    删除所有气象数据
    """
    
    print_log('Deleting All Weather Data...... ')
    delete_all_real_time_weathers()
    delete_all_date_weathers()
    print_log('Delete All Weather Data Successfully! ')



def update_all_weathers_data(province, city, real_time_weather, date_weathers):
    """
    更新所有气象数据
    
    :parameter province: 省份名
    :type province: <class 'str'>
    
    :parameter city: 城市名
    :type city: <class 'str'>
    
    :parameter real_time_weather: 实时天气类对象
    :type real_time_weather: <class 'weather.RealTimeWeather'>
    
    :parameter date_weathers: 日期天气类对象列表
    :type date_weathers: <class 'list'>
    """
    
    print_log('Updating All Weather Data...... ')
    update_real_time_weather(province, city, real_time_weather)
    update_date_weather(province, city, date_weathers)
    print_log('Update All Weather Data Successfully! ')



def query_all_weathers_data(province, city):
    """
    查询所有气象数据
    
    :parameter province: 省份名
    :type province: <class 'str'>
    
    :parameter city: 城市名
    :type city: <class 'str'>
    
    :returns
    
    :return: real_time_weather_results: MySQL查询结果字典列表
    :rtype: <class 'list'>
    
    :return: date_weathers_results: MySQL查询结果字典列表
    :rtype: <class 'list'>
    """
    
    print_log('Retrieving All Weather Data...... ')
    real_time_weather_results = query_real_time_weather(province, city)
    date_weathers_results = query_date_weather(province, city)
    print_log('Retrieve All Weather Data Successfully! ')
    
    return real_time_weather_results, date_weathers_results



if __name__ == '__main__':
    connection, cursor = mysql_initialization(host='localhost', username='root', password='qwaszx123',
                                              database='raingod')
    
    sql_drop = 'DROP TABLE IF EXISTS test;'
    delete(connection, cursor, 'drop', sql_drop)
    
    sql_create = '''
    CREATE TABLE IF NOT EXISTS test
    (
    test_id CHAR(5) NOT NULL,
    test_name VARCHAR(50) NOT NULL
    )
    DEFAULT CHARSET=utf8mb4;
    '''
    create(connection, cursor, sql_create)
    
    query_all = 'SELECT * FROM test;'
    results = retrieve(connection, cursor, query_all)
    
    for i in results:
        print(i)
    
    sql_insert = 'INSERT INTO test(test_id,test_name) VALUES (%s,%s);'
    insert_data = [('1', 'a'), ('2', 'b'), ('3', 'c')]
    update(connection, cursor, 'insert', sql_insert, insert_data)
    
    results = retrieve(connection, cursor, query_all)
    
    for i in results:
        print(i)
    
    sql_update = 'UPDATE test SET test_name=%s WHERE test_id=%s;'
    update_data = ['B', '2']
    update(connection, cursor, 'update', sql_update, update_data)
    
    results = retrieve(connection, cursor, query_all)
    
    for i in results:
        print(i)
    
    sql_delete = 'DELETE FROM test WHERE test_id=%s;'
    delete_data = ['3']
    delete(connection, cursor, 'delete', sql_delete, delete_data)
    
    results = retrieve(connection, cursor, query_all)
    
    for i in results:
        print(i)
    
    query_one = 'SELECT * FROM test WHERE test_id=%s;' % '1'
    results = retrieve(connection, cursor, query_one)
    
    for i in results:
        print(i)
    
    delete(connection, cursor, 'drop', sql_drop)
    
    mysql_close(connection, cursor)
