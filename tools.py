#!usr/bin/env python
# -*- coding: utf-8 -*-

"""
tools.py
Define Some Tool Functions.
1. print_log()
2. print_traceback_error()
"""

from traceback import format_exc
from time import strftime
from sys import argv



def print_log(string):
    """
    打印字符串到控制台
    
    :parameter string: 将在控制台输出的字符串
    :type string: <class 'str'>
    """
    
    print(strftime("%Y-%m-%d %H:%M:%S") + "\t" + string)



def print_traceback_error(exception, string):
    """
    打印异常信息到控制台
    
    :parameter exception: 捕捉到的异常
    :type exception: <class 'Exception'>
    
    :parameter string: 将在控制台输出的异常/错误类型
    :type string: <class 'str'>
    """
    
    error = format_exc()
    error_time = strftime("%Y-%m-%d %H:%M:%S")
    command_line_entry = " ".join(argv)
    
    print(error_time + "\t" + string + " ERROR! ", end="")
    print(exception)
    print(command_line_entry + "\n" + error)
