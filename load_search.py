#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time      :2020/8/26 15:25
# @Author    :XieYuHao


from pymysql import connect
from config.configInit import get_conf_dict


def get_mysql_conn(name):
    conf_dict = get_conf_dict('D:\workbench\cloudbrain-preprocess-python\config\config.ini')
    return connect(**conf_dict[name])

hi_conn = get_mysql_conn()