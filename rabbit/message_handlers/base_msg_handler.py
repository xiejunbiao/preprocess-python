#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time      :2020/8/3 17:30
# @Author    :XieYuHao
import json
import threading
import time

from config.configInit import config_dict

from pymysql import connect


class BaseMsgHandler(object):
    def __init__(self):
        self.message_queue = []
        self.main_consumer_obj = None

    def append_message(self, consumer_obj, unused_channel, basic_deliver, properties, body):
        if not self.main_consumer_obj:
            self.main_consumer_obj = consumer_obj
        self.message_queue.append({
            'basic_deliver': basic_deliver,
            'body': json.loads(body)
        })

    def start_consuming(self):
        t1 = threading.Thread(target=self.consume_loop)
        t1.start()

    def consume_loop(self):
        while True:
            time.sleep(0.5)
            if self.message_queue:
                self.on_message(**self.message_queue.pop(0))

    def on_message(self, basic_deliver, body):
        pass

    @staticmethod
    def get_mysql_conn(mysql_name):
        mysql_conf = config_dict[mysql_name]
        return connect(**mysql_conf)

