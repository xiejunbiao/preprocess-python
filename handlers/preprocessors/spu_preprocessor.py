#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time      :2020/8/17 14:05
# @Author    :XieYuHao

import threading
import time

import tornado.web
from pymysql import connect
from redis import StrictRedis

from config.configInit import config_dict


class SpuPreprocessHandler(tornado.web.RequestHandler):
    def initialize(self, logger):
        self._logger = logger

    def get(self):
        t1 = threading.Thread(target=self.load_area_spu_to_redis)
        t1.start()

    # 把每个小区下的商品以销量排序加载到redis，服务启动时调用一次
    def load_area_spu_to_redis(self):
        self._logger.info('开始加载小区数据到redis...')
        redis_conf = config_dict['redis']
        redis_conn = StrictRedis(**redis_conf)
        pl = redis_conn.pipeline()
        flag_key = 'area_code_refresh_flag'
        # if redis_conn.exists(flag_key):
        #     self._logger.info('已有小区数据无需预加载')
        #     return
        redis_conn.setex(flag_key, 3600, 1)
        start_time = time.time()
        sqyn_mysql_conf = config_dict['mysql_sqyn']
        sqyn_conn = connect(**sqyn_mysql_conf)
        with sqyn_conn.cursor() as cursor:
            # 获取全量商品根据销量的排序
            select_sql = '''
                SELECT
                    scope.area_code,
                    scope.spu_code 
                FROM
                    cb_goods_spu_for_filter AS filter
                    INNER JOIN cb_goods_scope AS scope
                ON
                    filter.spu_code = scope.spu_code
                WHERE
                    filter.goods_status = 1 
                    AND filter.store_status = 1 
                    AND filter.spu_name NOT REGEXP "链接|差价" 
                ORDER BY
                    filter.sale_month_count DESC
            '''
            cursor.execute(select_sql)
            area_spu_dict = {}
            for area, spu in cursor.fetchall():
                area_key = area + '_A'
                spu_li = area_spu_dict.setdefault(area_key, [])
                spu_li.append(spu)
            for area_key, spu_li in area_spu_dict.items():
                self._logger.info(f'小区{area_key}下有{len(spu_li)}个商品')
                # 挨个地区更新，先删除再push
                pl.delete(area_key)
                pl.rpush(area_key, *spu_li)
            pl.execute()
        self._logger.info('加载小区商品到redis完成，耗时：' + str(time.time() - start_time))
        pl.close()
        sqyn_conn.close()
        redis_conn.close()

