#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time      :2020/8/5 9:10
# @Author    :XieYuHao

from .base_msg_handler import BaseMsgHandler


class UserEvalHandler(BaseMsgHandler):

    def on_message(self, basic_deliver, body):
        self.main_consumer_obj.logger.info(body)
        if body['funcName'] != 'userEvalChange':
            return self.main_consumer_obj.acknowledge_message(basic_deliver.delivery_tag)
        hisense_conn = self.get_mysql_conn('mysql_hisense')
        sqyn_conn = self.get_mysql_conn('mysql_sqyn')
        affected_eval_no_li = body['data']
        # 删除评论
        if int(body['cmd']) == 0:
            delete_sql = '''
                DELETE
                FROM
                    cb_goods_spu_eval
                WHERE
                    eval_no = %s
            '''
            with sqyn_conn.cursor() as cur:
                try:
                    cur.executemany(delete_sql, affected_eval_no_li)
                    sqyn_conn.commit()
                except:
                    self.main_consumer_obj.logger.exception('{}评论删除异常, sendTime: {}'.format(affected_eval_no_li, body['sendTime']))
                    hisense_conn.rollback()
                else:
                    self.main_consumer_obj.logger.info('{}评论删除成功, sendTime: {}'.format(affected_eval_no_li, body['sendTime']))
        # 修改或新增评论
        else:
            # 到hisense库获取所有新增或修改的评论相关信息
            select_sql = '''
                SELECT
                    eval_no,
                    spu_code,
                    owner_code,
                    order_code,
                    eval_level,
                    eval_time,
                    eval_content,
                    shop_code 
                FROM
                    goods_spu_eval
                WHERE
                    eval_no IN %s
            '''
            with hisense_conn.cursor() as cur:
                cur.execute(select_sql, [affected_eval_no_li])
                insert_params = cur.fetchall()
            # 去sqyn库insert或update数据
            insert_sql = '''
                INSERT INTO cb_goods_spu_eval(
                    eval_no,
                    spu_code,
                    owner_code,
                    order_code,
                    eval_level,
                    eval_time,
                    eval_content,
                    shop_code 
                 )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
            '''
            with sqyn_conn.cursor() as cur:
                try:
                    cur.executemany(insert_sql, insert_params)
                    sqyn_conn.commit()
                except:
                    self.main_consumer_obj.logger.exception('{}评论添加异常, sendTime: {}'.format(affected_eval_no_li, body['sendTime']))
                    sqyn_conn.rollback()
                else:
                    self.main_consumer_obj.logger.info('{}评论添加成功, sendTime: {}'.format(affected_eval_no_li, body['sendTime']))
        sqyn_conn.close()
        hisense_conn.close()
        self.main_consumer_obj.acknowledge_message(basic_deliver.delivery_tag)
