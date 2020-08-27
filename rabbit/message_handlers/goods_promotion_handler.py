#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time      :2020/8/13 21:48
# @Author    :XieYuHao

from .base_msg_handler import BaseMsgHandler


class GoodsPromotionHandler(BaseMsgHandler):

    def on_message(self, basic_deliver, body):
        # d1 = {"sendTime": "2020-08-14 09:54:00",
        #       "funcName": "userCouponChange",
        #       "cmd": "ADD",
        #       "data": '460048265535029248'}
        self.main_consumer_obj.logger.info(f'促销活动变更{body}')
        # if body['funcName'] != 'userCouponChange':
        #     return self.main_consumer_obj.acknowledge_message(basic_deliver.delivery_tag)
        sqyn_conn = self.get_mysql_conn('mysql_sqyn')
        hisense_conn = self.get_mysql_conn('mysql_hisense')
        act_id = body['data']  # 促销活动Id
        send_time = body['sendTime']
        with sqyn_conn.cursor() as s_cur:
            # 总是要先删除，因为活动id下可能会取消某些商品
            delete_sql = '''
                DELETE 
                FROM
                    cb_goods_promotion 
                WHERE
                    act_code = %s
            '''
            try:
                count = s_cur.execute(delete_sql, act_id)
                sqyn_conn.commit()
            except:
                self.main_consumer_obj.logger.exception(f"促销活动删除异常, sendTime: {send_time}")
                sqyn_conn.rollback()
            else:
                self.main_consumer_obj.logger.info(f"促销活动删除成功, 删除{count}条数据, sendTime: {send_time}")
            # 如果是新增或是修改操作需要再次查询hisense并插入
            if body['cmd'] != 'CANCEL':
                with hisense_conn.cursor() as h_cur:
                    select_sql = '''
                        SELECT
                            p.act_code,
                            p.begin_time,
                            p.end_time,
                            p_scope.shop_code,
                            p_scope.area_code,
                            sku.spu_code 
                        FROM
                            act_promotion AS p
                            INNER JOIN act_promotion_scope AS p_scope ON p.act_code = p_scope.act_code
                            INNER JOIN act_promotion_sku AS p_sku ON p_scope.act_code = p_sku.act_code
                            INNER JOIN goods_sku AS sku ON p_sku.sku_no = sku.sku_no
                        WHERE
                            TO_DAYS(p.end_time) >= TO_DAYS(NOW())
                            AND p.is_valid = 1
                            AND p.act_code = %s
                    '''
                    count = h_cur.execute(select_sql, act_id)
                    insert_params = h_cur.fetchall()
                    self.main_consumer_obj.logger.info(f"查询出{count}条数据")
                insert_sql = '''
                    INSERT INTO cb_goods_promotion(
                        act_code,
                        begin_time,
                        end_time,
                        shop_code,
                        area_code,
                        spu_code
                    )
                    VALUES(%s, %s, %s, %s, %s, %s)
                '''
                try:
                    s_cur.executemany(insert_sql, insert_params)
                    sqyn_conn.commit()
                except:
                    self.main_consumer_obj.logger.exception(f"促销活动修改异常，sendTime:{send_time}")
                    sqyn_conn.rollback()
                else:
                    self.main_consumer_obj.logger.info(f"促销活动修改成功，sendTime:{send_time}")
        sqyn_conn.close()
        hisense_conn.close()
        self.main_consumer_obj.acknowledge_message(basic_deliver.delivery_tag)




 
