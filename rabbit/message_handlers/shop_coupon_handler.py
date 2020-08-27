#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time      :2020/8/13 21:47
# @Author    :XieYuHao

from .base_msg_handler import BaseMsgHandler


class ShopCouponHandler(BaseMsgHandler):

    def on_message(self, basic_deliver, body):
        # d1 = {"sendTime": "2020-08-14 09:54:00",
        #       "funcName": "userCouponChange",
        #       "cmd": "1",
        #       "data": ["454068526533967872D1"]}
        self.main_consumer_obj.logger.info(body)
        if body['funcName'] != 'userCouponChange':
            return self.main_consumer_obj.acknowledge_message(basic_deliver.delivery_tag)
        sqyn_conn = self.get_mysql_conn('mysql_sqyn')
        hisense_conn = self.get_mysql_conn('mysql_hisense')
        select_sql = '''
            SELECT
                coupon.id,
                coupon.type,
                coupon.eff_type,
                coupon.STATUS,
                coupon.offer_cnt,
                coupon.pickup_cnt,
                scope.area_code,
                IFNULL(shop.shop_code, 0) 
            FROM
                coupon_offer AS coupon
                INNER JOIN coupon_scope AS scope ON coupon.id = scope.coupon_offer_id
                LEFT JOIN coupon_shop AS shop ON coupon.id = shop.coupon_offer_id
            WHERE 
                coupon.id IN %s
        '''
        with hisense_conn.cursor() as cur:
            cur.execute(select_sql, [body['data']])
            insert_params = cur.fetchall()
        insert_sql = '''
            INSERT INTO cb_shop_coupon(
                coupon_id,
                coupon_type,
                eff_type,
                status,
                offer_cnt,
                pickup_cnt,
                area_code,
                shop_code 
            )
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                coupon_type = VALUES(coupon_type),
                eff_type = VALUES(eff_type),
                status = VALUES(status),
                offer_cnt = VALUES(offer_cnt),
                pickup_cnt = VALUES(pickup_cnt)
        '''
        with sqyn_conn.cursor() as cur:
            try:
                cur.executemany(insert_sql, insert_params)
                sqyn_conn.commit()
            except:
                self.main_consumer_obj.logger.exception(f"优惠券修改异常, sendTime: {body['sendTime']}")
                sqyn_conn.rollback()
            else:
                self.main_consumer_obj.logger.info(f"优惠券修改成功, sendTime: {body['sendTime']}")
        sqyn_conn.close()
        hisense_conn.close()
        self.main_consumer_obj.acknowledge_message(basic_deliver.delivery_tag)
