import json

import requests
from redis import StrictRedis

from .base_msg_handler import BaseMsgHandler
from config.configInit import config_dict


class ShopStatusHandler(BaseMsgHandler):
    
    def on_message(self, basic_deliver, body):
        self.main_consumer_obj.logger.info(body)
        if body['funcName'] != 'updateShopArea':
            return self.main_consumer_obj.acknowledge_message(basic_deliver.delivery_tag)
        redis_conf = config_dict['redis']
        redis_conf['db'] = int(redis_conf['db'])
        redis_conn = StrictRedis(**redis_conf)
        pl = redis_conn.pipeline()
        # 考虑到可能会有小区不存在mysql的情况所以选用insert语句
        insert_sql = '''
            INSERT INTO cb_area_shop_list(
                area_code,
                shop_code_list 
            )
            VALUES(%s, %s) 
            ON DUPLICATE KEY UPDATE
                shop_code_list = VALUES(shop_code_list)
        '''
        insert_param_li = []
        for shop_code, area_code_li in body['data'].items():
            for area_code in area_code_li:
                url = (config_dict['spring_cloud']['url'] +
                       '/xwj-commerce-shop/shopManage/getShopListByArea?areaCode={}&flag=4')  # 线上用
                # https: // testxwj.juhaolian.cn/xwj-commerce-shop/shopManage/getShopListByArea?areaCode={}
                # url = 'http://10.18.222.105:6200/shopManage/getShopListByArea?areaCode={}'  # 联调用
                res = requests.get(url.format(area_code))
                data = json.loads(res.text)['data']
                shop_code_list = [shop_info['shopCode'] for shop_info in data]
                shop_count = len(shop_code_list)
                self.main_consumer_obj.logger.info('给小区{}更新，获取到{}家商铺，商铺列表：{}'.format(
                    area_code, shop_count, shop_code_list
                ))
                # 无论什么操作都是查询接口然后批量更新mysql
                insert_param_li.append((area_code, ','.join(shop_code_list)))
                # 以下是对redis的操作
                area_key = area_code + '_shop'
                # TODO: 下面这两个判断移动到循环外以提高效率
                # 删除店铺
                if int(body['cmd']) == 0:
                    pl.lrem(area_key, 0, shop_code)
                # 添加店铺
                else:
                    pl.delete(area_key)
                    if shop_count:
                        pl.rpush(area_key, *shop_code_list)
        sqyn_conn = self.get_mysql_conn('mysql_sqyn')
        with sqyn_conn.cursor() as cur:
            try:
                cur.executemany(insert_sql, insert_param_li)
                sqyn_conn.commit()
            except:
                self.main_consumer_obj.logger.exception('商铺可见范围mysql变更失败, sendTime: {}'.format(body['sendTime']))
                sqyn_conn.rollback()
            else:
                self.main_consumer_obj.logger.info('商铺可见范围mysql变更成功, sendTime: {}'.format(body['sendTime']))
            finally:
                sqyn_conn.close()
        try:
            pl.execute()
        except:
            self.main_consumer_obj.logger.exception('商铺可见范围redis变更失败, sendTime: {}'.format(body['sendTime']))
        else:
            self.main_consumer_obj.logger.info('商铺可见范围redis变更成功, sendTime: {}'.format(body['sendTime']))
        finally:
            pl.close()
            redis_conn.close()
        self.main_consumer_obj.acknowledge_message(basic_deliver.delivery_tag)
