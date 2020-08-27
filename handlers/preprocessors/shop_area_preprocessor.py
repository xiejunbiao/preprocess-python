import json
from redis import StrictRedis
from pymysql import connect
import requests


class ShopAreaPreprocessor:
    def __init__(self, conf, logger):
        self.__conf = conf
        self.__logger = logger

    def run(self):
        redis_conf = self.__conf['redis']
        redis_conf['db'] = int(redis_conf['db'])
        redis_conn = StrictRedis(**redis_conf)
        # 在导入商铺数据到redis前存入一个标志表明正在初始化，以防重复执行
        # loading_shop_flag_key = 'loading_shop_flag'
        # redis_conn.set(loading_shop_flag_key, 1)
        pl = redis_conn.pipeline()
        self.__logger.info('开始同步小区下商铺数据')
        sqyn_sql_conn = self.__conf['mysql_sqyn']
        mysql_conn = connect(**sqyn_sql_conn)
        # 查出所有小区
        select_sql = '''
            SELECT DISTINCT
                area_code 
            FROM
                cb_goods_scope
        '''
        with mysql_conn.cursor() as cur:
            cur.execute(select_sql)
            area_list = [row_tuple[0] for row_tuple in cur.fetchall()]
        # 循环areaCode请求其他接口获得小区下可见的shopCode
        url = self.__conf['spring_cloud']['url'] + '/xwj-commerce-shop/shopManage/getShopListByArea?areaCode={}&flag=4'  # 线上用
        # url = 'http://10.18.222.105:6200/shopManage/getShopListByArea?areaCode={}&flag=4'  # 联调用
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
        for area_code in area_list:
            res = requests.get(url.format(area_code))
            data = json.loads(res.text)['data']
            shop_code_list = [shop_info['shopCode'] for shop_info in data]
            shop_count = len(shop_code_list)
            self.__logger.info('小区{}获取到{}家商铺'.format(area_code, shop_count))
            insert_param_li.append((area_code, ','.join(shop_code_list)))
            pl.delete(area_code + '_shop')
            if shop_count:
                pl.rpush(area_code + '_shop', *shop_code_list)
        # 导入完毕删掉标志位的key
        # pl.delete(loading_shop_flag_key)
        with mysql_conn.cursor() as cur:
            try:
                cur.executemany(insert_sql, insert_param_li)
                mysql_conn.commit()
            except:
                self.__logger.exception('小区商铺数据同步到mysql异常')
                mysql_conn.rollback()
            else:
                self.__logger.info('小区商铺数据同步到mysql成功')
            finally:
                mysql_conn.close()
        try:
            pl.execute()
        except:
            self.__logger.exception('小区商铺数据同步到redis异常')
        else:
            self.__logger.info('小区商铺数据同步到redis成功')
        finally:
            pl.close()
            redis_conn.close()