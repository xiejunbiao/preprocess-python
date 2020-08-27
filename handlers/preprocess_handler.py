import threading

import tornado.web
from pymysql import connect

from handlers.preprocessors.shop_area_preprocessor import ShopAreaPreprocessor


class PreproessHandler(tornado.web.RequestHandler):

    def initialize(self, logger, conf):
        self.__logger = logger
        self.__conf = conf
        hisense_mysql_conf = conf['mysql_hisense']
        sqyn_mysql_conf = conf['mysql_sqyn']
        self.hisense_conn = connect(**hisense_mysql_conf)
        self.sqyn_conn = connect(**sqyn_mysql_conf)

    def get(self):
        t1 = threading.Thread(target=self.start_preprocess)
        t1.start()

    def start_preprocess(self):
        ShopAreaPreprocessor(self.__conf, self.__logger).run()  # 商铺
        self.preprocess_user_collect_records('cb_user_collect_record')  # 收藏
        self.preprocess_goods_spu_eval('cb_goods_spu_eval')  # 评论
        self.preprocess_shop_coupon('cb_shop_coupon')  # 优惠券
        self.preprocess_goods_promotion('cb_goods_promotion')  # 促销
        self.preprocess_guess_you_like('cb_guesslike_owner_rec_goods')  # 猜你喜欢冷启动用户通用数据（销量排序）
        self.hisense_conn.close()
        self.sqyn_conn.close()

    # 猜你喜欢预置冷启动用户数据
    def preprocess_guess_you_like(self, sqyn_table):
        self.__logger.info('开始写入猜你喜欢冷启动用户数据')
        cate_dict = {
            '000_01': ["车厘子莓类", "柑橘橙柚", "苹果梨", "葡萄提子", "蜜瓜西瓜",
                       "桃", "绿叶菜", "茄果瓜果", "根茎类", "菌菇类"],  # 时令
            '000_02': ["鸡蛋禽蛋", "冷冻水产", "牛肉", "猪肉", "羊肉",
                       "海鲜半成品", "鸡鸭鸽", "牛排", "调味肉制品", "火锅烧烤"],  # 三餐
            '000_03': ["酸奶乳酸菌", "蛋糕点心", "下午茶", "薯片膨化", "巧克力", "坚果炒货",
                       "糕点甜品", "糖果果冻", "咖啡茶饮", "冲调谷物", "果蔬汁", "碳酸功能"],  # 休闲
            '000_04': ["衣物清洁", "家居日用", "头发护理", "口腔护理", "婴童食品", "婴童护理", "纸制品", "宠物用品"]  # 日用
        }
        with self.hisense_conn.cursor() as cur:
            for cate_tab, cate_name_li in cate_dict.items():
                # hisense库查询分类名对应的code
                select_sql = '''
                    SELECT
                        cate_code 
                    FROM
                        goods_cate
                    WHERE
                        cate_name IN %s
                        AND cate_level=2
                '''
                cur.execute(select_sql, [cate_name_li])
                cate_dict[cate_tab] = [cate_code_tuple[0] for cate_code_tuple in cur.fetchall()]
        with self.sqyn_conn.cursor() as cur:
            for cate_tab, cate_code_li in cate_dict.items():
                # sqyn库按销量查询每个类别下的商品写入到猜你喜欢表
                select_sql = '''
                    SELECT
                        filter.spu_code 
                    FROM
                        cb_goods_spu_for_filter AS filter
                        INNER JOIN cb_goods_spu_search AS search ON filter.spu_code = search.spu_code                   
                    WHERE
                        second_cate IN %s
                        AND search.shop_type_code = 1
                    ORDER BY
                        sale_month_count DESC
                '''
                cur.execute(select_sql, [cate_code_li])
                cate_dict[cate_tab] = ','.join([categ_code_tuple[0] for categ_code_tuple in cur.fetchall()])
            insert_sql = '''
                INSERT INTO {}(
                    owner_area_tab_code,
                    spu_code_list,
                    redis_index,
                    has_next_record,
                    total_num
                )
                VALUES(%s, %s, 1, 0, 0) 
                ON DUPLICATE KEY UPDATE
                    spu_code_list = VALUES(spu_code_list)
            '''.format(sqyn_table)
            try:
                cur.executemany(insert_sql, list(cate_dict.items()))
                self.sqyn_conn.commit()
            except:
                self.__logger.exception(f'{sqyn_table}写入数据异常')
                self.sqyn_conn.rollback()
            else:
                self.__logger.info(f'{sqyn_table}写入数据成功')

# 用户收藏
    def preprocess_user_collect_records(self, sqyn_table):
        select_sql = '''
            SELECT
                user_code,
                collect_type,
                content_code,
                created_time 
            FROM
                user_collect_record
            LIMIT %s, %s
        '''
        insert_sql = '''
            INSERT INTO {}(
                owner_code,
                collect_type,
                content_code,
                created_time
            )
            VALUES(%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                owner_code = VALUES(owner_code),
                collect_type = VALUES(collect_type),
                content_code = VALUES(content_code),
                created_time = VALUES(created_time)
        '''.format(sqyn_table)
        self.mysql_operation(select_sql, insert_sql, sqyn_table)

    # 用户评价
    def preprocess_goods_spu_eval(self, sqyn_table):
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
            LIMIT %s, %s
        '''
        insert_sql = '''
            INSERT INTO {}(
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
            ON DUPLICATE KEY UPDATE 
                spu_code = VALUES(spu_code),
                owner_code = VALUES(owner_code),
                order_code = VALUES(order_code),
                eval_level = VALUES(eval_level),
                eval_time = VALUES(eval_time),
                eval_content = VALUES(eval_content),
                shop_code = VALUES(shop_code) 
        '''.format(sqyn_table)
        self.mysql_operation(select_sql, insert_sql, sqyn_table)

    # 优惠券
    def preprocess_shop_coupon(self, sqyn_table):
        select_sql = """
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
                coupon.status = '02'
            LIMIT %s, %s
        """
        insert_sql = '''
            INSERT INTO {}(
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
        '''.format(sqyn_table)
        self.mysql_operation(select_sql, insert_sql, sqyn_table)

    # 商品促销
    def preprocess_goods_promotion(self, sqyn_table):
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
            LIMIT %s, %s
        '''
        insert_sql = '''
            INSERT INTO {}(
                act_code,
                begin_time,
                end_time,
                shop_code,
                area_code,
                spu_code
            )
            VALUES(%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                begin_time = VALUES(begin_time),
                end_time = VALUES(end_time),
                shop_code = VALUES(shop_code)
        '''.format(sqyn_table)
        self.mysql_operation(select_sql, insert_sql, sqyn_table)

    def mysql_operation(self, select_sql, insert_sql, sqyn_table):
        page = 1
        query_rows = 1000
        self.__logger.info(f'{sqyn_table}开始写入数据')
        while True:
            # 查询数据
            start_index = (page - 1) * query_rows
            with self.hisense_conn.cursor() as cur:
                count = cur.execute(select_sql, [start_index, query_rows])
                if not count:
                    self.__logger.info('没有查询到数据')
                    return
                self.__logger.info(f'第{page}页查询到{count}条数据')
                rows = cur.fetchall()
            # 写入数据
            with self.sqyn_conn.cursor() as cur:
                try:
                    cur.executemany(insert_sql, rows)
                    self.sqyn_conn.commit()
                except:
                    self.__logger.exception(f'{sqyn_table}写入数据异常')
                    self.sqyn_conn.rollback()
                    return
                else:
                    # 小于1000终止循环
                    if count < query_rows:
                        break
                    page += 1
        self.__logger.info(f'{sqyn_table}写入数据成功')
