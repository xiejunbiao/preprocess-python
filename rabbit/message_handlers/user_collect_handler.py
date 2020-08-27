from .base_msg_handler import BaseMsgHandler


class UserCollectHandler(BaseMsgHandler):

    def on_message(self, basic_deliver, body):
        self.main_consumer_obj.logger.info(body)
        if body['funcName'] != 'updateOwnerCollectionChange':
            return self.main_consumer_obj.acknowledge_message(basic_deliver.delivery_tag)
        mysql_conn = self.get_mysql_conn('mysql_sqyn')
        if int(body['cmd']) == 1:  # 收藏
            insert_sql = '''
                INSERT INTO cb_user_collect_record(
                    owner_code,
                    collect_type,
                    content_code,
                    created_time
                    )
                VALUES(%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    created_time = VALUES(created_time)
            '''
            data = body['data']
            with mysql_conn.cursor() as cur:
                try:
                    cur.execute(insert_sql, (
                        data['userCode'],
                        data['collectType'],
                        data['contentCode'],
                        body['sendTime']
                    ))
                    mysql_conn.commit()
                except:
                    self.main_consumer_obj.logger.exception('用户{}收藏异常，sendTime:{}'.format(data['userCode'], body['sendTime']))
                    mysql_conn.rollback()
                else:
                    self.main_consumer_obj.logger.info('用户{}收藏成功，sendTime:{}'.format(data['userCode'], body['sendTime']))
        else:  # 取消收藏
            delete_sql = '''
                DELETE 
                FROM
                    cb_user_collect_record 
                WHERE
                    owner_code = %s
                    AND content_code = %s
            '''
            delete_params = [(collect_info['userCode'], collect_info['contentCode']) for collect_info in body['data']]
            with mysql_conn.cursor() as cur:
                try:
                    cur.executemany(delete_sql, delete_params)
                    mysql_conn.commit()
                except:
                    self.main_consumer_obj.logger.exception('取消收藏异常，sendTime:{}'.format(body['sendTime']))
                    mysql_conn.rollback()
                else:
                    self.main_consumer_obj.logger.info('取消收藏成功, sendTime:{}'.format(body['sendTime']))
        self.main_consumer_obj.acknowledge_message(basic_deliver.delivery_tag)
        mysql_conn.close()
