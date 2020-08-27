import json
import functools

import requests
from pika.adapters.tornado_connection import TornadoConnection
import pika

from pymysql import connect
from redis import StrictRedis

from .message_handlers import message_handler_map

# LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
#               '-35s %(lineno) -5d: %(message)s')
# self.logger = logging.getLogger(__name__)


class TornadoConsumer(object):
    """This is an example consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """
    COLLECTION_EXCHANGE = 'XwjSysUserCollectionExchange'  # 店铺收藏的exchange
    EVAL_EXCHANGE = 'XwjCommerceSaleEvalExchange'  # 店铺评价的exchange
    SHOP_AREA_CHANGE_EXCHANGE = 'XwjCommerceShopStatusChangeExchange'  # 商铺可见范围变更的exchange
    SHOP_COUPON_HANDLER_EXCHANGE = 'XwjCommerceCouponChangeExchange'  # 店铺优惠券
    GOODS_PROMOTION_HANDLER_EXCHANGE = 'XwjCommercePromotionChangeExchange'  # 商品促销
    EXCHANGE_TYPE = 'fanout'   # 全部使用广播模式
    COLLECTION_CHANGE_QUEUE = 'CollectionChangeCloudbrainpreprocessQueue'  # 店铺收藏的queue
    USER_EVAL_CHANGE_QUEUE = 'EvalChangeCloudbrainpreprocessQueue'  # 店铺评价的queue
    SHOP_AREA_CHANGE_QUEUE = 'ShopStatusChangeCloudbrainpreprocessQueue'  # 商铺可见范围变更的queue
    SHOP_COUPON_HANDLER_QUEUE = 'CouponChangeCloudbrainpreprocessQueue'  # 店铺优惠券的queue
    GOODS_PROMOTION_HANDLER_QUEUE = 'PromotionChangeCloudbrainpreprocessQueue'  # 商品促销的queue

    # executor = ThreadPoolExecutor(5)

    def __init__(self, logger, conf):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        rabbit_conf = conf['rabbit']
        amqp_url = 'amqp://{}:{}@{}:{}/%2F'.format(
            rabbit_conf['user'],
            rabbit_conf['password'],
            rabbit_conf['host'],
            rabbit_conf['port']
        )
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url
        self.logger = logger
        self._conf = conf

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        self.logger.info('Connecting to %s', self._url)
        return TornadoConnection(pika.URLParameters(self._url), self.on_connection_open)

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        self.logger.info('Closing connection')
        self._connection.close()

    def add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when RabbitMQ closes the connection to the publisher unexpectedly.

        """
        self.logger.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self.logger.warning('Connection closed, reopening in 5 seconds: %s',
                           reason)
            self._connection.ioloop.call_later(5, self.reconnect)

    def on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :param pika.SelectConnection _unused_connection: The connection

        """
        self.logger.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        if not self._closing:

            # Create a new connection
            self._connection = self.connect()

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        self.logger.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param Exception reason: why the channel was closed

        """
        self.logger.warning('Channel %i was closed: %s', channel, reason)
        self._connection.close()

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        self.logger.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange()
        self.start_consuming()

    def setup_exchange(self):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        self.logger.info('Declaring exchange')
        self._channel.exchange_declare(self.COLLECTION_EXCHANGE, self.EXCHANGE_TYPE, durable=True)
        self._channel.exchange_declare(self.EVAL_EXCHANGE, self.EXCHANGE_TYPE, durable=True)
        self._channel.exchange_declare(self.SHOP_AREA_CHANGE_EXCHANGE, self.EXCHANGE_TYPE, durable=True)
        self._channel.exchange_declare(self.SHOP_COUPON_HANDLER_EXCHANGE, self.EXCHANGE_TYPE, durable=True)
        self._channel.exchange_declare(self.GOODS_PROMOTION_HANDLER_EXCHANGE, self.EXCHANGE_TYPE, durable=True)
        # self.on_exchange_declareok()
        self.setup_queue()

    # def on_exchange_declareok(self):
    #     """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
    #     command.
    #
    #     :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
    #
    #     """
    #     self.logger.info('Exchange declared')
    #     self.setup_queue()

    def setup_queue(self):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        self.logger.info('Declaring queue')
        self._channel.queue_declare(self.COLLECTION_CHANGE_QUEUE, durable=True)
        self._channel.queue_declare(self.USER_EVAL_CHANGE_QUEUE, durable=True)
        self._channel.queue_declare(self.SHOP_AREA_CHANGE_QUEUE, durable=True)
        self._channel.queue_declare(self.SHOP_COUPON_HANDLER_QUEUE, durable=True)
        self._channel.queue_declare(self.GOODS_PROMOTION_HANDLER_QUEUE, durable=True)
        self.on_queue_declareok()

    def on_queue_declareok(self):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        self.logger.info('Binding %s to %s', self.COLLECTION_CHANGE_QUEUE, self.COLLECTION_CHANGE_QUEUE)
        self._channel.queue_bind(
            queue=self.COLLECTION_CHANGE_QUEUE,
            exchange=self.COLLECTION_EXCHANGE
        )
        self.logger.info('Binding %s to %s', self.USER_EVAL_CHANGE_QUEUE, self.EVAL_EXCHANGE)
        self._channel.queue_bind(
            queue=self.USER_EVAL_CHANGE_QUEUE,
            exchange=self.EVAL_EXCHANGE
        )
        self.logger.info('Binding %s to %s', self.SHOP_AREA_CHANGE_QUEUE, self.SHOP_AREA_CHANGE_EXCHANGE)
        self._channel.queue_bind(
            queue=self.SHOP_AREA_CHANGE_QUEUE,
            exchange=self.SHOP_AREA_CHANGE_EXCHANGE
        )
        self.logger.info('Binding %s to %s', self.SHOP_COUPON_HANDLER_QUEUE, self.SHOP_COUPON_HANDLER_EXCHANGE)
        self._channel.queue_bind(
            queue=self.SHOP_COUPON_HANDLER_QUEUE,
            exchange=self.SHOP_COUPON_HANDLER_EXCHANGE
        )
        self.logger.info('Binding %s to %s', self.GOODS_PROMOTION_HANDLER_QUEUE, self.GOODS_PROMOTION_HANDLER_EXCHANGE)
        self._channel.queue_bind(
            queue=self.GOODS_PROMOTION_HANDLER_QUEUE,
            exchange=self.GOODS_PROMOTION_HANDLER_EXCHANGE
        )
        # self.on_bindok()
        self.start_consuming()

    # def on_bindok(self):
    #     """Invoked by pika when the Queue.Bind method has completed. At this
    #     point we will start consuming messages by calling start_consuming
    #     which will invoke the needed RPC commands to start the process.
    #
    #     :param pika.frame.Method unused_frame: The Queue.BindOk response frame
    #
    #     """
        # self.logger.info('Queue bound')
        # self.start_consuming()

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        self.logger.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            self.COLLECTION_CHANGE_QUEUE,
            functools.partial(message_handler_map['userCollectHandler'].append_message, self)
        )
        self._channel.basic_consume(
            self.USER_EVAL_CHANGE_QUEUE,
            functools.partial(message_handler_map['userEvalHandler'].append_message, self)
        )
        self._channel.basic_consume(
            self.SHOP_AREA_CHANGE_QUEUE,
            functools.partial(message_handler_map['shopStatusHandler'].append_message, self)
        )
        self._channel.basic_consume(
            self.SHOP_COUPON_HANDLER_QUEUE,
            functools.partial(message_handler_map['shopCouponHandler'].append_message, self)
        )
        self._channel.basic_consume(
            self.GOODS_PROMOTION_HANDLER_QUEUE,
            functools.partial(message_handler_map['goodsPromotionHandler'].append_message, self)
        )

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            self.logger.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        self.logger.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        self.logger.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        self.logger.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        self.logger.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        self.logger.info('Closing the channel')
        self._channel.close()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        self.logger.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        self.logger.info('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        self.logger.info('Stopped')


# def main():
#     logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
#     consumer = TornadoConsumer()
#     try:
#         consumer.run()
#     except KeyboardInterrupt:
#         consumer.stop()
# 
# 
# if __name__ == '__main__':
#     main()
