import logging
import tornado.ioloop
import tornado.web
from config.configInit import config_dict
from handlers.preprocess_handler import PreproessHandler
from handlers.preprocessors.spu_preprocessor import SpuPreprocessHandler
from rabbit.rabbit_consumer import TornadoConsumer

logging.basicConfig()


def get_logger(logger_name=None):
    logger = logging.getLogger(logger_name)
    logger.propagate = False
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.setLevel(logging.INFO)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger


def start():
    consumer = TornadoConsumer(get_logger('RABBIT'), config_dict)
    port = 6608  # by me
    app = tornado.web.Application(
        handlers=[
            (r"/cloudbrain-preprocess-python/preprocess", PreproessHandler,
             dict(logger=get_logger("PREPROCESS"), conf=config_dict)),
            (r"/cloudbrain-preprocess-python/preprocess-spu-to-redis", SpuPreprocessHandler,
             dict(logger=get_logger("PREPROCESS-SPU"))),
        ])
    app.listen(port)
    # tornado.ioloop.IOLoop.current().start()
    consumer.run()


if __name__ == "__main__":
    start()
