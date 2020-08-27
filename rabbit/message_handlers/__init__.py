from .shop_status_handler import ShopStatusHandler
from .shop_coupon_handler import ShopCouponHandler
from .user_collect_handler import UserCollectHandler
from .user_eval_handler import UserEvalHandler
from .goods_promotion_handler import GoodsPromotionHandler


shop_status_handler_obj = ShopStatusHandler()
shop_status_handler_obj.start_consuming()

shop_coupon_handler_obj = ShopCouponHandler()
shop_coupon_handler_obj.start_consuming()

user_collect_handler_obj = UserCollectHandler()
user_collect_handler_obj.start_consuming()

user_eval_handler_obj = UserEvalHandler()
user_eval_handler_obj.start_consuming()

goods_promotion_handler_obj = GoodsPromotionHandler()
goods_promotion_handler_obj.start_consuming()

message_handler_map = {
    'shopStatusHandler': shop_status_handler_obj,
    'shopCouponHandler': shop_coupon_handler_obj,
    'userCollectHandler': user_collect_handler_obj,
    'userEvalHandler': user_eval_handler_obj,
    'goodsPromotionHandler': goods_promotion_handler_obj,
}