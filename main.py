# coding=utf-8
from __future__ import print_function, absolute_import
from gm.api import *
from gm.csdk.c_sdk import BarLikeDict2, TickLikeDict2
from gm.model import DictLikeAccountStatus, DictLikeExecRpt, DictLikeIndicator, DictLikeOrder, DictLikeParameter
import math
import random
import collections
from typing import List
from datetime import datetime, timedelta, timezone

class CodeInfo:
    """各个股票的仓位信息"""

    def __init__(self):
        self.__code: str = None  # 股票代码
        self.ratio = 0  # 仓位
        self.total_time = 0  # 购买总时间(分钟)
        self.__min_amount = 50000  # 单笔最低金额(元)
        self.__max_amount = 0  # 单笔最高金额(元)
        self.button = 0  # 下单开关

        self.amount_type = None  # 下单类型, 融资买入, 本金买入，担保品卖出

        self.order_time = None  # 上次下单时间
        self.end_time = None  # 下单截至时间
        self.target_amount = None  # 当前剩余的委托目标金额
        self.interval = None  # 下单间隔时长
        self.prices = collections.deque(maxlen=2)  # 最近的2个最新价格

    @property
    def code_symbol(self):
        return self.__code

    @property
    def code(self):
        if self.__code is None:
            return 0
        return int(self.__code[5:])

    @code.setter
    def code(self, value):
        self.__code = f"SHSE.{int(value):06d}"

    @property
    def min_amount(self):
        return self.__min_amount // 10000

    @min_amount.setter
    def min_amount(self, value):
        self.__min_amount = value * 10000

    @property
    def max_amount(self):
        return self.__max_amount // 10000

    @max_amount.setter
    def max_amount(self, value):
        self.__max_amount = value * 10000

    def run_tasks(self, now_time):
        """开始运行任务"""
        # 程序开始运行
        if self.order_time is None:
            if self.amount_type == "担保品卖出":
                for position in get_position():
                    if position.symbol != self.code_symbol:
                        continue
                    target_amount = position.amount * self.ratio
                    break
                else:
                    print(f"当前无该标的股票的持仓:{self.code_symbol}")
                    return False
            else:
                net_assets = get_cash().nav
                print(f"当前本金:{net_assets}")
                target_amount = int(self.ratio * net_assets)
        
            self.end_time = now_time + self.total_time * 60
            self.order_time = now_time
            self.target_amount = target_amount
            print(f"程序从头开始,截至时间:{self.end_time},目标金额:{self.target_amount}")
        # 程序暂停后继续运行
        else:
            self.end_time = now_time + self.end_time
            print(f"程序继续运行,截至时间:{self.end_time}")
        self.button = 1
        self.update_interval(now_time)
        return True

    def pause_task(self, now_time):
        """暂停任务"""
        self.button = 0
        self.end_time = self.end_time - now_time
        print(f"程序暂停,剩余时间:{self.end_time}秒")
    
    def clear_task(self):
        """清除任务"""
        if self.order_time is None:
            return
        self.end_time = None  # 下单截至时间
        self.order_time = None  # 上次下单时间
        self.target_amount = None  # 委托目标金额
        print("数据发生变化,清除旧有任务")

    def update_interval(self, now_time):
        """更新下单间隔时长"""
        if now_time - self.end_time > 0:
            self.interval = 999999999999
            print(f"下单已结束, 不在更新下单间隔时长")
            return 
        if self.target_amount < self.__min_amount:
            self.interval = 999999999999
            print(f"剩余目标金额不足, 不在更新下单间隔时长")
            return 
        interval = int((self.end_time - now_time) / (self.target_amount * 2 / (self.__min_amount + self.__max_amount)))
        self.interval = random.randint(int(interval * 0.8), int(interval * 1.2))
        print(f"更新下单间隔时长:{self.interval}秒,基准线:{interval}秒")

    def calculate_order_volume(self, price):
        """计算委托数量"""
        if self.target_amount < self.__min_amount:
            return 0
        if self.target_amount < self.__max_amount:
            order_volume = math.floor(self.target_amount / price / 100) * 100
        else:
            max_volume = math.floor(self.__max_amount / price / 100)
            min_volume = math.ceil(self.__min_amount / price / 100)
            order_volume = random.randint(min_volume, max_volume) * 100
        self.target_amount -= order_volume * price
        return order_volume

    def get_order_price(self, quotes: List[dict]):
        """根据逻辑获取所需的下单委托价格
        Args:
            prices (List[float]): 最近的2个最新价格
            quotes (List[dict]): 买卖5档数据
        """
        price_up = self.prices[1] > self.prices[0]
        if self.amount_type != "担保品卖出":
            if not price_up:
                return quotes[random.randint(0, 2)]["bid_p"]
            bid_amount, ask_amount = self.calculated_amount(quotes)
            if ask_amount * 1.2 >= bid_amount:
                return quotes[random.randint(0, 2)]["bid_p"]
            return quotes[random.randint(0, 2)]["ask_p"]
        else:
            if price_up:
                return quotes[random.randint(0, 2)]["ask_p"]
            bid_amount, ask_amount = self.calculated_amount(quotes)
            if bid_amount * 1.2 >= ask_amount:
                return quotes[random.randint(0, 2)]["ask_p"]
            return quotes[random.randint(0, 2)]["bid_p"]

    @staticmethod
    def calculated_amount(quotes: List[dict]):
        """计算买卖5档的成交量"""
        bid_amount, ask_amount = 0, 0
        for quote in quotes:
            bid_amount += quote['bid_v'] * quote["bid_p"]
            ask_amount += quote['ask_v'] * quote["ask_p"]
        return bid_amount, ask_amount

def test(context):
    """"测试方法"""
    code_info: CodeInfo = context.code_infos[0]
    code_info.code = 600000
    code_info.ratio = 0.33
    code_info.total_time = 30
    code_info.min_amount = 5
    code_info.max_amount = 10
    
    context.code_key[0] = "SHSE.600000"

    subscribe(symbols=code_info.code_symbol, frequency="tick", fields="symbol,quotes,price")
    code_info.run_tasks(context.now.timestamp())

def init(context):
    """
    策略中必须有init方法,且策略会首先运行init定义的内容，可用于
    * 获取低频数据(get_fundamentals, get_fundamentals_n, get_instruments, get_history_instruments, get_instrumentinfos,
    get_constituents, get_history_constituents, get_sector, get_industry, get_trading_dates, get_previous_trading_date,
    get_next_trading_date, get_dividend, get_continuous_contracts, history, history_n, )
    * 申明订阅的数据参数和格式(subscribe)，并附带数据事件驱动功能
    * 申明定时任务(schedule)，附带本地时间事件驱动功能
    * 读取静态的本地数据或第三方数据
    * 定义全局常量,如 context.user_data = 'balabala'
    * 最好不要在init中下单(order_volume, order_value, order_percent, order_target_volume, order_target_value, order_target_percent)
    """
    finance_code = CodeInfo()
    finance_code.amount_type = "融资买入"
    principal_code = CodeInfo()
    principal_code.amount_type = "本金买入"
    close_code = CodeInfo()
    close_code.amount_type = "担保品卖出"
    context.code_infos = [finance_code, principal_code, close_code]
    context.code_key = ["", "", ""]
    for i, name in enumerate(["杂毛低吸(融资)", "杂毛低吸(本金)", "杂毛抛出"]):
        add_parameter(f"code_{i}", 0,  name='代码:', group=name)
        add_parameter(f"ratio_{i}", 0, min=0, max=2, name='仓位:', group=name)
        add_parameter(f"total_time_{i}", 0,  name='时长:', intro="自动下单总时长(分钟)", group=name)
        add_parameter(f"min_amount_{i}", 5,  name='最小:', intro="委托单笔最小金额(万)", group=name)
        add_parameter(f"max_amount_{i}", 0,  name='最大:', intro="委托单笔最大金额(万)", group=name)
        add_parameter(f"button_{i}", 0,  name='开/停:', intro="1是运行,其他暂停", group=name)
    # TODO 回测使用，正式运行时请注释掉
    schedule(schedule_func=test, date_rule="1d", time_rule="14:30:00")


def on_parameter(context, parameter:DictLikeParameter):
    """动态参数修改时间推送"""
    code_index = int(parameter.key[-1])
    param_name = parameter.key[:-2]
    value = parameter.value
    code_info: CodeInfo = context.code_infos[code_index]
    if getattr(code_info, param_name) == value:
        return
    if param_name == "button":
        now_time = context.now.timestamp()
        if value == 1:
            code_info.run_tasks(now_time)
        else:
            code_info.pause_task(now_time)
        return
    if code_info.button == 1:
        print("正在运行,禁止修改参数")
        parameter["value"] = getattr(code_info, param_name)
        set_parameter(**parameter)
        return
    
    if param_name == "code":
        code_str = f"SHSE.{int(value):06d}"
        code_name = get_symbol_infos(1010, symbols=code_str)
        if len(code_name) == 0:
            print(f"输入的代码:{code_str},有误,不存在该股票")
            return
        code_name = code_name[0]["sec_name"]
        if code_str not in context.code_key:
            subscribe(symbols=code_str, frequency="tick", fields="symbol,quotes,price")
            print(f"订阅数据,股票代码:{code_str}, 股票名称:{code_name}")
        old_code_str = context.code_key[code_index]
        if old_code_str != "":
            unsubscribe(symbols=old_code_str, frequency="tick")
            print(f"取消订阅数据,股票代码:{old_code_str}")
        context.code_key[code_index] = code_str
    code_info.clear_task()
    setattr(code_info, param_name, value)
    names = ["杂毛低吸(融资)", "杂毛低吸(本金)", "杂毛抛出"]
    print(f"更新{names[code_index]}的{param_name}参数为:{value}")


def on_tick(context, tick):
    """数据事件驱动函数, 当订阅的数据有更新时, 会触发该函数, 该函数的参数为订阅的数据"""
    now: datetime = context.now
    if now.hour < 9 or (now.hour == 9 and now.minute < 31):
        return
    now_time = now.timestamp()
    symbol = tick['symbol']
    code_info: CodeInfo = context.code_infos[context.code_key.index(symbol)]
    code_info.prices.append(tick['price'])
    if code_info.button == 0:
        return
    # 撤销委托
    if now_time - code_info.end_time > 30:
        print("已超过最终下单时间30秒,撤销所有未结委托")
        for order in get_unfinished_orders():
            if order['symbol'] != symbol:
                continue
            order_cancel(wait_cancel_orders=order)
        code_info.pause_task(now_time)
        code_info.clear_task()
        return 
    # 未到订单执行时间
    if now_time - code_info.order_time < code_info.interval:
        return
    order_price = code_info.get_order_price(tick["quotes"])
    if order_price == 0:
        print("当前买卖五档存在价格为0的情况")
        return
    order_volume = code_info.calculate_order_volume(order_price)
    if order_volume == 0:
        print("当前委托量为0")
        return
    code_info.order_time = now_time
    if code_info.amount_type == "融资买入":
        credit_buying_on_margin(
            symbol=symbol,
            volume=order_volume,
            price=order_price,
            order_type=OrderType_Limit,
            position_src=PositionSrc_Unknown
        )
    elif code_info.amount_type == "本金买入":
        credit_buying_on_collateral(
            symbol=symbol,
            volume=order_volume,
            price=order_price,
            order_type=OrderType_Limit
        )
    else:
        credit_selling_on_collateral(
            symbol=symbol,
            volume=order_volume,
            price=order_price,
            order_type=OrderType_Limit
        )
    code_info.update_interval(now_time)
    print(f'{context.now}:标的:{symbol},操作:{code_info.amount_type},以限价发起委托, 价格:{order_price}, 数量:{order_volume}')


def on_order_status(context, order):
    """委托状态更新时间"""
    # 查看下单后的委托状态，等于3代表委托全部成交
    status = order['status']
    if status != 3:
        return
    # 标的代码
    symbol = order['symbol']
    # 委托价格
    price = order['price']
    # 委托数量
    volume = order['volume']
    # 买卖方向，1为买入，2为卖出
    side = order['side']
    # 开平仓类型，1为开仓，2为平仓
    effect = order['position_effect']
    if side == 1 and effect == 1:
        side_effect = '买入'
    elif side == 2 and effect == 2:
        side_effect = "卖出"
    else:
        raise Exception("订单异常状态")
    print(f'{context.now}:标的:{symbol},操作:以限价委托{side_effect},委托价格:{price},委托数量:{volume},'
          f'成交量:{order["filled_volume"]},均价:{order["filled_vwap"]:.3f}')

if __name__ == '__main__':
    '''
        strategy_id策略ID, 由系统生成
        filename文件名, 请与本文件名保持一致
        mode运行模式, 实时模式:MODE_LIVE回测模式:MODE_BACKTEST
        token绑定计算机的ID, 可在系统设置-密钥管理中生成
        backtest_start_time回测开始时间
        backtest_end_time回测结束时间
        backtest_adjust股票复权方式, 不复权:ADJUST_NONE前复权:ADJUST_PREV后复权:ADJUST_POST
        backtest_initial_cash回测初始资金
        backtest_commission_ratio回测佣金比例
        backtest_slippage_ratio回测滑点比例
        backtest_match_mode市价撮合模式，以下一tick/bar开盘价撮合:0，以当前tick/bar收盘价撮合：1
    '''
    now_time = datetime.now()
    backtest_start_time = str(datetime(now_time.year, now_time.month, now_time.day, 9, 30) - timedelta(days=1))[:19]
    backtest_end_time = str(datetime(now_time.year, now_time.month, now_time.day, 15, 0) - timedelta(days=1))[:19]
    run(strategy_id='6273266a-f99a-11ef-8a3a-00ffce4ccc5a',
        filename='main.py',
        mode=MODE_BACKTEST,
        token='90a9433545fd918efd82ca08be0ad76fcc08761b',
        backtest_start_time=backtest_start_time,
        backtest_end_time=backtest_end_time,
        backtest_adjust=ADJUST_PREV,
        backtest_initial_cash=10000000,
        backtest_commission_ratio=0.0001,
        backtest_slippage_ratio=0.0001,
        backtest_match_mode=1)


