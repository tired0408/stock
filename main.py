# coding=utf-8
from __future__ import print_function, absolute_import
from gm.api import *
from gm.csdk.c_sdk import BarLikeDict2, TickLikeDict2
from gm.model import DictLikeAccountStatus, DictLikeExecRpt, DictLikeIndicator, DictLikeOrder, DictLikeParameter, Dict
import abc
import math
import time
import random
import collections
from typing import List
from datetime import datetime, timedelta

class TradeBase(abc.ABC):
    """各个股票的交易类"""

    def __init__(self):
        self._code = None  # 股票代码
        self.ui_ratio = 0  # 仓位
        self.ui_total_time = 0  # 购买总时长(分钟)
        self._min_amount = 5  # 单笔最低金额(万)
        self._max_amount = 0  # 单笔最高金额(万)

        self.end_time = -1  # 结束时间
        self.remain_time = None  # 剩余运行时间
        self.order_prices = None  # 可委托的价格列表
        self._last_order_time = None  # 上次下单时间
        self.interval = 99999999  # 下单间隔时间
        self.prices = collections.deque(maxlen=2)  # 最近的2个最新价格
        self.quotes = []  # 当前五档行情
        self.target_value = None  # 目标金额(股数)
        self.timer_id = None

    @property
    def ui_code(self):
        if self._code is None:
            return 0
        return int(self._code[5:])

    @ui_code.setter
    def ui_code(self, value):
        self._code = value

    @property
    def ui_min_amount(self):
        return self._min_amount // 10000

    @ui_min_amount.setter
    def ui_min_amount(self, value):
        self._min_amount = value * 10000

    @property
    def ui_max_amount(self):
        return self._max_amount // 10000

    @ui_max_amount.setter
    def ui_max_amount(self, value):
        self._max_amount = value * 10000

    @property
    def ui_status(self):
        if self.end_time == -1:
            return 0
        if self.remain_time is not None:
            return 0
        return 1

    def set_interval_full(self):
        """设置下单间隔为无限长"""
        self.interval = 9999

    def is_interval_full(self):
        """判断下单间隔是否是无限长"""
        return self.interval == 9999
    

    def clear_run_cache(self):
        """清理运行缓存"""
        self.end_time = -1
        self.remain_time = None
        self.order_prices = None

    def change_status(self, context):
        """修改程序运行状态"""
        now_time: datetime = context.now
        now_time_str = now_time.strftime('%H:%M:%S')
        # 停止转为开始运行
        if self.end_time == -1:
            # 检查参数
            if self._min_amount >= self._max_amount:
                print(f"[{self._code}]{now_time_str}:单笔最大金额必须大于最小金额")
                return False
            if self.ui_total_time == 0:
                print(f"[{self._code}]{now_time_str}:购买总时长不能为0")
                return False
            self.target_value = self.calculated_target_value(now_time_str)
            if self.target_value == 0:
                print(f"[{self._code}]{now_time_str}:目标金额(股数)为0,无法运行")
                return False
            # 开始运行程序
            self._last_order_time = now_time.timestamp()
            self.set_interval_full()
            self.end_time = now_time.timestamp() + self.ui_total_time * 60
            self.timer_id = timer(timer_func=self.each_trade, period=1, start_delay=0)
            end_time_str = datetime.fromtimestamp(self.end_time).strftime('%H:%M:%S')
            print(f"[{self._code}]{now_time_str}:开始运行,截至时间:{end_time_str},目标金额(股数):{self.target_value}")
        elif self.end_time - now_time.timestamp() < 0 or self.is_interval_full():
            self.clear_run_cache()
            print(f"[{self._code}]{now_time_str}:已到截至时间,清理运行缓存数据")
        # 运行转为暂停
        elif self.remain_time is None:
            self.remain_time = int(self.end_time - now_time.timestamp())
            timer_stop(self.timer_id['timer_id'])
            print(f"[{self._code}]{now_time_str}:程序暂停运行,剩余运行时长:{self.remain_time}秒")
        # 暂停转为运行
        else:
            self.end_time = now_time.timestamp() + self.remain_time
            self.remain_time = None
            self.timer_id = timer(timer_func=self.each_trade, period=1, start_delay=0)
            end_time_str = datetime.fromtimestamp(self.end_time).strftime('%H:%M:%S')
            print(f"[{self._code}]{now_time_str}:程序继续运行,截至时间:{end_time_str},目标金额(股数):{self.target_value}")
        return True

    def each_trade(self, context):
        """每毫秒进行交易判断"""
        now_time: datetime = context.now
        now_time_str = now_time.strftime('%H:%M:%S')
        if self._last_order_time + self.interval - now_time.timestamp() > 0:
            return
        if self.end_time - now_time.timestamp() < 0 or self.is_interval_full():
            print(f"[{self._code}]{now_time_str}:已到达截至时间,停止运行,传递到动态参数")
            timer_stop(self.timer_id['timer_id'])
            parameter_map: Dict[str, DictLikeParameter] = context.parameters
            for i in range(3):
                code_key = f"ui_code_{i}"
                if parameter_map[code_key]["value"] != self.ui_code:
                    continue
                parameter = parameter_map[f"ui_status_{i}"]
                parameter["value"] = 0
                set_parameter(**parameter)
            return
        if len(self.order_prices) == 0:
            return
        order_price_value = random.choice(self.order_prices)
        order_volume_value = self.calculate_order_volume(order_price_value)
        if order_volume_value == 0:
            print(f"[{self._code}]{now_time_str}:当前委托量存在异常为0")
            return
        self.place_order(now_time_str, order_price_value, order_volume_value)
        self.update_interval(now_time)
        self._last_order_time = now_time.timestamp()

        


    @staticmethod
    def calculated_amount(quotes: List[dict]):
        """计算买卖5档的成交量"""
        bid_amount, ask_amount = 0, 0
        for quote in quotes:
            bid_amount += quote['bid_v'] * quote["bid_p"]
            ask_amount += quote['ask_v'] * quote["ask_p"]
        return bid_amount, ask_amount

    def update_tick(self, now_time:datetime, price, quotes):
        """更新tick数据"""
        now_time_str = now_time.strftime('%H:%M:%S')
        self.prices.append(price)
        self.quotes = quotes
        if len(self.prices) < 2:
            return
        if self.end_time == -1:
            return
        self.update_order_prices(now_time_str, quotes)
        if self.end_time is not None and self._last_order_time is not None and self.is_interval_full():
            print(f"[{self._code}]{now_time_str}:更新下单间隔,提供程序开始运行的信号")
            self.update_interval(now_time)

    def update_interval(self, now_time: datetime):
        """更新下单间隔时长"""
        now_time_str = now_time.strftime('%H:%M:%S')
        if now_time.timestamp() - self.end_time > 0:
            self.set_interval_full()
            print(f"[{self._code}]{now_time_str}:下单结束,不再更新下单间隔时长")
            return
        if self.target_value  <= 0:
            self.set_interval_full()
            print(f"[{self._code}]{now_time_str}:剩余目标金额(股数)不足, 不在更新下单间隔时长")
            return 
        remain_amount = self.calculate_remain_amount()
        interval = (self.end_time - now_time.timestamp()) / (remain_amount * 2 / (self._min_amount + self._max_amount))
        self.interval = random.uniform(0.8, 1.2) * interval
        print(f"[{self._code}]{now_time_str}:更新下单间隔,基准线:{interval:.3f}秒,实际时长:{self.interval:.3f}秒")

    @abc.abstractmethod
    def update_order_prices(self, now_time_str, quotes):
        """更新可委托列表"""

    @abc.abstractmethod
    def calculate_order_volume(self, order_price_value):
        """计算委托数量"""

    @abc.abstractmethod
    def place_order(self, now_time_str, price, volume):
        """委托下单"""
    
    @abc.abstractmethod
    def calculate_remain_amount(self):
        """计算剩余金额"""


    @abc.abstractmethod
    def calculated_target_value(self, now_time_str):
        """计算目标金额或股数"""


class TradeBuy(TradeBase):
    """融资(担保品)买入的交易基类"""

    def update_order_prices(self, now_time_str, quotes):
        """更新可委托列表"""
        buy_amount, sell_amount = self.calculated_amount(quotes)
        if self.prices[1] < self.prices[0]:
            # print(f"[{self._code}]{now_time_str}:价格处于下跌状态, 使用买1-3价格")
            self.order_prices = [quotes[i]["bid_p"] for i in range(3) if quotes[i]["bid_p"] > 0]    
        elif sell_amount * 1.2 >= buy_amount:
            # print(f"[{self._code}]{now_time_str}:卖方金额大于买方金额,使用买1-3价格")
            self.order_prices = [quotes[i]["bid_p"] for i in range(3) if quotes[i]["bid_p"] > 0]
        else:
            # print(f"[{self._code}]{now_time_str}:卖方金额小于买方金额, 使用卖1-3价格")
            self.order_prices = [quotes[i]["ask_p"] for i in range(3) if quotes[i]["ask_p"] > 0]

    def calculate_order_volume(self, price):
        """计算委托数量"""
        if self.target_value <= self._max_amount:
            return math.floor(self.target_value / price / 100) * 100
        max_volume = math.floor(self._max_amount / price / 100)
        min_volume = math.ceil(self._min_amount / price / 100)
        order_volume = random.randint(min_volume, max_volume) * 100
        return order_volume

    def calculate_remain_amount(self):
        """计算剩余金额"""
        return self.target_value


    def calculated_target_value(self, now_time_str):
        """计算目标金额"""
        net_assets = get_cash().nav
        target_value = math.ceil(self.ui_ratio * net_assets)
        print(f"[{self._code}]{now_time_str}:当前本金:{net_assets},预计购买金额:{target_value}")
        return target_value

class TradeBuyMargin(TradeBuy):
    """融资买入类"""
    def place_order(self, now_time_str, price, volume):
        """委托下单"""
        credit_buying_on_margin(
            symbol=self._code,
            volume=volume,
            price=price,
            order_type=OrderType_Limit,
            position_src=PositionSrc_Unknown
        )
        self.target_value -= volume * price
        print(f'[{self._code}]{now_time_str}:融资买入,以限价发起委托, 价格:{price}, 数量:{volume}')

class TradeBuyCollateral(TradeBuy):
    """本金买入类"""
    def place_order(self, now_time_str, price, volume):
        """委托下单"""
        credit_buying_on_collateral(
            symbol=self._code,
            volume=volume,
            price=price,
            order_type=OrderType_Limit
        )
        self.target_value -= volume * price
        print(f'[{self._code}]{now_time_str}:担保品买入,以限价发起委托, 价格:{price}, 数量:{volume}')

class TradeSell(TradeBase):
    """担保品卖出的交易类"""

    def update_order_prices(self, now_time_str, quotes):
        """更新可委托列表"""
        bid_amount, ask_amount = self.calculated_amount(quotes)
        if self.prices[1] > self.prices[0]:
            # print(f"[{self._code}]{now_time_str}:价格处于上涨状态, 使用卖1-3价格")
            self.order_prices = [quotes[i]["ask_p"] for i in range(3) if quotes[i]["ask_p"] > 0]    
        elif bid_amount * 1.2 >= ask_amount:
            # print(f"[{self._code}]{now_time_str}:卖方金额小于买方金额,使用卖1-3价格")
            self.order_prices = [quotes[i]["ask_p"] for i in range(3) if quotes[i]["ask_p"] > 0]
        else:
            # print(f"[{self._code}]{now_time_str}:卖方金额大于买方金额, 使用买1-3价格")
            self.order_prices = [quotes[i]["bid_p"] for i in range(3) if quotes[i]["bid_p"] > 0]
    
    def calculate_order_volume(self, price):
        """计算委托数量"""
        if self.target_value*price <= self._max_amount:
            return self.target_value
        max_volume = math.floor(self._max_amount / price / 100)
        min_volume = math.ceil(self._min_amount / price / 100)
        order_volume = random.randint(min_volume, max_volume) * 100
        return order_volume

    def place_order(self, now_time_str, price, volume):
        """委托下单"""
        credit_selling_on_collateral(
            symbol=self._code,
            volume=volume,
            price=price,
            order_type=OrderType_Limit
        )
        self.target_value -= volume
        print(f'[{self._code}]{now_time_str}:担保品卖出,以限价发起委托, 价格:{price}, 数量:{volume}')
    
    def calculate_remain_amount(self):
        """计算剩余金额"""
        return self.target_value * self.prices[-1]

    def calculated_target_value(self, now_time_str):
        """计算目标股数"""
        for position in get_position():
            if position.symbol != self._code:
                continue
            target_value = math.ceil(position.volume * self.ui_ratio)
            print(f"[{self._code}]{now_time_str}:当前持仓:{position.volume},预计平仓股数:{target_value}")
            return target_value
        else:
            print(f"[{self._code}]{now_time_str}:当前无该标的股票持仓")
            return 0
        
        
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
    trade_map = {
        "杂毛低吸(融资)": TradeBuyMargin(),
        "杂毛低吸(本金)": TradeBuyCollateral(),
        "杂毛抛出": TradeSell()
    }
    for i, name in enumerate(trade_map.keys()):
        add_parameter(f"ui_code_{i}", 0,  name='代码:', group=name)
        add_parameter(f"ui_ratio_{i}", 0, min=0, max=2, name='仓位:', group=name)
        add_parameter(f"ui_total_time_{i}", 0,  name='时长:', intro="自动下单总时长(分钟)", group=name)
        add_parameter(f"ui_min_amount_{i}", 5,  name='最小:', intro="委托单笔最小金额(万)", group=name)
        add_parameter(f"ui_max_amount_{i}", 0,  name='最大:', intro="委托单笔最大金额(万)", group=name)
        add_parameter(f"ui_status_{i}", 0,  name='开/停:', intro="1是运行,其他暂停", group=name)
    context.trade_map = trade_map


def on_parameter(context, parameter:DictLikeParameter):
    """动态参数修改时间推送"""
    code_key = parameter.group
    param_name = parameter.key[:-2]
    value = parameter.value
    symbol_trade: TradeBase = context.trade_map[code_key]
    # 无数据变化不响应
    if getattr(symbol_trade, param_name) == value:
        return
    # 运行按钮
    if param_name == "ui_status":
        if symbol_trade.change_status(context):
            return
        parameter["value"] = 0 if parameter["value"] == 1 else 1
        set_parameter(**parameter)
        return
    # 防止运行中篡改参数
    if symbol_trade.ui_status == 1:
        print("正在运行,禁止修改参数")
        parameter["value"] = getattr(symbol_trade, param_name)
        set_parameter(**parameter)
        return
    # 订阅(取消订阅)相应股票信息
    if param_name == "ui_code":
        symbol_info = f"{int(value):06d}"
        symbol_info = get_symbol_infos(1010, symbols=[f"SHSE.{symbol_info}", f"SZSE.{symbol_info}"])
        if len(symbol_info) == 0:
            print(f"输入的代码:{value},有误,不存在该股票")
            parameter["value"] = symbol_trade.ui_code
            set_parameter(**parameter)
            return
        if symbol_trade._code is not None:
            unsubscribe(symbols=symbol_trade._code, frequency="tick")
            print(f"取消订阅数据,股票代码:{symbol_trade._code}")
        symbol_info = symbol_info[0]
        if symbol_info["symbol"] in context.symbols:
            print(f"已订阅该股票,股票代码:{symbol_info['symbol']}, 股票名称:{symbol_info['sec_name']}")
        else:
            subscribe(symbols=symbol_info["symbol"], frequency="tick", fields="symbol,quotes,price")
            print(f"订阅数据,股票代码:{symbol_info['symbol']}, 股票名称:{symbol_info['sec_name']}")
        value = symbol_info["symbol"]
    symbol_trade.clear_run_cache()
    setattr(symbol_trade, param_name, value)
    print(f"更新{code_key}的{param_name}参数为:{value}")

def on_tick(context, tick):
    symbol = tick['symbol']
    price = tick['price']
    quotes = tick["quotes"]
    trade_map: Dict[str, TradeBase] = context.trade_map
    for group, symbol_trade in trade_map.items():
        if symbol_trade._code != symbol:
            continue
        symbol_trade.update_tick(context.now, price, quotes)

def on_order_status(context, order: DictLikeOrder):
    """委托状态更新时间"""
    if order['status'] != 3:
        return 
    symbol = order['symbol']  # 标的代码
    price = order['price']  # 委托价格
    volume = order['volume']  # 委托数量
    order_bussiness = order["order_business"]
    if order_bussiness == OrderBusiness_CREDIT_BOM:
        operation_name = "融资买入"
    elif order_bussiness == OrderBusiness_CREDIT_BOC:
        operation_name = "担保品买入"
    elif order_bussiness == OrderBusiness_CREDIT_SOC:
        operation_name = "担保品卖出"
    else:
        print(order)
        raise Exception(f"订单异常状态")
    print(f'[{symbol}]{str(context.now)[11:19]}:操作类型:{operation_name},委托价格:{price},委托数量:{volume},'
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
    input_strategy_id = '58d95d1d-0535-11f0-8677-00ffce4ccc5a'
    input_token = '42d01f0aa40c9cd4a4d77cac825db51ac95d3d41'
    input_now_time = datetime.now()
    backtest_start_time = str(datetime(input_now_time.year, input_now_time.month, input_now_time.day, 9, 30) - timedelta(days=3))[:19]
    backtest_end_time = str(datetime(input_now_time.year, input_now_time.month, input_now_time.day, 15, 0) - timedelta(days=1))[:19]
    run(strategy_id=input_strategy_id,
        filename='main.py',
        mode=MODE_BACKTEST,
        token=input_token,
        backtest_start_time=backtest_start_time,
        backtest_end_time=backtest_end_time,
        backtest_adjust=ADJUST_PREV,
        backtest_initial_cash=10000000,
        backtest_commission_ratio=0.0001,
        backtest_slippage_ratio=0.0001,
        backtest_match_mode=1)