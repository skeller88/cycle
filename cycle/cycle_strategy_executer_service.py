from typing import Tuple, Optional

from pandas.tests.indexes.conftest import zero
from sqlalchemy.orm import scoped_session, Session
from trading_platform.core.strategy.strategy_execution import StrategyExecution
from trading_platform.exchanges.data.enums.order_side import OrderSide
from trading_platform.exchanges.data.enums.order_status import OrderStatus
from trading_platform.exchanges.data.financial_data import FinancialData, one
from trading_platform.exchanges.data.order import Order
from trading_platform.exchanges.data.pair import Pair
from trading_platform.exchanges.exchange_service_abc import ExchangeServiceAbc
from trading_platform.exchanges.order_execution_service import OrderExecutionService
from trading_platform.storage.daos.strategy_execution_dao import StrategyExecutionDao
from trading_platform.strategy.services.strategy_executer_service_abc import StrategyExecuterServiceAbc
from trading_platform.utils.datetime_operations import datetime_now_with_utc_offset


class CycleStrategyExecuterService(StrategyExecuterServiceAbc):
    def __init__(self, **kwargs):
        self.logger = kwargs.get('logger')
        self.exchange: ExchangeServiceAbc = kwargs.get('exchange')
        self.order_execution_service: OrderExecutionService = kwargs.get('order_execution_service')
        self.scoped_session_maker: scoped_session = kwargs.get('scoped_session_maker')
        self.strategy_execution_dao: StrategyExecutionDao = kwargs.get('strategy_execution_dao')

        self.strategy_execution: Optional[StrategyExecution] = None
        self.buy_window: Tuple[float, float] = kwargs.get('buy_window')
        self.sell_window: Tuple[float, float] = kwargs.get('sell_window')
        self.pair: Pair = kwargs.get('pair')
        self.order_padding_percent: FinancialData = kwargs.get('order_padding_percent')
        self.balance_percent_per_trade: FinancialData = kwargs.get('balance_percent_per_trade')

    def refresh_state(self, repeat: bool, refresh_freq_sec: int):
        """
        Preload the exchange state for faster trade execution.

        Args:
            repeat:
            refresh_freq_sec:

        Returns:

        """

    def initialize(self, strategy_id: str):
        strategy_execution: StrategyExecution = StrategyExecution(**{
            'strategy_id': strategy_id,
            'state': {
                'buy_order_count': 0,
                'sell_order_count': 0
            }
        })
        saved: StrategyExecution = self.strategy_execution_dao.save(self.scoped_session_maker(),
                                                                    popo=strategy_execution, commit=True)
        self.strategy_execution = strategy_execution

    def step(self, **kwargs):
        now: float = datetime_now_with_utc_offset().timestamp()
        order_side: Optional[int] = None
        if self.buy_window[0] <= now < self.buy_window[1]:
            order_side = OrderSide.buy
        elif self.sell_window[0] <= now < self.sell_window[1]:
            order_side = OrderSide.sell

        if order_side is not None:
            self.exchange.fetch_balances()
            self.exchange.fetch_latest_tickers()

            if order_side == OrderSide.buy:
                order_amount: FinancialData = self.balance_percent_per_trade * self.exchange.get_balance(self.pair.base)
                order_price: FinancialData = self.exchange.get_ticker(self.pair.name).ask * (
                        one + self.order_padding_percent)
                self.strategy_execution.state['buy_order_count'] += 1
            else:
                order_amount: FinancialData = self.balance_percent_per_trade * self.exchange.get_balance(
                    self.pair.quote)
                order_price: FinancialData = self.exchange.get_ticker(self.pair.name).bid * (
                        one - self.order_padding_percent)
                self.strategy_execution.state['sell_order_count'] += 1

            if order_amount >= zero:
                session: Session = self.scoped_session_maker()
                order: Order = Order(**{
                    'exchange_id': self.exchange.exchange_id,

                    'amount': order_amount,
                    'price': order_price,

                    'base': self.pair.base,
                    'quote': self.pair.quote,
                    'order_side': OrderSide.sell,
                    'order_status': OrderStatus.open
                })
                self.order_execution_service.execute_order(order, session=session, write_pending_order=True,
                                                           check_if_order_filled=True)
                self.strategy_execution_dao.update_fetch_by_column(session=session, column_name='strategy_execution',
                                                                   update_dict={
                                                                       'state': self.strategy_execution.state
                                                                   },
                                                                   column_value=self.strategy_execution, commit=True)
