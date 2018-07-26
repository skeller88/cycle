import argparse
import datetime
import logging
import os
import random
import sys
from time import sleep
from typing import Callable, Dict, List

import pandas
from trading_platform.exchanges.backtest import backtest_subclasses
from trading_platform.exchanges.backtest.backtest_exchange_service import BacktestExchangeService
from trading_platform.exchanges.data.pair import Pair
from trading_platform.exchanges.ticker_service import TickerService

sys.path.append(os.getcwd())
import daemon
from trading_platform.aws_utils.parameter_store_service import ParameterStoreService
from trading_platform.core.services.logging_service import LoggingService
from trading_platform.exchanges.exchange_service_abc import ExchangeServiceAbc
from trading_platform.exchanges.live import live_subclasses
from trading_platform.exchanges.order_execution_service import OrderExecutionService
from trading_platform.properties.env_properties import EnvProperties, DatabaseProperties, OrderExecutionProperties
from trading_platform.storage.daos.order_dao import OrderDao
from trading_platform.storage.daos.strategy_execution_dao import StrategyExecutionDao
from trading_platform.storage.sql_alchemy_dtos import table_classes
from trading_platform.storage.sql_alchemy_engine import SqlAlchemyEngine
from trading_platform.utils.datetime_operations import datetime_now_with_utc_offset, strftime_minutes

from cycle.cycle_properties import CycleProperties
from cycle.cycle_strategy_executer_service import CycleStrategyExecuterService


def main(logger: logging.Logger, live: bool, ticker_dir: str):
    mode_name: str = 'live' if live else 'backtest'
    logger.info('running cycle strategy in {0} mode'.format(mode_name))

    table_classes.exchange_data_tables()

    if EnvProperties.is_prod:
        ParameterStoreService.load_properties_from_parameter_store_and_set('database_credentials')
        engine_maker_method: Callable = SqlAlchemyEngine.rds_engine
    else:
        engine_maker_method: Callable = SqlAlchemyEngine.local_engine_maker

    DatabaseProperties.set_properties_from_env_variables()
    engine = engine_maker_method()
    engine.add_engine_pidguard()
    engine.update_tables()

    if live:
        exchanges_by_id: Dict[int, ExchangeServiceAbc] = live_subclasses.instantiate(
            subclasses=live_subclasses.all_live())
    else:
        exchanges_by_id: Dict[int, ExchangeServiceAbc] = backtest_subclasses.instantiate()

    order_execution_service: OrderExecutionService = OrderExecutionService(**{
        'logger': logger,
        'exchanges_by_id': exchanges_by_id,
        'order_dao': OrderDao(),
        'multithreaded': False,
        'num_order_status_checks': OrderExecutionProperties.num_order_status_checks,
        'sleep_time_sec_between_order_checks': OrderExecutionProperties.sleep_time_sec_between_order_checks,
        'scoped_session_maker': engine.scoped_session_maker
    })

    pair: Pair = Pair(base=CycleProperties.base_currency, quote=CycleProperties.quote_currency)
    cycle_strategy_executer_service: CycleStrategyExecuterService = CycleStrategyExecuterService(**{
        'order_execution_service': order_execution_service,
        'strategy_execution_dao': StrategyExecutionDao(),
        'scoped_session_maker': engine.scoped_session_maker,

        'pair': pair,

        'buy_window': (CycleProperties.buy_window_utc_hour_start, CycleProperties.buy_window_utc_hour_end),
        'sell_window': (CycleProperties.sell_window_utc_hour_start, CycleProperties.sell_window_utc_hour_end),

        'balance_percent_per_trade': CycleProperties.balance_percent_per_trade,
        'order_padding_percent': CycleProperties.order_padding_percent,
    })

    # Example: cycle_strategy_btc_usd_buy_7_9_sell_20_22
    strategy_id = '{0}_{1}_buy_{2}_{3}_sell_{4}_{5}'.format(
        CycleStrategyExecuterService.strategy_base_id, pair.name, CycleProperties.buy_window_utc_hour_start,
        CycleProperties.buy_window_utc_hour_end, CycleProperties.sell_window_utc_hour_start,
        CycleProperties.sell_window_utc_hour_end
    )
    cycle_strategy_executer_service.initialize(strategy_id)

    exchange: ExchangeServiceAbc = exchanges_by_id.get(CycleProperties.exchange_id_to_trade)
    if live:
        while True:
            cycle_strategy_executer_service.step(**{
                'exchange': exchange,
                'now_datetime': datetime_now_with_utc_offset()
            })
            sleep(3600 / CycleProperties.executions_per_hour)
    else:
        ticker_filenames: List[str] = os.listdir(ticker_dir)
        ticker_filenames.sort()

        exchange.deposit_immediately(CycleProperties.base_currency, CycleProperties.initial_base_capital)
        exchanges_to_trade: Dict[int, BacktestExchangeService] = {exchange.exchange_id: exchange}

        for ticker_filename in ticker_filenames:
            print(ticker_filename)
            ticker_df = pandas.read_csv(os.path.join(ticker_dir, ticker_filename), parse_dates=['app_create_timestamp'])
            ticker_df['app_create_timestamp_min'] = ticker_df['app_create_timestamp'].dt.round('min')
            ticker_df.set_index('app_create_timestamp_min', inplace=True)

            for ticker_period in ticker_df.index.unique():
                tickers = ticker_df.loc[ticker_period]
                TickerService.set_latest_tickers_from_file(exchanges_to_trade, tickers)
                # 60 minute-level ticker files per hour. Execute the strategy a certain number of times per hour.
                if random.randint(0, 60 / CycleProperties.executions_per_hour) == 0:
                    cycle_strategy_executer_service.step(**{
                        'exchange': exchange,
                        'now_datetime': ticker_period.to_datetime()
                    })


def get_cli_args() -> Dict:
    parser = argparse.ArgumentParser()
    parser.add_argument('--run_daemon', help='Whether to run the script as a daemon. Can be "True" or "False".')
    parser.add_argument('--live',
                        help='Whether to run the strategy in live or backtest mode. Can be "True" or "False".')
    parser.add_argument('--ticker_dir', help='Absolute path of the ticker directory. For use in backtest mode only.')
    arg_dict: Dict = vars(parser.parse_args())
    arg_dict['live'] = arg_dict['live'] == 'True'
    arg_dict['run_daemon'] = arg_dict['run_daemon'] == 'True'
    arg_dict['logfile_path'] = arg_dict.get('logfile_path',
                                            os.path.dirname(__file__).replace('cycle/cycle', 'cycle/logs'))
    return arg_dict


if __name__ == '__main__':
    arg_dict = get_cli_args()
    file: str = os.path.join(arg_dict.get('logfile_path'),
                             'cycle_{0}.log'.format(datetime_now_with_utc_offset().strftime(strftime_minutes)))
    print('Logging to {0}'.format(file))
    file_handler: logging.FileHandler = logging.FileHandler(filename=file, mode='w+')
    file_handler.setFormatter(LoggingService.get_default_formatter())
    logger: logging.Logger = LoggingService.set_logger(name=None, handler=file_handler)

    if arg_dict.get('run_daemon'):
        with daemon.DaemonContext(files_preserve=[file_handler.stream]):
            main(logger, arg_dict.get('live'), arg_dict.get('ticker_dir'))
    else:
        main(logger, arg_dict.get('live'), arg_dict.get('ticker_dir'))
