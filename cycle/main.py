import argparse
import logging
import os
import sys
from time import sleep
from typing import Callable, Dict

sys.path.append(os.getcwd())
import daemon
from trading_platform.aws_utils.parameter_store_service import ParameterStoreService
from trading_platform.core.services.logging_service import LoggingService
from trading_platform.exchanges.data.enums import exchange_ids
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


def main(logger: logging.Logger):
    logger.info('running new_market main method')

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

    exchanges_by_id: Dict[int, ExchangeServiceAbc] = live_subclasses.instantiate(subclasses=live_subclasses.all_live())

    order_execution_service: OrderExecutionService = OrderExecutionService(**{
        'logger': logger,
        'exchanges_by_id': exchanges_by_id,
        'order_dao': OrderDao(),
        'multithreaded': False,
        'num_order_status_checks': OrderExecutionProperties.num_order_status_checks,
        'sleep_time_sec_between_order_checks': OrderExecutionProperties.sleep_time_sec_between_order_checks,
        'scoped_session_maker': engine.scoped_session_maker
    })

    exchange: ExchangeServiceAbc = exchanges_by_id.get(exchange_ids.binance)
    cycle_strategy_executer_service: CycleStrategyExecuterService = CycleStrategyExecuterService(**{
        'exchange': exchange,
        'order_execution_service': order_execution_service,
        'strategy_execution_dao': StrategyExecutionDao(),
        'scoped_session_maker': engine.scoped_session_maker,

        'buy_window': (CycleProperties.buy_window_utc_hour_start, CycleProperties.buy_window_utc_hour_end),
        'sell_window': (CycleProperties.sell_window_utc_hour_start, CycleProperties.sell_window_utc_hour_end),

        'balance_percent_per_trade': CycleProperties.balance_percent_per_trade,
        'order_padding_percent': CycleProperties.order_padding_percent,
    })

    while True:
        cycle_strategy_executer_service.step()
        sleep(3600/CycleProperties.executions_per_hour)


def get_cli_args() -> Dict:
    parser = argparse.ArgumentParser()
    parser.add_argument('--run_daemon', help='Whether to run the script as a daemon. Can be "True" or "False".')
    arg_dict: Dict = vars(parser.parse_args())
    arg_dict['run_daemon'] = arg_dict['run_daemon'] == 'True'
    return arg_dict


if __name__ == '__main__':
    file: str = os.path.join(os.getcwd(),
                             'new_market_{0}.log'.format(datetime_now_with_utc_offset().strftime(strftime_minutes)))
    print('Logging to {0}'.format(file))
    file_handler: logging.FileHandler = logging.FileHandler(filename=file, mode='w+')
    file_handler.setFormatter(LoggingService.get_default_formatter())
    logger: logging.Logger = LoggingService.set_logger(name=None, handler=file_handler)

    arg_dict = get_cli_args()

    if arg_dict.get('run_daemon'):
        with daemon.DaemonContext(files_preserve=[file_handler.stream]):
            main(logger)
    else:
        main(logger)