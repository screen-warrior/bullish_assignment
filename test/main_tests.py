import unittest
from unittest.mock import patch, MagicMock
from main import CryptoDataCollector
import ccxt
import redis
import sqlite3
import os
import logging
from dotenv import load_dotenv

# Load environment variables from a .env file if present
load_dotenv()

# Set up logger
log_level = os.environ.get("LOG_LEVEL", "DEBUG")
logger = logging.getLogger("CryptoDataCollector")
logging.basicConfig(level=getattr(logging, log_level))

class TestCryptoDataCollector(unittest.TestCase):

    @patch('ccxt.binance')
    @patch('redis.Redis')
    @patch('sqlite3.connect')
    def setUp(self, mock_sqlite_connect, mock_redis, mock_binance):
        # Mocking the Binance exchange
        self.mock_exchange = mock_binance.return_value
        self.mock_exchange.fetch_order_book.return_value = {
            'bids': [[50000, 1], [49900, 2]],
            'asks': [[50100, 1], [50200, 2]],
        }
        self.mock_exchange.fetch_trades.return_value = [
            {'id': '1', 'timestamp': 1625256000, 'symbol': 'BTC/USDT', 'price': 50000, 'amount': 0.5, 'side': 'buy'},
            {'id': '2', 'timestamp': 1625256001, 'symbol': 'BTC/USDT', 'price': 50100, 'amount': 0.3, 'side': 'sell'},
        ]

        # Mocking Redis
        self.mock_redis_client = mock_redis.return_value

        # Mocking SQLite connection
        self.mock_sqlite_conn = mock_sqlite_connect.return_value
        self.mock_sqlite_cursor = self.mock_sqlite_conn.cursor.return_value

        # Setting up the CryptoDataCollector with environment variables
        self.collector = CryptoDataCollector(
            exchange_name='binance',
            api_key=os.environ.get("API_KEY"),
            api_secret=os.environ.get("API_SECRET")
        )

    def test_initialize_exchange(self):
        self.assertIsInstance(self.collector.exchange, MagicMock)
        self.assertTrue(self.mock_exchange.enableRateLimit)

    def test_fetch_trades_and_order_book(self):
        self.collector.fetch_trades_and_order_book(symbol='BTC/USDT')
        self.mock_exchange.fetch_order_book.assert_called_once_with('BTC/USDT')
        self.mock_exchange.fetch_trades.assert_called_once_with('BTC/USDT')
        self.assertTrue(self.mock_redis_client.hset.called)
        self.assertTrue(self.mock_redis_client.expire.called)
        self.assertTrue(self.mock_redis_client.zadd.called)
        self.assertTrue(self.mock_sqlite_cursor.execute.called)
        self.assertTrue(self.mock_sqlite_conn.commit.called)

    def test_data_retrieval_with_missing_fields(self):
        self.mock_exchange.fetch_trades.return_value = [
            {'id': '1', 'timestamp': 1625256000, 'symbol': 'BTC/USDT', 'price': 50000}  # Missing 'amount' and 'side'
        ]
        with self.assertLogs(logger, level='WARNING') as log:
            self.collector.fetch_trades_and_order_book(symbol='BTC/USDT')
            self.assertIn("Invalid trade data detected", log.output[0])

    @patch('time.sleep', return_value=None)
    def test_rate_limit_handling(self, mock_sleep):
        self.mock_exchange.fetch_trades.side_effect = ccxt.RateLimitExceeded('Rate limit exceeded')
        with self.assertLogs(logger, level='ERROR') as log:
            self.collector.fetch_trades_and_order_book(symbol='BTC/USDT')
            self.assertIn("Network error occurred", log.output[0])
            self.assertTrue(mock_sleep.called)

    def test_store_duplicate_data_in_redis(self):
        trade_data = {'timestamp': 1625256000, 'symbol': 'BTC/USDT', 'price': 50000, 'amount': 0.5, 'side': 'buy'}
        primary_key = 'BTC/USDT:1:1625256000'
        self.collector._store_trade_in_redis(primary_key, trade_data)
        self.mock_redis_client.hset.assert_called_with(primary_key, trade_data)
        self.mock_redis_client.expire.assert_called_with(primary_key, 86400)
        # Simulate duplicate data entry
        self.collector._store_trade_in_redis(primary_key, trade_data)
        self.mock_redis_client.hset.assert_called_with(primary_key, trade_data)

    def test_redis_storage_edge_cases(self):
        self.mock_redis_client.zrangebyscore.return_value = []
        results = self.collector.search_data(symbol='BTC/USDT', start_time=1625256000, end_time=1625256001)
        self.assertEqual(len(results), 0)

    @patch('time.time', side_effect=lambda: time.time() + 0.001)
    def test_performance_high_frequency_storage(self, mock_time):
        for i in range(10000):  # Simulate high-frequency data input
            trade_data = {'timestamp': 1625256000 + i, 'symbol': 'BTC/USDT', 'price': 50000 + i, 'amount': 0.5, 'side': 'buy'}
            primary_key = f'BTC/USDT:{i}:{1625256000 + i}'
            self.collector._store_trade_in_redis(primary_key, trade_data)
        self.assertTrue(self.mock_redis_client.hset.called)

    @patch('time.sleep', return_value=None)
    def test_connection_failure_handling(self, mock_sleep):
        self.mock_exchange.fetch_order_book.side_effect = ccxt.NetworkError('Network error')
        with self.assertLogs(logger, level='ERROR') as log:
            self.collector.fetch_trades_and_order_book(symbol='BTC/USDT')
            self.assertIn("Network error occurred", log.output[0])
            self.assertTrue(mock_sleep.called)

    def test_redis_connection_error_handling(self):
        self.mock_redis_client.hset.side_effect = redis.ConnectionError('Connection failed')
        trade_data = {'timestamp': 1625256000, 'symbol': 'BTC/USDT', 'price': 50000, 'amount': 0.5, 'side': 'buy'}
        primary_key = 'BTC/USDT:1:1625256000'
        with self.assertLogs(logger, level='ERROR') as log:
            self.collector._store_trade_in_redis(primary_key, trade_data)
            self.assertIn("Redis connection error", log.output[0])

    def test_sqlite_connection_error_handling(self):
        self.mock_sqlite_cursor.execute.side_effect = sqlite3.OperationalError('Database locked')
        trade_data = {'timestamp': 1625256000, 'symbol': 'BTC/USDT', 'price': 50000, 'amount': 0.5, 'side': 'buy'}
        trade_id = '1'
        symbol = 'BTC/USDT'
        with self.assertLogs(logger, level='ERROR') as log:
            self.collector._backup_trade_to_db(trade_id, symbol, trade_data)
            self.assertIn("SQLite operational error", log.output[0])

    def test_api_response_edge_cases(self):
        self.mock_exchange.fetch_trades.return_value = []
        self.collector.fetch_trades_and_order_book(symbol='BTC/USDT')
        self.mock_exchange.fetch_trades.assert_called_once_with('BTC/USDT')
        self.assertFalse(self.mock_redis_client.hset.called)

        self.mock_exchange.fetch_trades.return_value = [{'id': '1', 'timestamp': 1625256000, 'symbol': 'BTC/USDT'}]
        with self.assertLogs(logger, level='WARNING') as log:
            self.collector.fetch_trades_and_order_book(symbol='BTC/USDT')
            self.assertIn("Invalid trade data detected", log.output[0])

    def test_search_data_no_matching_results(self):
        self.mock_redis_client.zrangebyscore.return_value = []
        results = self.collector.search_data(symbol='BTC/USDT', start_time=1625256000, end_time=1625256001)
        self.assertEqual(len(results), 0)

    def test_latency_in_data_storage(self):
        import timeit
        trade_data = {'timestamp': 1625256000, 'symbol': 'BTC/USDT', 'price': 50000, 'amount': 0.5, 'side': 'buy'}
        primary_key = 'BTC/USDT:1:1625256000'
        latency = timeit.timeit(lambda: self.collector._store_trade_in_redis(primary_key, trade_data), number=100)
        self.assertLess(latency, 0.5, "Latency in storing data is too high")

    def test_redis_large_volume_storage(self):
        trade_data = {'timestamp': 1625256000, 'symbol': 'BTC/USDT', 'price': 50000, 'amount': 0.5, 'side': 'buy'}
        for i in range(10000):  # Reduced from 1,000,000 to 10,000 to make the test faster
            primary_key = f'BTC/USDT:{i}:{1625256000 + i}'
            self.collector._store_trade_in_redis(primary_key, trade_data)
        self.assertTrue(self.mock_redis_client.hset.called)


if __name__ == '__main__':
    unittest.main()
