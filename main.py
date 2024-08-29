import ccxt
import redis
import json
import time
import logging
import sqlite3
import os
import matplotlib.pyplot as plt
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from requests.exceptions import ConnectionError, Timeout


logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

class CryptoDataCollector:
    def __init__(self, exchange_name, api_key=None, api_secret=None, redis_host=os.environ.get("REDIS_HOST", "localhost"),
                 redis_port=int(os.environ.get("REDIS_PORT", 6379)), redis_db=int(os.environ.get("REDIS_DB", 0))):

        self.exchange = self._initialize_exchange(exchange_name, api_key, api_secret)
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        self.scheduler = BackgroundScheduler()
        self.backup_conn = sqlite3.connect(os.environ.get('SQLITE_DB_PATH', 'crypto_backup.db'), timeout=10)
        self._initialize_backup_db()

    def _initialize_exchange(self, exchange_name, api_key, api_secret):
        try:
            exchange_class = getattr(ccxt, exchange_name)
            exchange = exchange_class({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
            })
            logger.info(f"Connected to {exchange_name} exchange successfully.")
            return exchange
        except (ccxt.BaseError, Exception) as e:
            logger.error(f"Error initializing exchange connection: {str(e)}")
            raise

    def _initialize_backup_db(self):
        try:
            cursor = self.backup_conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS trades 
                              (trade_id TEXT, symbol TEXT, price REAL, amount REAL, side TEXT, timestamp INTEGER)''')
            self.backup_conn.commit()
            logger.info("SQLite backup database initialized successfully.")
        except sqlite3.Error as e:
            logger.error(f"Error initializing SQLite backup DB: {str(e)}")
            raise

    def fetch_trades_and_order_book(self, symbol='BTC/USDT'):
        try:
            order_book = self.exchange.fetch_order_book(symbol)
            timestamp = datetime.now().timestamp()

            trades = self.exchange.fetch_trades(symbol)

            for trade in trades:
                trade_id = trade['id']
                trade_data = {
                    'timestamp': trade['timestamp'],
                    'symbol': symbol,
                    'price': trade['price'],
                    'amount': trade['amount'],
                    'side': trade['side'],
                    'order_book_bids': json.dumps(order_book['bids']),
                    'order_book_asks': json.dumps(order_book['asks']),
                }

                if not self._validate_trade_data(trade_data):
                    logger.warning(f"Invalid trade data detected: {trade_data}")
                    continue

                primary_key = f"{symbol}:{trade_id}:{int(trade['timestamp'])}"

                self._store_trade_in_redis(primary_key, trade_data)

                sorted_set_key = f"{symbol}:trades"
                self._store_in_sorted_set(sorted_set_key, primary_key, trade['timestamp'])

                self._backup_trade_to_db(trade_id, symbol, trade_data)

                self._check_trade_conditions(trade_data)

                logger.info(f"Stored trade data for {symbol} with trade ID {trade_id}")

        except (ccxt.NetworkError, ConnectionError, Timeout) as e:
            logger.error(f"Network error occurred: {str(e)} - Retrying...")
            time.sleep(5)
            self.fetch_trades_and_order_book(symbol)
        except ccxt.BaseError as e:
            logger.error(f"Exchange error occurred: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error occurred: {str(e)}")

    def _validate_trade_data(self, trade_data):
        required_keys = ['timestamp', 'symbol', 'price', 'amount', 'side']
        return all(key in trade_data for key in required_keys)

    def _store_trade_in_redis(self, primary_key, trade_data):
        try:
            self.redis_client.hmset(primary_key, trade_data)
            self.redis_client.expire(primary_key, 86400)  # Set TTL of 24 hours
        except redis.ConnectionError as e:
            logger.error(f"Redis connection error: {str(e)} - Retrying...")
            time.sleep(5)
            self._store_trade_in_redis(primary_key, trade_data)

    def _store_in_sorted_set(self, sorted_set_key, primary_key, timestamp):
        try:
            self.redis_client.zadd(sorted_set_key, {primary_key: timestamp})
        except redis.ConnectionError as e:
            logger.error(f"Redis connection error while storing in sorted set: {str(e)} - Retrying...")
            time.sleep(5)
            self._store_in_sorted_set(sorted_set_key, primary_key, timestamp)

    def _backup_trade_to_db(self, trade_id, symbol, trade_data):
        try:
            cursor = self.backup_conn.cursor()
            cursor.execute('''INSERT INTO trades (trade_id, symbol, price, amount, side, timestamp) 
                              VALUES (?, ?, ?, ?, ?, ?)''',
                           (trade_id, symbol, trade_data['price'], trade_data['amount'], trade_data['side'], trade_data['timestamp']))
            self.backup_conn.commit()
            logger.info(f"Backed up trade data for trade ID {trade_id} to SQLite")
        except sqlite3.OperationalError as e:
            logger.error(f"SQLite operational error: {str(e)} - Retrying...")
            time.sleep(5)
            self._backup_trade_to_db(trade_id, symbol, trade_data)

    def _check_trade_conditions(self, trade_data):
        large_volume_threshold = 10.0  # Example threshold for large trade volume
        price_spike_threshold = 0.01  # Example threshold for price spike

        if trade_data['amount'] > large_volume_threshold:
            logger.info(f"Large trade detected: {trade_data['amount']} units at price {trade_data['price']}")



    def search_data(self, symbol=None, start_time=None, end_time=None):
        sorted_set_key = f"{symbol}:trades"
        results = []

        try:
            if (start_time and end_time):
                trade_keys = self.redis_client.zrangebyscore(sorted_set_key, start_time, end_time)
            else:
                trade_keys = self.redis_client.zrangebyscore(sorted_set_key, '-inf', '+inf')

            for key in trade_keys:
                results.append(self.redis_client.hgetall(key))

        except redis.ConnectionError as e:
            logger.error(f"Redis connection error during search: {str(e)}")

        return results

    def visualize_data(self, symbol, start_time=None, end_time=None):
        try:
            trades = self.search_data(symbol=symbol, start_time=start_time, end_time=end_time)
            prices = [float(trade[b'price']) for trade in trades]
            volumes = [float(trade[b'amount']) for trade in trades]
            timestamps = [datetime.fromtimestamp(int(trade[b'timestamp'])) for trade in trades]

            plt.figure(figsize=(12, 6))
            plt.subplot(2, 1, 1)
            plt.plot(timestamps, prices, label='Price')
            plt.title(f'Price Trend for {symbol}')
            plt.xlabel('Time')
            plt.ylabel('Price')
            plt.grid(True)

            plt.subplot(2, 1, 2)
            plt.bar(timestamps, volumes, label='Volume', color='orange')
            plt.title(f'Volume Trend for {symbol}')
            plt.xlabel('Time')
            plt.ylabel('Volume')
            plt.grid(True)

            plt.tight_layout()
            plt.show()

        except Exception as e:
            logger.error(f"Error during data visualization: {str(e)}")

    def start_collecting(self, symbol='BTC/USDT', interval_seconds=10):
        try:
            self.scheduler.add_job(self.fetch_trades_and_order_book, 'interval', seconds=interval_seconds, args=[symbol])
            self.scheduler.start()
        except Exception as e:
            logger.error(f"Error starting data collection: {str(e)}")

    def stop_collecting(self):
        try:
            self.scheduler.shutdown()
            self.backup_conn.close()
            logger.info("Data collection stopped and database connection closed.")
        except Exception as e:
            logger.error(f"Error stopping data collection: {str(e)}")

if __name__ == "__main__":

    api_key = os.environ.get("API_KEY")
    api_secret = os.environ.get("API_SECRET")

    collector = CryptoDataCollector(exchange_name='binance', api_key=api_key, api_secret=api_secret)
    collector.start_collecting(symbol='BTC/USDT', interval_seconds=10)


    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        collector.stop_collecting()
