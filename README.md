Overview
The Crypto Data Collector is a Python-based tool designed to connect to a cryptocurrency exchange, fetch real-time trade and order book data, store the data in Redis for quick access, back up the data to an SQLite database for persistence, and visualize the collected data. The script is highly configurable through environment variables, making it adaptable to various deployment environments.

Features
Real-Time Data Collection: Connects to a specified cryptocurrency exchange (e.g., Binance) and fetches live trade and order book data.
Data Storage:
Redis: For fast, temporary storage with a 24-hour TTL (Time to Live).
SQLite: For persistent storage, ensuring data is not lost.
Data Visualization: Generates plots for price trends and volume trends over time using Matplotlib.
Automated Scheduling: Uses APScheduler to periodically fetch data at specified intervals.
Error Handling: Robust error handling and retry logic for network issues and database operations.
Configurable: Environment variables allow for easy configuration of exchange API keys, Redis connection details, log levels, and more.

Requirements
Python 3.x
Required Python libraries (can be installed via pip):
ccxt
redis
sqlite3 (included in Python standard library)
matplotlib
apscheduler
requests
python-dotenv
