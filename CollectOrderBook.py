import logging
import os
import time
from datetime import datetime
import signal
import sys
from binance.client import Client
from pymongo import MongoClient, ASCENDING

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("orderbook_collector.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

running = True
fetch_interval = 1

def signal_handler(sig, frame):
    global running
    logger.info("Shutdown signal received")
    running = False
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_mongo_client():
    try:
        client = MongoClient('mongodb://localhost:27017/')
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return None

def initialize_database():
    client = get_mongo_client()
    if not client:
        return False
    
    try:
        db = client['crypto_orderbook']
        
        collection_names = db.list_collection_names()
        if 'orderbook_deep' not in collection_names:
            db.create_collection('orderbook_deep')
        
        db.orderbook_deep.create_index([('symbol', ASCENDING)])
        db.orderbook_deep.create_index([('symbol', ASCENDING), ('timestamp', ASCENDING)])
        
        logger.info("Database initialized")
        return True
    except Exception as e:
        logger.error(f"Error initializing MongoDB: {e}")
        return False
    finally:
        client.close()

def format_orderbook_data(orderbook):
    formatted_bids = []
    formatted_asks = []
    
    for price, quantity in orderbook.get('bids', []):
        formatted_bids.append({
            'price': float(price),
            'volume': float(quantity)
        })
    
    for price, quantity in orderbook.get('asks', []):
        formatted_asks.append({
            'price': float(price),
            'volume': float(quantity)
        })
    
    return formatted_bids, formatted_asks

def store_orderbook_snapshot(symbol, orderbook):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    formatted_bids, formatted_asks = format_orderbook_data(orderbook)
    
    client = get_mongo_client()
    if not client:
        return
    
    try:
        db = client['crypto_orderbook']
        
        snapshot_doc = {
            'symbol': symbol,
            'timestamp': timestamp,
            'bids': formatted_bids,
            'asks': formatted_asks
        }
        db.orderbook_deep.insert_one(snapshot_doc)
    
    except Exception as e:
        logger.error(f"Error storing data for {symbol}: {e}")
    finally:
        client.close()

def fetch_orderbook(client, symbol, limit=1000):
    try:
        orderbook = client.get_order_book(symbol=symbol, limit=limit)
        return orderbook
    except Exception as e:
        logger.error(f"Error fetching orderbook for {symbol}: {e}")
        return None

def fetch_and_store_orderbook(client, symbol, limit=1000):
    orderbook = fetch_orderbook(client, symbol, limit)
    if orderbook:
        store_orderbook_snapshot(symbol, orderbook)
        return True
    return False

def fetch_orderbooks_with_throttling(symbols, interval):
    api_key = os.environ.get('BINANCE_API_KEY')
    api_secret = os.environ.get('BINANCE_API_SECRET')
    
    client = Client(api_key, api_secret) if api_key and api_secret else Client()
    
    logger.info(f"Starting orderbook collection for {len(symbols)} symbols")
    
    global running
    cycle_count = 0
    
    # For limit=1000 (weight=10)
    API_REQUESTS_PER_SECOND = 1.5  # Conservative value (actual limit ~2)
    
    while running:
        cycle_count += 1
        success_count = 0
        cycle_start = time.time()
        number_of_symbols = len(symbols)
        desired_sleep = interval/number_of_symbols
        min_sleep_for_rate_limit = 1/API_REQUESTS_PER_SECOND
        
        # Take the larger of the two sleep times
        sleep_time = max(desired_sleep, min_sleep_for_rate_limit)
        
        for symbol in symbols:
            if not running:
                break
                
            if fetch_and_store_orderbook(client, symbol):
                success_count += 1
            
            time.sleep(sleep_time)
        
        if cycle_count % 5 == 0:
            cycle_duration = time.time() - cycle_start
            logger.info(f"Cycle {cycle_count}: Collected {success_count}/{len(symbols)} symbols in {cycle_duration:.2f}s")
        

def validate_symbols(client, symbol_list):
    valid_symbols = []
    
    try:
        exchange_info = client.get_exchange_info()
        valid_binance_symbols = [s['symbol'] for s in exchange_info['symbols']]
        
        for symbol in symbol_list:
            if symbol in valid_binance_symbols:
                valid_symbols.append(symbol)
            else:
                logger.warning(f"Symbol {symbol} not found on Binance")
    except Exception as e:
        logger.error(f"Error validating symbols: {e}")
        return symbol_list
    
    logger.info(f"Validated {len(valid_symbols)} symbols")
    return valid_symbols

def main():
    global fetch_interval
    fetch_interval = 1
    
    base_symbols = [
        'BTC', 'ETH', 'SOL', 'BNB', 'ADA', 
        'XLM', 'DOGE', 'TRX', 'LINK', 'AVAX',
    ]
    
    symbols = [f"{base}USDT" for base in base_symbols]
    logger.info("Starting orderbook collector")
    
    try:
        if not initialize_database():
            logger.error("Database initialization failed")
            return
        
        api_key = os.environ.get('BINANCE_API_KEY')
        api_secret = os.environ.get('BINANCE_API_SECRET')
        client = Client(api_key, api_secret) if api_key and api_secret else Client()
        
        valid_symbols = validate_symbols(client, symbols)
        
        if not valid_symbols:
            logger.error("No valid symbols found")
            return
        
        fetch_orderbooks_with_throttling(valid_symbols, fetch_interval)
        
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")

if __name__ == "__main__":
    main()