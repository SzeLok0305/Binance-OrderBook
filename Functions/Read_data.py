from pymongo import MongoClient
from datetime import datetime
import pytz

import pandas as pd
import numpy as np


def load_data(ticker='BNB', start_date=None, end_date=None, limit=None, max_gap_seconds=60, fullbook=False):

    # Convert string dates to datetime
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    
    # Set default dates
    if start_date is None:
        start_date = datetime(2025, 1, 1)
    if end_date is None:
        end_date = datetime.now()
    
    # Ensure timezone info
    utc = pytz.UTC
    if start_date.tzinfo is None:
        start_date = utc.localize(start_date)
    if end_date.tzinfo is None:
        end_date = utc.localize(end_date)
    
    ticker = f"{ticker}USDT"
    
    try:
        # Optimize MongoDB connection
        client = MongoClient('mongodb://localhost:27017/', 
                             maxPoolSize=50,
                             connectTimeoutMS=30000,
                             socketTimeoutMS=30000)
        db = client['crypto_orderbook']
        collection = db['orderbook_deep']
        
        # Ensure proper indexes exist (uncomment if you need to create them)
        # collection.create_index([('symbol', 1), ('timestamp', 1)])
        
        # Format dates for query
        start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
        
        query = {
            'symbol': ticker,
            'timestamp': {
                '$gte': start_date_str,
                '$lte': end_date_str
            }
        }
        
        # Use projection to only retrieve necessary fields
        projection = {'timestamp': 1, 'symbol': 1}
        if fullbook:
            # Need full orderbook data
            projection.update({'bids': 1, 'asks': 1})
        else:
            # Only need first few levels for midprice extraction
            projection.update({'bids': {'$slice': 1}, 'asks': {'$slice': 1}})
        
        # Process in optimized batches
        batch_size = 10000
        all_processed_dfs = []
        current_segment = []
        last_timestamp = None
        total_records = 0
        
        # Sort by timestamp for consistent results
        cursor = collection.find(query, projection).sort('timestamp', 1)
        if limit:
            cursor = cursor.limit(limit)
        
        # Set larger batch size for network efficiency
        cursor = cursor.batch_size(1000)
        
        # Process in streaming batches to avoid memory issues
        for record in cursor:
            total_records += 1
            
            try:
                # Convert timestamp to datetime
                timestamp = pd.to_datetime(record['timestamp'])
                
                # Check for time gaps
                if last_timestamp is not None:
                    time_diff = (timestamp - last_timestamp).total_seconds()
                    if time_diff > max_gap_seconds:
                        # Process current segment if large enough
                        if len(current_segment) >= 360:  # half-hour data
                            segment_df = pd.DataFrame(current_segment)
                            segment_df = segment_df.sort_values('timestamp')
                            
                            if fullbook:
                                processed_df = process_orderbook_segmented(segment_df)
                            else:
                                # No need for full processing, we already extracted midprice
                                processed_df = segment_df
                            
                            all_processed_dfs.append(processed_df)
                        
                        # Start a new segment
                        current_segment = []
                
                # Extract data based on mode
                bids = record.get('bids', [])
                asks = record.get('asks', [])
                
                if not bids or not asks:
                    continue
                
                best_bid = bids[0]['price']
                best_ask = asks[0]['price']
                mid_price = (best_bid + best_ask) / 2
                spread = best_ask - best_bid
                
                # Create a simplified record with just what we need
                new_record = {
                    'timestamp': timestamp,
                    'symbol': record.get('symbol'),
                    'mid_price': mid_price,
                    'spread': spread
                }
                
                # If we need full orderbook data, keep original record for later processing
                if fullbook:
                    new_record['bids'] = bids
                    new_record['asks'] = asks
                
                current_segment.append(new_record)
                last_timestamp = timestamp
                
                # Output progress for large datasets
                if total_records % batch_size == 0:
                    print(f"Processed {total_records} records...")
                
            except (KeyError, IndexError, TypeError) as e:
                print(f"Skipping record due to: {e}")
                continue
        
        # Process the final segment
        if len(current_segment) >= 360:
            segment_df = pd.DataFrame(current_segment)
            segment_df = segment_df.sort_values('timestamp')
            
            if fullbook:
                processed_df = process_orderbook_segmented(segment_df)
            else:
                # No need for full processing, we already extracted midprice
                processed_df = segment_df
            
            all_processed_dfs.append(processed_df)
        
        client.close()
        
        if not all_processed_dfs:
            print(f"No valid data segments found for {ticker} between {start_date} and {end_date}")
            return []
        
        print(f"Split data into {len(all_processed_dfs)} continuous segments")
        print(f"Total records processed: {total_records}")
        return all_processed_dfs
        
    except Exception as e:
        print(f"Error accessing MongoDB: {e}")
        import traceback
        traceback.print_exc()
        return []
    
def load_multiple_tickers(tickers=None, start_date=None, end_date=None, limit=None, max_gap_seconds=60, time_threshold=1.0):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['crypto_orderbook']
    collection = db['orderbook_deep']
    
    if tickers == 'All':
        all_symbols = collection.distinct('symbol')
        tickers = [symbol.replace('USDT', '') for symbol in all_symbols]
    elif not isinstance(tickers, list):
        tickers = [tickers]
    
    ticker_segments = {}
    max_segments = 0
    
    for ticker in tickers:
        print(f"Loading data for {ticker}...")
        ticker_data = load_data(ticker, start_date, end_date, limit, max_gap_seconds, fullbook=False)
        
        if ticker_data:
            ticker_segments[ticker] = []
            for segment_df in ticker_data:
                segment_df = segment_df.set_index('timestamp')
                if 'mid_price' in segment_df.columns:
                    ticker_segments[ticker].append(segment_df[['mid_price']])
            
            max_segments = max(max_segments, len(ticker_segments[ticker]))
            print(f"Loaded {len(ticker_segments[ticker])} segments for {ticker}")
    
    if not ticker_segments:
        print("No data found for any of the specified tickers")
        return []
    
    aligned_segments = []
    
    for i in range(max_segments):
        segment_dfs = []
        
        for ticker in ticker_segments:
            if i < len(ticker_segments[ticker]):
                segment_df = ticker_segments[ticker][i].copy()
                segment_df.columns = [ticker]
                segment_dfs.append(segment_df)
        
        if segment_dfs:
            print(f"Aligning segment {i+1}/{max_segments} with {len(segment_dfs)} tickers")
            combined_df = pd.concat(segment_dfs, axis=1)
            aligned_df = align_by_time_proximity(combined_df, time_threshold)
            
            if not aligned_df.empty:
                aligned_segments.append(aligned_df)
    
    print(f"Created {len(aligned_segments)} aligned segments")
    return aligned_segments

def align_by_time_proximity(df, time_threshold=8.0):
    df_reset = df.reset_index()
    timestamps = df_reset['timestamp'].drop_duplicates().sort_values().to_list()
    
    # Debug information
    print(f"Processing {len(timestamps)} unique timestamps with threshold {time_threshold} seconds")
    
    if len(timestamps) <= 1:
        return df
    
    # Group timestamps into clusters
    clusters = []
    current_cluster = [timestamps[0]]
    
    for i in range(1, len(timestamps)):
        current_ts = timestamps[i]
        prev_ts = current_cluster[-1]
        time_diff = (current_ts - prev_ts).total_seconds()
        
        if time_diff <= time_threshold:
            current_cluster.append(current_ts)
        else:
            clusters.append(current_cluster)
            current_cluster = [current_ts]
    
    # Add the last cluster
    if current_cluster:
        clusters.append(current_cluster)
    
    print(f"Formed {len(clusters)} clusters from {len(timestamps)} timestamps")
    
    # Process each cluster
    aligned_data = []
    
    for cluster in clusters:
        if len(cluster) > 1:
            # Calculate average timestamp for the cluster
            avg_ts = pd.Timestamp(sum(t.value for t in cluster) // len(cluster))
            
            # Debug info
            time_diffs = [(cluster[i+1] - cluster[i]).total_seconds() for i in range(len(cluster)-1)]
            
            data_point = {'timestamp': avg_ts}
            for ticker in df.columns:
                values = []
                for ts in cluster:
                    val = df_reset.loc[df_reset['timestamp'] == ts, ticker].values
                    if len(val) > 0 and not pd.isna(val[0]):
                        values.append(val[0])
                
                if values:
                    data_point[ticker] = sum(values) / len(values)
                else:
                    data_point[ticker] = np.nan
            
            aligned_data.append(data_point)
        else:
            # Only one timestamp in cluster
            single_ts = cluster[0]
            data_point = {'timestamp': single_ts}
            for ticker in df.columns:
                val = df_reset.loc[df_reset['timestamp'] == single_ts, ticker].values
                data_point[ticker] = val[0] if len(val) > 0 else np.nan
            
            aligned_data.append(data_point)
    
    if not aligned_data:
        return pd.DataFrame()
    
    aligned_df = pd.DataFrame(aligned_data)
    aligned_df = aligned_df.set_index('timestamp')
    
    return aligned_df

################################################################################################################################################################################################################################################
    
def extract_midprice(df):
    """Extract only the midprice from orderbook data for faster processing."""
    if df.empty:
        return pd.DataFrame()
    
    processed_data = []
    
    for idx, row in df.iterrows():
        bids = row['bids']
        asks = row['asks']
        
        new_row = {}
        new_row['timestamp'] = row['timestamp']
        new_row['symbol'] = row['symbol'] if 'symbol' in row else None
        
        if not bids or not asks:
            processed_data.append(new_row)
            continue
        
        best_bid = bids[0]['price']
        best_ask = asks[0]['price']
        mid_price = (best_bid + best_ask) / 2
        spread = best_ask - best_bid
        
        new_row['mid_price'] = mid_price
        new_row['spread'] = spread
        
        processed_data.append(new_row)
    
    result_df = pd.DataFrame(processed_data)
    return result_df

def process_orderbook_segmented(df, num_levels=1000, segment_size=10, time_window=5):
    if df.empty:
        return pd.DataFrame()
    
    num_segments = (num_levels + segment_size - 1) // segment_size
    
    processed_data = []
    
    for idx, row in df.iterrows():
        bids = row['bids'][:num_levels] if len(row['bids']) >= num_levels else row['bids']
        asks = row['asks'][:num_levels] if len(row['asks']) >= num_levels else row['asks']
        
        new_row = {}
        new_row['timestamp'] = row['timestamp']
        
        if not bids or not asks:
            processed_data.append(new_row)
            continue
        
        best_bid = bids[0]['price']
        best_ask = asks[0]['price']
        mid_price = (best_bid + best_ask) / 2
        spread = best_ask - best_bid
        
        new_row['mid_price'] = mid_price
        new_row['spread'] = spread
        
        # Level-by-level imbalance for the first few levels
        for level in range(min(50, min(len(bids), len(asks)))):
            bid_vol = bids[level]['volume']
            ask_vol = asks[level]['volume']
            total_vol = bid_vol + ask_vol
            
            new_row[f'bid_vol_level_{level+1}'] = bid_vol
            new_row[f'ask_vol_level_{level+1}'] = ask_vol
            
            if total_vol > 0:
                level_imbalance = bid_vol / total_vol
                new_row[f'imbalance_level_{level+1}'] = level_imbalance
            else:
                new_row[f'imbalance_level_{level+1}'] = 0.5
        
        total_bid_volume = 0
        total_ask_volume = 0
        bid_notional = 0
        ask_notional = 0
        
        bid_segments = {}
        ask_segments = {}
        
        for seg in range(num_segments):
            start_idx = seg * segment_size
            end_idx = min((seg + 1) * segment_size, len(bids))
            
            if start_idx >= len(bids):
                continue
                
            segment_bids = bids[start_idx:end_idx]
            if not segment_bids:
                continue
                
            seg_volume = 0
            seg_price_weighted = 0
            seg_prices = []
            
            for bid in segment_bids:
                price = bid['price']
                volume = bid['volume']
                
                seg_volume += volume
                seg_price_weighted += price * volume
                seg_prices.append(price)
            
            total_bid_volume += seg_volume
            bid_notional += seg_price_weighted
            
            if seg_volume > 0:
                bid_segments[seg] = {
                    'avg_price': sum(seg_prices) / len(seg_prices),
                    'vwap': seg_price_weighted / seg_volume,
                    'volume': seg_volume,
                    'min_price': min(seg_prices),
                    'max_price': max(seg_prices)
                }
                
                new_row[f'bid_price_segment_{seg+1}'] = bid_segments[seg]['avg_price']
                new_row[f'bid_vwap_segment_{seg+1}'] = bid_segments[seg]['vwap']
                new_row[f'bid_volume_segment_{seg+1}'] = seg_volume
        
        for seg in range(num_segments):
            start_idx = seg * segment_size
            end_idx = min((seg + 1) * segment_size, len(asks))
            
            if start_idx >= len(asks):
                continue
                
            segment_asks = asks[start_idx:end_idx]
            if not segment_asks:
                continue
                
            seg_volume = 0
            seg_price_weighted = 0
            seg_prices = []
            
            for ask in segment_asks:
                price = ask['price']
                volume = ask['volume']
                
                seg_volume += volume
                seg_price_weighted += price * volume
                seg_prices.append(price)
            
            total_ask_volume += seg_volume
            ask_notional += seg_price_weighted
            
            if seg_volume > 0:
                ask_segments[seg] = {
                    'avg_price': sum(seg_prices) / len(seg_prices),
                    'vwap': seg_price_weighted / seg_volume,
                    'volume': seg_volume,
                    'min_price': min(seg_prices),
                    'max_price': max(seg_prices)
                }
                
                new_row[f'ask_price_segment_{seg+1}'] = ask_segments[seg]['avg_price']
                new_row[f'ask_vwap_segment_{seg+1}'] = ask_segments[seg]['vwap']
                new_row[f'ask_volume_segment_{seg+1}'] = seg_volume
        
        # Calculate segment-level imbalances
        for seg in range(num_segments):
            if seg in bid_segments and seg in ask_segments:
                bid_vol = bid_segments[seg]['volume']
                ask_vol = ask_segments[seg]['volume']
                total_vol = bid_vol + ask_vol
                
                if total_vol > 0:
                    segment_imbalance = bid_vol / total_vol
                    new_row[f'imbalance_segment_{seg+1}'] = segment_imbalance
        
        # Order Book Depth
        new_row['total_bid_volume'] = total_bid_volume
        new_row['total_ask_volume'] = total_ask_volume
        new_row['total_volume'] = total_bid_volume + total_ask_volume
        
        # Volume Imbalance
        if total_bid_volume + total_ask_volume > 0:
            new_row['volume_imbalance'] = (total_bid_volume) / (total_bid_volume + total_ask_volume)
        else:
            new_row['volume_imbalance'] = 0
        
        # Liquidity metrics
        if total_bid_volume > 0:
            new_row['bid_vwap'] = bid_notional / total_bid_volume
        if total_ask_volume > 0:
            new_row['ask_vwap'] = ask_notional / total_ask_volume
        
        # Microstructure features
        
        # Order book slope (linear regression)
        if len(bid_segments) >= 3:
            x_bid = []
            y_bid = []
            for seg_idx, seg_data in bid_segments.items():
                x_bid.append(seg_data['avg_price'])
                y_bid.append(seg_data['volume'])
            
            if x_bid and y_bid:
                x_bid_mean = sum(x_bid) / len(x_bid)
                y_bid_mean = sum(y_bid) / len(y_bid)
                
                numerator = sum((x - x_bid_mean) * (y - y_bid_mean) for x, y in zip(x_bid, y_bid))
                denominator = sum((x - x_bid_mean) ** 2 for x in x_bid)
                
                if denominator != 0:
                    bid_slope = numerator / denominator
                    new_row['bid_slope'] = bid_slope
        
        if len(ask_segments) >= 3:
            x_ask = []
            y_ask = []
            for seg_idx, seg_data in ask_segments.items():
                x_ask.append(seg_data['avg_price'])
                y_ask.append(seg_data['volume'])
            
            if x_ask and y_ask:
                x_ask_mean = sum(x_ask) / len(x_ask)
                y_ask_mean = sum(y_ask) / len(y_ask)
                
                numerator = sum((x - x_ask_mean) * (y - y_ask_mean) for x, y in zip(x_ask, y_ask))
                denominator = sum((x - x_ask_mean) ** 2 for x in x_ask)
                
                if denominator != 0:
                    ask_slope = numerator / denominator
                    new_row['ask_slope'] = ask_slope
        
        # Book pressure ratio (top 3 segments)
        top_bid_volume = sum(bid_segments[seg]['volume'] for seg in range(min(3, len(bid_segments))) if seg in bid_segments)
        top_ask_volume = sum(ask_segments[seg]['volume'] for seg in range(min(3, len(ask_segments))) if seg in ask_segments)
        
        if top_ask_volume > 0:
            new_row['book_pressure_ratio'] = top_bid_volume / top_ask_volume
        
        # Distance to large orders
        large_bid_distance = find_large_order_distance(bids, best_bid, 'bid')
        large_ask_distance = find_large_order_distance(asks, best_ask, 'ask')
        
        if large_bid_distance:
            new_row['large_bid_distance_bps'] = abs(large_bid_distance / mid_price * 10000)
        if large_ask_distance:
            new_row['large_ask_distance_bps'] = abs(large_ask_distance / mid_price * 10000)
        
        # Liquidity concentration
        if total_bid_volume > 0:
            new_row['bid_liquidity_top3_pct'] = top_bid_volume / total_bid_volume
        if total_ask_volume > 0:
            new_row['ask_liquidity_top3_pct'] = top_ask_volume / total_ask_volume
        
        processed_data.append(new_row)
    
    result_df = pd.DataFrame(processed_data)

    return result_df

def Technical_indicator(result_df,time_window):
    
    # Add time-series features
    if len(result_df) > 1:
        # Returns
        if 'mid_price' in result_df.columns:
            result_df['return'] = result_df['mid_price'].pct_change()
        
        # Volatility (rolling standard deviation of returns)
        if 'return' in result_df.columns and len(result_df) > time_window:
            result_df['volatility'] = result_df['return'].rolling(time_window).std()
        
        # Momentum (cumulative return over window)
        if 'return' in result_df.columns and len(result_df) > time_window:
            result_df['momentum'] = result_df['return'].rolling(time_window).sum()
        
        # Imbalance trend
        if 'volume_imbalance' in result_df.columns and len(result_df) > time_window:
            result_df['imbalance_trend'] = result_df['volume_imbalance'].diff()

    return result_df


def find_large_order_distance(orders, best_price, side):
    large_threshold = sum(order['volume'] for order in orders) * 0.05  # 5% of total volume
    
    for order in orders:
        if order['volume'] > large_threshold:
            if side == 'bid':
                return best_price - order['price']
            else:
                return order['price'] - best_price
    
    return None