from pymongo import MongoClient
from datetime import datetime
import pytz

import pandas as pd
import numpy as np


def load_data(ticker='BNB', start_date=None, end_date=None, limit=None, max_gap_seconds=60, 
              fullbook=False, num_levels=100, segment_size=10):
    """
    Load orderbook data from MongoDB with configurable processing parameters.
    
    Parameters:
    -----------
    ticker : str
        The cryptocurrency ticker symbol (e.g., 'BNB')
    start_date, end_date : datetime or str
        Date range to query data for
    limit : int, optional
        Maximum number of records to retrieve
    max_gap_seconds : int
        Maximum allowed time gap between records in a segment
    fullbook : bool
        Whether to retrieve and process the full orderbook
    num_levels : int
        Number of price levels to process in the orderbook
    segment_size : int
        Number of price levels to group into each segment
        
    Returns:
    --------
    list of DataFrame
        List of processed dataframes, each representing a continuous segment of data
    """
    num_levels = min(num_levels,1000)

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
            # Need full orderbook data but limit to requested levels for efficiency
            projection.update({'bids': {'$slice': num_levels}, 'asks': {'$slice': num_levels}})
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
                                processed_df = process_orderbook_segmented(segment_df, num_levels, segment_size)
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
                processed_df = process_orderbook_segmented(segment_df, num_levels, segment_size)
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
        ticker_data = load_data(ticker, start_date, end_date, limit, max_gap_seconds, fullbook=False, num_levels=5, segment_size=5)
        
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

def process_orderbook_segmented(df, num_levels=1000, segment_size=10):
    """
    Process orderbook data into segments and extract features.
    
    Parameters:
    -----------
    df : DataFrame
        DataFrame containing orderbook data
    num_levels : int
        Number of price levels to process
    segment_size : int
        Number of price levels to group into each segment
        
    Returns:
    --------
    DataFrame
        Processed data with extracted features
    """
    if df.empty:
        return pd.DataFrame()
    
    num_segments = (num_levels + segment_size - 1) // segment_size
    
    # Pre-allocate list with estimated size for better performance
    processed_data = []
    
    # Use vectorized operations where possible
    if 'bids' in df.columns and 'asks' in df.columns:
        # Extract best bid/ask prices for all rows at once
        df['best_bid'] = df['bids'].apply(lambda x: x[0]['price'] if x and len(x) > 0 else None)
        df['best_ask'] = df['asks'].apply(lambda x: x[0]['price'] if x and len(x) > 0 else None)
        
        # Calculate mid price and spread
        mask = (~df['best_bid'].isna()) & (~df['best_ask'].isna())
        df.loc[mask, 'mid_price'] = (df.loc[mask, 'best_bid'] + df.loc[mask, 'best_ask']) / 2
        df.loc[mask, 'spread'] = df.loc[mask, 'best_ask'] - df.loc[mask, 'best_bid']
    
    for idx, row in df.iterrows():
        if 'bids' not in row or 'asks' not in row:
            continue
            
        bids = row['bids'][:num_levels] if len(row['bids']) >= 1 else []
        asks = row['asks'][:num_levels] if len(row['asks']) >= 1 else []
        
        if not bids or not asks:
            continue
        
        new_row = {
            'timestamp': row['timestamp'],
            'mid_price': row.get('mid_price', None),
            'spread': row.get('spread', None)
        }
        
        # Level-by-level imbalance for the first few levels - limit to 50 for performance
        max_levels = min(50, min(len(bids), len(asks)))
        
        # Process level data in batches for better performance
        bid_vols = np.array([bid['volume'] for bid in bids[:max_levels]])
        ask_vols = np.array([ask['volume'] for ask in asks[:max_levels]])
        total_vols = bid_vols + ask_vols
        
        # Vectorized calculation of imbalances
        level_imbalances = np.divide(
            bid_vols, 
            total_vols, 
            out=np.ones_like(bid_vols) * 0.5, 
            where=total_vols > 0
        )
        
        # Add to new_row
        for level in range(max_levels):
            new_row[f'bid_vol_level_{level+1}'] = bid_vols[level]
            new_row[f'ask_vol_level_{level+1}'] = ask_vols[level]
            new_row[f'imbalance_level_{level+1}'] = level_imbalances[level]
        
        # Initialize segment processing variables
        total_bid_volume = 0
        total_ask_volume = 0
        bid_notional = 0
        ask_notional = 0
        
        bid_segments = {}
        ask_segments = {}
        
        # Process bid segments
        for seg in range(num_segments):
            start_idx = seg * segment_size
            end_idx = min((seg + 1) * segment_size, len(bids))
            
            if start_idx >= len(bids):
                continue
                
            segment_bids = bids[start_idx:end_idx]
            if not segment_bids:
                continue
            
            # Use numpy for faster calculations
            seg_prices = np.array([bid['price'] for bid in segment_bids])
            seg_volumes = np.array([bid['volume'] for bid in segment_bids])
            
            seg_volume = np.sum(seg_volumes)
            seg_price_weighted = np.sum(seg_prices * seg_volumes)
            
            total_bid_volume += seg_volume
            bid_notional += seg_price_weighted
            
            if seg_volume > 0:
                bid_segments[seg] = {
                    'avg_price': np.mean(seg_prices),
                    'vwap': seg_price_weighted / seg_volume,
                    'volume': seg_volume,
                    'min_price': np.min(seg_prices),
                    'max_price': np.max(seg_prices)
                }
                
                new_row[f'bid_price_segment_{seg+1}'] = bid_segments[seg]['avg_price']
                new_row[f'bid_vwap_segment_{seg+1}'] = bid_segments[seg]['vwap']
                new_row[f'bid_volume_segment_{seg+1}'] = seg_volume
        
        # Process ask segments - similar to bid segments
        for seg in range(num_segments):
            start_idx = seg * segment_size
            end_idx = min((seg + 1) * segment_size, len(asks))
            
            if start_idx >= len(asks):
                continue
                
            segment_asks = asks[start_idx:end_idx]
            if not segment_asks:
                continue
            
            # Use numpy for faster calculations
            seg_prices = np.array([ask['price'] for ask in segment_asks])
            seg_volumes = np.array([ask['volume'] for ask in segment_asks])
            
            seg_volume = np.sum(seg_volumes)
            seg_price_weighted = np.sum(seg_prices * seg_volumes)
            
            total_ask_volume += seg_volume
            ask_notional += seg_price_weighted
            
            if seg_volume > 0:
                ask_segments[seg] = {
                    'avg_price': np.mean(seg_prices),
                    'vwap': seg_price_weighted / seg_volume,
                    'volume': seg_volume,
                    'min_price': np.min(seg_prices),
                    'max_price': np.max(seg_prices)
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
        
        # Microstructure features - calculate only if enough segments exist
        if len(bid_segments) >= 3:
            # Use numpy for linear regression calculation
            x_bid = np.array([seg_data['avg_price'] for seg_data in bid_segments.values()])
            y_bid = np.array([seg_data['volume'] for seg_data in bid_segments.values()])
            
            if len(x_bid) > 1:  # Need at least 2 points for regression
                bid_slope = np.polyfit(x_bid, y_bid, 1)[0]
                new_row['bid_slope'] = bid_slope
        
        if len(ask_segments) >= 3:
            x_ask = np.array([seg_data['avg_price'] for seg_data in ask_segments.values()])
            y_ask = np.array([seg_data['volume'] for seg_data in ask_segments.values()])
            
            if len(x_ask) > 1:  # Need at least 2 points for regression
                ask_slope = np.polyfit(x_ask, y_ask, 1)[0]
                new_row['ask_slope'] = ask_slope
        
        # Book pressure ratio (top 3 segments)
        top_bid_volume = sum(bid_segments[seg]['volume'] for seg in range(min(3, len(bid_segments))) if seg in bid_segments)
        top_ask_volume = sum(ask_segments[seg]['volume'] for seg in range(min(3, len(ask_segments))) if seg in ask_segments)
        
        if top_ask_volume > 0:
            new_row['book_pressure_ratio'] = top_bid_volume / top_ask_volume
        
        # Liquidity concentration
        if total_bid_volume > 0:
            new_row['bid_liquidity_top3_pct'] = top_bid_volume / total_bid_volume
        if total_ask_volume > 0:
            new_row['ask_liquidity_top3_pct'] = top_ask_volume / total_ask_volume
        
        processed_data.append(new_row)
    
    result_df = pd.DataFrame(processed_data)
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