from pymongo import MongoClient
from datetime import datetime
import pytz

import pandas as pd


def load_data(ticker='BNB', start_date=None, end_date=None, limit=None, max_gap_seconds=15):
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    
    if start_date is None:
        start_date = datetime(2025, 1, 1)
    if end_date is None:
        end_date = datetime.now()
    
    utc = pytz.UTC
    if start_date.tzinfo is None:
        start_date = utc.localize(start_date)
    if end_date.tzinfo is None:
        end_date = utc.localize(end_date)
    
    ticker = f"{ticker}USDT"
    
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client['crypto_orderbook']
        collection = db['orderbook_deep']
        
        query = {
            'symbol': ticker,
            'timestamp': {
                '$gte': start_date.strftime('%Y-%m-%d %H:%M:%S'),
                '$lte': end_date.strftime('%Y-%m-%d %H:%M:%S')
            }
        }
        
        if limit:
            cursor = collection.find(query).limit(limit)
        else:
            cursor = collection.find(query)
        
        records = list(cursor)
        client.close()
        
        if not records:
            print(f"No data found for {ticker} between {start_date} and {end_date}")
            return []
        
        df = pd.DataFrame(records)
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')
        
        # Calculate time differences between consecutive rows
        df['time_diff'] = df['timestamp'].diff().dt.total_seconds()
        
        # Find gaps where time difference exceeds the max_gap_seconds
        gap_indices = df[df['time_diff'] > max_gap_seconds].index.tolist()
        
        # Split the dataframe at the gap points
        dfs = []
        if gap_indices:
            start_idx = 0
            for idx in gap_indices:
                chunk = df.iloc[start_idx:idx].copy()
                if len(chunk) >= 360: # haft-hour data
                    processd_chuck = process_orderbook_segmented(chunk)
                    dfs.append(processd_chuck)
                start_idx = idx
            
            # Don't forget the last chunk
            last_chunk = df.iloc[start_idx:].copy()
            if len(last_chunk) >= 360: # haft-hour data
                processd_last_chunk = process_orderbook_segmented(last_chunk)
                dfs.append(processd_last_chunk)
        else:
            # No gaps found, process the entire dataframe
            df_copy = process_orderbook_segmented(df)
            dfs.append(df_copy)
        
        print(f"Split data into {len(dfs)} continuous segments")
        return dfs
        
    except Exception as e:
        print(f"Error accessing MongoDB: {e}")
        return []
    

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