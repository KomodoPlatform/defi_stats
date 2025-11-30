#!/usr/bin/env python3
"""
Script to extract all swaps that included SMTF and SFUSD coins and export them by year
"""
import os
import sys
import json
import csv
from datetime import datetime, timezone
from collections import defaultdict

# Add the API root to the path to import the existing modules
API_ROOT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'api')
sys.path.append(API_ROOT_PATH)

def check_environment():
    """
    Check if required environment variables are set
    """
    # Load .env file if it exists
    try:
        from dotenv import load_dotenv
        env_path = os.path.join(API_ROOT_PATH, '.env')
        if os.path.exists(env_path):
            load_dotenv(env_path)
            print(f"Loaded environment from {env_path}")
        else:
            print(f"No .env file found at {env_path}")
    except ImportError:
        print("python-dotenv not available, checking system environment variables...")
    
    # Check for required database variables
    required_vars = {
        'PostgreSQL': ['POSTGRES_HOST', 'POSTGRES_USERNAME', 'POSTGRES_PASSWORD', 'POSTGRES_PORT'],
        'MySQL': ['MYSQL_HOST', 'MYSQL_USERNAME', 'MYSQL_PASSWORD', 'MYSQL_DATABASE'],
        'Local SQLite': ['LOCAL_MM2_DB_PATH_7777', 'LOCAL_MM2_DB_PATH_8762']
    }
    
    print("\nChecking environment variables:")
    available_dbs = []
    
    for db_type, vars_list in required_vars.items():
        missing = [var for var in vars_list if not os.getenv(var)]
        if not missing:
            available_dbs.append(db_type)
            print(f"✓ {db_type}: All variables set")
        else:
            print(f"✗ {db_type}: Missing {', '.join(missing)}")
    
    if not available_dbs:
        print("\n❌ No database configurations found!")
        print("\nPlease set up one of the following:")
        print("1. PostgreSQL: POSTGRES_HOST, POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_PORT")
        print("2. MySQL: MYSQL_HOST, MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_DATABASE")
        print("3. SQLite: LOCAL_MM2_DB_PATH_7777, LOCAL_MM2_DB_PATH_8762")
        print("\nYou can create an api/.env file or set these as environment variables.")
        return False
    
    print(f"\n✓ Available databases: {', '.join(available_dbs)}")
    return True

# Import the existing database modules
try:
    import db.sqldb as db
    from util.logger import logger
    
    os.environ["IS_TESTING"] = "False"
except ImportError as e:
    print(f"Error importing required modules: {e}")
    print("Make sure you're running this from the defi_stats root directory")
    print("and that all dependencies are installed.")
    sys.exit(1)

def get_coin_swaps(coin_ticker):
    """
    Get all swaps that include the specified coin (either as maker or taker)
    """
    try:
        # Initialize database connection using the existing infrastructure
        query = db.SqlQuery()
        
        print(f"Querying {coin_ticker} swaps...")
        
        # Get all swaps for the coin (this will include all variants)
        # Using a very wide time range to get all historical data
        start_time = 1577836800  # January 1, 2020
        end_time = int(datetime.now().timestamp())  # Current time
        
        result = query.get_swaps_for_coin(
            coin=coin_ticker,
            start_time=start_time,
            end_time=end_time,
            success_only=False,  # Include both successful and failed swaps
            all_variants=True    # Include all coin variants
        )
        
        if isinstance(result, dict) and "error" in result:
            print(f"Error querying database: {result['error']}")
            return []
            
        print(f"Found {len(result)} {coin_ticker} swaps")
        return result
        
    except Exception as e:
        print(f"Error retrieving {coin_ticker} swaps: {e}")
        print("This could be due to:")
        print("1. Database connection issues")
        print("2. Missing environment variables")
        print("3. Database not running")
        return []

def group_swaps_by_year(swaps):
    """
    Group swaps by year based on the finished_at timestamp
    """
    swaps_by_year = defaultdict(list)
    
    for swap in swaps:
        # Use finished_at timestamp, fall back to started_at if not available
        timestamp = swap.get('finished_at', swap.get('started_at', 0))
        
        if timestamp:
            # Convert timestamp to datetime
            dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            year = dt.year
            swaps_by_year[year].append(swap)
    
    return dict(swaps_by_year)

def analyze_year_summary(year_swaps, year, target_coin):
    """
    Create detailed analysis summary for a year of swaps for a specific coin
    """
    if not year_swaps:
        return {}
    
    # Get date range
    timestamps = []
    for swap in year_swaps:
        timestamp = swap.get('finished_at', swap.get('started_at', 0))
        if timestamp:
            timestamps.append(timestamp)
    
    if not timestamps:
        return {}
    
    start_timestamp = min(timestamps)
    end_timestamp = max(timestamps)
    start_date = datetime.fromtimestamp(start_timestamp, tz=timezone.utc).strftime("%d/%m/%y")
    end_date = datetime.fromtimestamp(end_timestamp, tz=timezone.utc).strftime("%d/%m/%y")
    
    # Analyze pairs and volumes
    pairs_analysis = defaultdict(lambda: {
        'maker_volumes': defaultdict(float),
        'taker_volumes': defaultdict(float), 
        'prices': [],
        'swap_count': 0
    })
    
    for swap in year_swaps:
        pair_std = swap.get('pair_std', '')
        if not pair_std:
            continue
            
        maker_coin = swap.get('maker_coin_ticker', '')
        taker_coin = swap.get('taker_coin_ticker', '')
        maker_amount = float(swap.get('maker_amount', 0))
        taker_amount = float(swap.get('taker_amount', 0))
        price = float(swap.get('price', 0))
        
        pairs_analysis[pair_std]['maker_volumes'][maker_coin] += maker_amount
        pairs_analysis[pair_std]['taker_volumes'][taker_coin] += taker_amount
        if price > 0:
            pairs_analysis[pair_std]['prices'].append(price)
        pairs_analysis[pair_std]['swap_count'] += 1
    
    # Format pairs summary
    pairs_summary = {}
    for pair, data in pairs_analysis.items():
        # Calculate total volumes and average price
        total_maker_volume = sum(data['maker_volumes'].values())
        total_taker_volume = sum(data['taker_volumes'].values())
        avg_price = sum(data['prices']) / len(data['prices']) if data['prices'] else 0
        
        # Determine which coin is the target coin to calculate price per target coin
        target_volume = 0
        other_coin = ''
        other_volume = 0
        
        if target_coin in data['maker_volumes']:
            target_volume = data['maker_volumes'][target_coin]
            # Find the other coin
            for coin, vol in data['taker_volumes'].items():
                if coin != target_coin:
                    other_coin = coin
                    other_volume = vol
                    break
        elif target_coin in data['taker_volumes']:
            target_volume = data['taker_volumes'][target_coin]
            # Find the other coin
            for coin, vol in data['maker_volumes'].items():
                if coin != target_coin:
                    other_coin = coin
                    other_volume = vol
                    break
        
        # Calculate average price per target coin
        price_per_target = other_volume / target_volume if target_volume > 0 else 0
        
        pairs_summary[pair] = {
            target_coin: round(target_volume, 8),
            other_coin: round(other_volume, 8),
            f'average_price_per_{target_coin}': round(price_per_target, 8),
            'swap_count': data['swap_count']
        }
    
    return {
        'swaps_count': len(year_swaps),
        'start_date': start_date,
        'end_date': end_date,
        'pairs': pairs_summary,
        'results': year_swaps
    }

def export_swaps_to_csv(swaps, filename):
    """
    Export swaps to CSV format
    """
    if not swaps:
        print(f"No swaps to export for {filename}")
        return
    
    fieldnames = [
        'uuid', 'pair', 'started_at', 'finished_at', 'duration',
        'maker_coin', 'maker_coin_ticker', 'maker_amount', 'maker_coin_usd_price',
        'taker_coin', 'taker_coin_ticker', 'taker_amount', 'taker_coin_usd_price',
        'price', 'reverse_price', 'is_success', 'maker_gui', 'taker_gui',
        'maker_version', 'taker_version', 'maker_pubkey', 'taker_pubkey'
    ]
    
    # Create exports directory if it doesn't exist
    os.makedirs('exports', exist_ok=True)
    filepath = os.path.join('exports', filename)
    
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        
        for swap in swaps:
            # Convert timestamps to readable format for CSV
            row = swap.copy()
            for timestamp_field in ['started_at', 'finished_at']:
                if timestamp_field in row and row[timestamp_field]:
                    dt = datetime.fromtimestamp(row[timestamp_field], tz=timezone.utc)
                    row[f'{timestamp_field}_readable'] = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
            
            writer.writerow(row)
    
    print(f"Exported {len(swaps)} swaps to {filepath}")

def export_swaps_to_json(swaps, filename):
    """
    Export swaps to JSON format
    """
    if not swaps:
        print(f"No swaps to export for {filename}")
        return
    
    # Create exports directory if it doesn't exist
    os.makedirs('exports', exist_ok=True)
    filepath = os.path.join('exports', filename)
    
    # Add readable timestamps for JSON export
    swaps_with_readable_dates = []
    for swap in swaps:
        swap_copy = swap.copy()
        for timestamp_field in ['started_at', 'finished_at']:
            if timestamp_field in swap_copy and swap_copy[timestamp_field]:
                dt = datetime.fromtimestamp(swap_copy[timestamp_field], tz=timezone.utc)
                swap_copy[f'{timestamp_field}_readable'] = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
        swaps_with_readable_dates.append(swap_copy)
    
    with open(filepath, 'w', encoding='utf-8') as jsonfile:
        json.dump(swaps_with_readable_dates, jsonfile, indent=2, default=str)
    
    print(f"Exported {len(swaps)} swaps to {filepath}")

def export_summary_to_json(summary, filename):
    """
    Export enhanced summary to JSON format
    """
    if not summary:
        print(f"No summary to export for {filename}")
        return
    
    # Create exports directory if it doesn't exist
    os.makedirs('exports', exist_ok=True)
    filepath = os.path.join('exports', filename)
    
    # Clean up the results data for export (remove SQLAlchemy artifacts)
    clean_summary = summary.copy()
    if 'results' in clean_summary:
        clean_results = []
        for swap in clean_summary['results']:
            clean_swap = {k: v for k, v in swap.items() if not k.startswith('_sa_')}
            # Add readable timestamps
            for timestamp_field in ['started_at', 'finished_at']:
                if timestamp_field in clean_swap and clean_swap[timestamp_field]:
                    dt = datetime.fromtimestamp(clean_swap[timestamp_field], tz=timezone.utc)
                    clean_swap[f'{timestamp_field}_readable'] = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
            clean_results.append(clean_swap)
        clean_summary['results'] = clean_results
    
    with open(filepath, 'w', encoding='utf-8') as jsonfile:
        json.dump(clean_summary, jsonfile, indent=2, default=str)
    
    print(f"Exported summary with {summary.get('swaps_count', 0)} swaps to {filepath}")

def print_summary(coin_ticker, summaries_by_year):
    """
    Print an enhanced summary of swaps by year for a specific coin
    """
    print("\n" + "="*60)
    print(f"{coin_ticker} SWAPS DETAILED SUMMARY")
    print("="*60)
    
    total_swaps = sum(summary.get('swaps_count', 0) for summary in summaries_by_year.values())
    print(f"Total {coin_ticker} swaps found: {total_swaps}")
    
    for year in sorted(summaries_by_year.keys()):
        summary = summaries_by_year[year]
        print(f"\n📅 {year} ({summary['start_date']} - {summary['end_date']}):")
        print(f"   Swaps: {summary['swaps_count']}")
        
        if summary.get('pairs'):
            print("   Trading Pairs:")
            for pair, data in summary['pairs'].items():
                target_vol = data.get(coin_ticker, 0)
                avg_price_key = f'average_price_per_{coin_ticker}'
                avg_price = data.get(avg_price_key, 0)
                swap_count = data.get('swap_count', 0)
                
                # Find the other coin (not the target coin)
                other_coin = ''
                other_vol = 0
                for key, value in data.items():
                    if key not in [coin_ticker, avg_price_key, 'swap_count']:
                        other_coin = key
                        other_vol = value
                        break
                
                print(f"     {pair}: {swap_count} swaps")
                print(f"       • {coin_ticker} Volume: {target_vol:,.2f}")
                print(f"       • {other_coin} Volume: {other_vol:,.2f}")
                print(f"       • Average Price per {coin_ticker}: {avg_price:.6f} {other_coin}")
    
    print("="*60)

def process_coin(coin_ticker):
    """
    Process swaps for a specific coin and export results
    """
    print(f"\n{'='*50}")
    print(f"Processing {coin_ticker} swaps...")
    print('='*50)
    
    # Get all swaps for this coin
    swaps = get_coin_swaps(coin_ticker)
    
    if not swaps:
        print(f"No {coin_ticker} swaps found or error occurred.")
        return
    
    # Group swaps by year
    swaps_by_year = group_swaps_by_year(swaps)
    
    # Create enhanced summaries for each year
    summaries_by_year = {}
    for year, year_swaps in swaps_by_year.items():
        summaries_by_year[year] = analyze_year_summary(year_swaps, year, coin_ticker)
    
    # Print enhanced summary
    print_summary(coin_ticker, summaries_by_year)
    
    # Export data for each year
    for year, summary in summaries_by_year.items():
        year_swaps = summary['results']
        
        # Export to CSV (traditional format)
        csv_filename = f"{coin_ticker.lower()}_swaps_{year}.csv"
        export_swaps_to_csv(year_swaps, csv_filename)
        
        # Export to JSON (traditional format)
        json_filename = f"{coin_ticker.lower()}_swaps_{year}.json"
        export_swaps_to_json(year_swaps, json_filename)
        
        # Export enhanced summary (new format with analysis)
        summary_filename = f"{coin_ticker.lower()}_summary_{year}.json"
        export_summary_to_json(summary, summary_filename)
    
    # Also export all swaps combined
    all_swaps = []
    for summary in summaries_by_year.values():
        all_swaps.extend(summary['results'])
    
    if all_swaps:
        export_swaps_to_csv(all_swaps, f"{coin_ticker.lower()}_swaps_all.csv")
        export_swaps_to_json(all_swaps, f"{coin_ticker.lower()}_swaps_all.json")
        
        # Create and export combined summary
        all_summary = analyze_year_summary(all_swaps, "all_years", coin_ticker)
        export_summary_to_json(all_summary, f"{coin_ticker.lower()}_summary_all.json")
    
    print(f"\n✅ {coin_ticker} files created:")
    for year in sorted(summaries_by_year.keys()):
        print(f"  📊 {year} Analysis:")
        print(f"    - {coin_ticker.lower()}_swaps_{year}.csv (raw data)")
        print(f"    - {coin_ticker.lower()}_swaps_{year}.json (raw data)")
        print(f"    - {coin_ticker.lower()}_summary_{year}.json (enhanced analysis)")
    if all_swaps:
        print(f"  📈 Combined:")
        print(f"    - {coin_ticker.lower()}_swaps_all.csv")
        print(f"    - {coin_ticker.lower()}_swaps_all.json") 
        print(f"    - {coin_ticker.lower()}_summary_all.json (comprehensive analysis)")

def main():
    """
    Main function to extract and export SMTF and SFUSD swaps
    """
    print("Multi-Coin Swap Extraction Tool")
    print("================================")
    print("Extracting swaps for: SMTF, SFUSD")
    
    # Check environment setup
    if not check_environment():
        print("\n💡 To set up the environment:")
        print("1. Copy api/.env.example to api/.env (if available)")
        print("2. Or set the required environment variables")
        print("3. Make sure the database is running")
        sys.exit(1)
    
    print("Connecting to database...")
    
    # List of coins to process
    coins = ['SMTF', 'SFUSD']
    
    # Process each coin separately
    for coin in coins:
        try:
            process_coin(coin)
        except Exception as e:
            print(f"\n❌ Error processing {coin}: {e}")
            continue
    
    print(f"\n🎉 All exports completed! Check the 'exports' directory for the files.")

if __name__ == "__main__":
    main() 