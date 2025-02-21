import requests
import os
from models import Exchange
from constants import api_key
import asyncio
from hftbacktest.data.utils.snapshot import create_last_snapshot
from hftbacktest.data.utils import tardis
import time
from datetime import datetime, timedelta
import argparse

async def download_L2_data(type:Exchange, symbol:str, date:str):
    #check if file exists
    path = f"data/{type.value}/{symbol}/L2_{date}.csv.gz"
    if os.path.exists(path):
        print(f"File already exists: {path}")
        return

    year = date[:4] 
    month = date[4:6]
    day = date[6:8]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    url = f"https://datasets.tardis.dev/v1/{type.value}/incremental_book_L2/{year}/{month}/{day}/{symbol}.csv.gz"
    headers = {
        "Authorization": f"Bearer {api_key}"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open(path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded file: {path}")
    else:
        print(f"Failed to download file: {path}, {response.content}")
    

async def download_trades_data(type:Exchange, symbol:str, date:str):
    path = f"data/{type.value}/{symbol}/trades_{date}.csv.gz"
    if os.path.exists(path):
        print(f"File already exists: {path}")
        return
    
    year = date[:4] 
    month = date[4:6]
    day = date[6:8]
    headers = {
        "Authorization": f"Bearer {api_key}"
    }   
    os.makedirs(os.path.dirname(path), exist_ok=True)
    url = f"https://datasets.tardis.dev/v1/{type.value}/trades/{year}/{month}/{day}/{symbol}.csv.gz"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open(path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded file: {path}")
    else:
        print(f"Failed to download file: {path}, {response.content}")


async def download_data(type:Exchange, symbol:str, date:str):
    await asyncio.gather(
        download_L2_data(type, symbol, date),
        download_trades_data(type, symbol, date)
    )



# combine all trades into a single file
async def combine_trades(exchange:Exchange, symbol:str, date:str, lot_size:float, tick_size:float):
    for attempt in range(5):
        try:
            if not os.path.exists(f'data/{exchange.value}/{symbol}/trades_{date}.csv.gz') or \
               not os.path.exists(f'data/{exchange.value}/{symbol}/L2_{date}.csv.gz'):
                raise FileNotFoundError("One or more input files are missing.")
            
            
            if not os.path.exists(f'data/{exchange.value}/{symbol}/combined_{date}.npz'):
                _ = tardis.convert(
                    [f'data/{exchange.value}/{symbol}/trades_{date}.csv.gz', f'data/{exchange.value}/{symbol}/L2_{date}.csv.gz'],
                    output_filename=f'data/{exchange.value}/{symbol}/combined_{date}.npz',
                    buffer_size=200_000_000
                )

            if not os.path.exists(f'data/{exchange.value}/{symbol}/eod_{date}.npz'):
                _ = create_last_snapshot(
                    [f'data/{exchange.value}/{symbol}/combined_{date}.npz'],
                    tick_size=tick_size,
                    lot_size=lot_size,
                    output_snapshot_filename=f'data/{exchange.value}/{symbol}/eod_{date}.npz'
                )
            break  # Exit the loop if successful
        except FileNotFoundError as e:
            print(f"Attempt {attempt + 1}: {e}")
            if attempt < 4:  # Wait only if it's not the last attempt
                time.sleep(1)
            else:
                raise  # Re-raise the exception if all attempts fail

counter = 0
async def main(start_date, end_date, symbol, lot_size, tick_size):
    _start_date = datetime.strptime(start_date, "%Y%m%d")
    _end_date = datetime.strptime(end_date, "%Y%m%d")

    current_date = _start_date
    previous_date = None

    while current_date <= _end_date:
        date = current_date.strftime("%Y%m%d")
        
        # Start downloading data for the current date
        download_task = asyncio.create_task(download_data(Exchange.BINANCE_FUTURE, symbol, date))
        
        # Combine trades for the previous day if it exists
        if previous_date:
            await combine_trades(Exchange.BINANCE_FUTURE, symbol, previous_date, lot_size, tick_size)
        
        # Wait for the download to complete
        await download_task
        
        # Update previous_date to the current date
        previous_date = date
        current_date += timedelta(days=1)

    # Combine trades for the last day
    if previous_date:
        await combine_trades(Exchange.BINANCE_FUTURE, symbol, previous_date, lot_size, tick_size)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and combine trade data.")
    parser.add_argument("--start_date", type=str, required=True, help="Start date in YYYYMMDD format")
    parser.add_argument("--end_date", type=str, required=True, help="End date in YYYYMMDD format")
    parser.add_argument("--symbol", type=str, required=True, help="Trading symbol, e.g., SOLUSDT")
    parser.add_argument("--lot_size", type=float, required=True, help="Lot size for trades")
    parser.add_argument("--tick_size", type=float, required=True, help="Tick size for trades")

    args = parser.parse_args()

    # Run the main function with parsed arguments
    asyncio.run(main(args.start_date, args.end_date, args.symbol, args.lot_size, args.tick_size))


