import glob
import shutil
from datetime import datetime
import nest_asyncio
from tardis_dev import datasets
import os
import sys
from dotenv import load_dotenv
import gzip

load_dotenv()

def get_data_from_tardis(timestamp, symbol):
        tardis_api_key = os.getenv("TARDIS_API_KEY")
        nest_asyncio.apply()

        timestamp_dt = datetime.fromtimestamp(int(timestamp))

        # Convert datetime to date string
        date_start = timestamp_dt.strftime('%Y-%m-%d')
        date_end = date_start

        # Construct the file name and paths
        file_name = f'binance_book_snapshot_5_{date_start}_{symbol}.csv'
        input_path = os.path.join('datasets', f'{file_name}.gz')
        output_path = os.path.join('..', 'data', file_name) #Might want to organize by pair? Not sure
        
        # Check if the path exists
        if os.path.exists(output_path):
            return output_path
        
        # Start the data download
        try:
            datasets.download(
                    exchange="binance",
                    data_types=["book_snapshot_5"],
                    from_date=date_start,
                    to_date=date_end,
                    symbols=[symbol],
                    api_key=tardis_api_key,
                )
        except:
            print(f"Error thrown when trying to pull the pair: {symbol}")
            return("NA")
                
        # Decompress the gz file
        with gzip.open(input_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Delete the datasets directory and its contents
        shutil.rmtree('datasets')
        
        return output_path




get_data_from_tardis("1723483005", "ETHUSDC")