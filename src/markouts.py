import glob
import shutil
from datetime import datetime
import nest_asyncio
from tardis_dev import datasets
import os
import sys
from dotenv import load_dotenv
import gzip
import logging
import json
import logging
from typing import List, Optional
import pandas as pd
import numpy as np

load_dotenv()

class Markouts:
    def __init__(self):
        """
        Initialize the Markouts object.

        Args:
            markout_distance_array (List[int]): List of markout distances in seconds
        """
        self.tardis_api_key = os.getenv("TARDIS_API_KEY")
        self.logger = logging.getLogger(__name__)
        self.available_symbols = self._load_available_symbols()

    def _load_available_symbols(self) -> List[str]:
        """
        Load the list of available symbols from the JSON file.

        Returns:
            List[str]: List of available symbols
        """
        file_path = os.path.join("..", "data", "available_tardis_pairs_list.json")
        try:
            with open(file_path, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            self.logger.error(f"Symbol list file not found: {file_path}")
            return []
        except json.JSONDecodeError:
            self.logger.error(f"Error decoding JSON from file: {file_path}")
            return []

    def is_valid_symbol(self, symbol: str) -> bool:
        """
        Check if the given symbol is in the list of available symbols.

        Args:
            symbol (str): The symbol to check

        Returns:
            bool: True if the symbol is valid, False otherwise
        """
        return symbol.upper() in self.available_symbols

    def get_data_from_tardis(self, timestamp: int, symbol: str) -> Optional[str]:
        """
        Retrieve data from Tardis API.

        Args:
            timestamp (int): Unix timestamp
            symbol (str): Trading symbol

        Returns:
            Optional[str]: Path to the downloaded data file, or "NA" if an error occurred
        """
        timestamp_dt = datetime.fromtimestamp(int(timestamp))

        # Convert datetime to date string
        date_start = timestamp_dt.strftime('%Y-%m-%d')
        date_end = date_start

        # Construct the file name and paths
        file_name = f'binance_book_snapshot_5_{date_start}_{symbol}.csv'
        input_path = os.path.join('datasets', f'{file_name}.gz')
        output_path = os.path.join('..', 'data', "binance_data", file_name)
        
        # Check if the path exists
        if os.path.exists(output_path):
            self.logger.info(f"File already exists: {output_path}")
            return output_path
        
        # Start the data download
        try:
            datasets.download(
                exchange="binance",
                data_types=["book_snapshot_5"],
                from_date=date_start,
                to_date=date_end,
                symbols=[symbol],
                api_key=self.tardis_api_key,
            )
        except Exception as e:
            self.logger.error(f"Error when trying to pull the pair: {symbol}. Error: {str(e)}")
            return "NA"
                
        # Decompress the gz file
        try:
            with gzip.open(input_path, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        except Exception as e:
            self.logger.error(f"Error decompressing file: {str(e)}")
            return "NA"

        # Delete the datasets directory and its contents
        try:
            shutil.rmtree('datasets')
        except Exception as e:
            self.logger.warning(f"Error deleting datasets directory: {str(e)}")
        
        self.logger.info(f"Successfully downloaded and processed data: {output_path}")
        return output_path

    def calculate_markouts(self, df: pd.DataFrame, markout_distance_array: List[int]) -> pd.DataFrame:
        """
        Calculate markouts for the given DataFrame.

        Args:
            df (pd.DataFrame): Input DataFrame with columns (token in, token out, timestamp, amount in, amount out)
            markout_distance_array (List[int]): List of markout distances in seconds

        Returns:
            pd.DataFrame: Original DataFrame with appended markout columns
        """
        for distance in markout_distance_array:
            df[f'markout_{distance}'] = np.nan

        for index, row in df.iterrows():
            token_in = row['token_in']
            #print(f"Token in: {token_in}")
            token_out = row['token_out']
            #print(f"Token out: {token_out}")
            timestamp = row['timestamp']
            #print(f"Timestamp: {timestamp}")

            symbol1 = f"{token_in}{token_out}".upper()
            symbol2 = f"{token_out}{token_in}".upper()

            valid_symbol = symbol1 if self.is_valid_symbol(symbol1) else (symbol2 if self.is_valid_symbol(symbol2) else None)

            if valid_symbol is None:
                self.logger.error(f"Could not find valid symbol for either {symbol1} or {symbol2}")
                continue

            dex_data_path = self.get_data_from_tardis(timestamp, valid_symbol)
            if dex_data_path == "NA" or dex_data_path is None:
                self.logger.error(f"Could not find dex_data for {valid_symbol}")
                continue

            try:
                dex_data = pd.read_csv(dex_data_path)
            except Exception as e:
                self.logger.error(f"Error reading data from {dex_data_path}: {str(e)}")
                continue

            if dex_data.empty:
                self.logger.error(f"Dex Data is empty for {valid_symbol}")
                continue

            row_timestamp_micro = int(timestamp * 1_000_000)  # Convert to microseconds

            for distance in markout_distance_array:
                filter_timestamp = row_timestamp_micro + (distance * 1_000_000)
                relevant_data = dex_data[dex_data['timestamp'] <= filter_timestamp]
                
                if relevant_data.empty:
                    continue

                last_row = relevant_data.iloc[-1]
                midpoint = (last_row['asks[0].price'] + last_row['bids[0].price']) / 2
                df.at[index, f'markout_{distance}'] = midpoint

        return df
    
    def get_trade_price(self, row: pd.Series) -> float:
        """
        Calculate the trade price from the row data.
        """
        return row['amount_out'] / row['amount_in']
    
    def _get_dex_head(self, path: str, sample_size: int) -> Optional[str]:
        """
        Get the head of the dex data df from Binance that is easier to process. This private function is meant to be called with the returned path from get_data_from_tardis as input.

        Args:
            path (str): A path to some dex df.
            sample_size (int): The size of the sample desired.

        Returns:
            path (str): Optional return to the path of the sample df
        """
        try:
            # Read the CSV file
            df = pd.read_csv(path)
            
            # Take a random sample
            sample_df = df.head(sample_size)
            
            sample_dir = os.path.join("..", "data", "sample_binance_data")
            os.makedirs(sample_dir, exist_ok=True)
            
            # Create a new filename for the sample
            base_name = os.path.basename(path)
            sample_name = f"sample_{sample_size}_{base_name}"
            sample_path = os.path.join(sample_dir, sample_name)
            
            # Save the sample to a new CSV file
            sample_df.to_csv(sample_path, index=False)
            
            self.logger.info(f"Sample of size {sample_size} saved to {sample_path}")
            return sample_path
        
        except Exception as e:
            self.logger.error(f"Error creating sample from {path}: {str(e)}")
            return None
        
if __name__ == "__main__":
    # logging.basicConfig(level=logging.INFO)
    # markouts = Markouts()
    # result = markouts.get_data_from_tardis(1723483005, "ETHUSDC")
    # markouts._get_dex_head(result, 500)

    data = {
        'token_in': ['USDC', 'ETH', 'USDC'],
        'token_out': ['ETH', 'USDC', 'ETH'],
        'timestamp': [1723483005, 1723483010, 1723483015],
        'amount_in': [1000, 1, 2000],
        'amount_out': [0.5, 2000, 1]
    }
    df = pd.DataFrame(data)

    markouts = Markouts()
    markout_distances = [2, 4, 12, 24]
    result = markouts.calculate_markouts(df, markout_distances)
    print(result)
    
    
