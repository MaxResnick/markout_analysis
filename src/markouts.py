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

load_dotenv()

class Markouts:
    def __init__(self, markout_distance_array: List[int]):
        """
        Initialize the Markouts object.

        Args:
            markout_distance_array (List[int]): List of markout distances in seconds
        """
        self.tardis_api_key = os.getenv("TARDIS_API_KEY")
        self.logger = logging.getLogger(__name__)
        self.markout_distance_array = markout_distance_array
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
        output_path = os.path.join('..', 'data', file_name)
        
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

    # def process_data(self, data_path: str) -> pd.DataFrame:
    #     """
    #     Process the downloaded data.
    #     
    #     Args:
    #         data_path (str): Path to the data file
    #     
    #     Returns:
    #         pd.DataFrame: Processed data as a pandas DataFrame
    #     """
    #     # Implementation here
    #     pass

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    markouts = Markouts()
    result = markouts.get_data_from_tardis(1723483005, "ETHUSDC")
    print(result)