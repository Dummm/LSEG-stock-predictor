"""
Requirements
Your solution should utilize 2 APIs/Functions
• 1st API/Function that, for each file provided, returns 10 consecutive data points starting from a random
timestamp.
• 2nd API/function that gets the output from 1st one and predicts the next 3 values in the timeseries data.
Data & Inputs
Sample data is provided as a set of folders, one for each exchange, .csv files. Each file has
• Stock-ID, Timestamp (dd-mm-yyyy), stock price value.

Input parameter to your solution: The recommended number of files to be sampled for each Stock Exchange.
Possible input values are 1 or 2. If there aren’t enough files present for a given exchange, process whatever
number of files are present even if it is lower. E.g., input is 2 but only 1 file is present, so you process 1 file.

Prediction Logic: You can write your own prediction algorithm (in such case pls provide the logic and rationale)
or go by below for the sake of simplicity:
• first predicted (n+1) data point is same as the 2nd highest value present in the 10 data points
• n+2 data point has half the difference between n and n +1
• n+3 data point has 1/4th the difference between n+1 and n+2

Output Format
One .csv output file for each file processed. Each .csv file should have 3 columns on each row as shown below.
Timestamp & stock price have same format as input file
Stock-ID, Timestamp-1, stock price 1
..
Stock-ID, Timestamp-n, stock price n
Stock-ID, Timestamp-n+1, stock price n+1
Stock-ID, Timestamp-n+2, stock price n+2
Stock-ID, Timestamp-n+3, stock price n+3
"""
import logging
import argparse
import glob
import os
import itertools
from collections import defaultdict
import pandas as pd

APP_NAME = "Stock-Predictor"
DATA_DIRECTORY = "data/"

logger = logging.getLogger(__name__)

def _validate_arguments(args):
    if args.input_file_count < 1 or args.input_file_count > 2:
        return (False, f"Invalid file count [1-2]: {args.input_file_count}") 

    if args.data_directory_path:
        if not os.path.exists(args.data_directory_path):
            return (False), f"Data directory not found: {args.data_directory_path}"
    
    return (True, f"Arguments are valid: {args}") 

def _read_csv_files_from_data(data_directory=DATA_DIRECTORY):
    return glob.iglob(f"{data_directory}**/*.csv", recursive=True)
        
def _filter_csv_list(csv_list, data_directory, input_file_count):
    dd_length = len(data_directory)
    _exchange_stock_dict = defaultdict(list)
    
    # Ignore CSV files not following structure
    for _csv in csv_list:
        path_within_dd = _csv[dd_length:]
        path_parts = path_within_dd.split(os.sep)

        if len(path_parts) != 2:
            logger.debug(f"Skipping CSV not following path structure: {_csv}")
            continue
        
        exchange, csv_filename = path_parts 
        _exchange_stock_dict[exchange].append(_csv)
    
    for _exchange, _csv_list in _exchange_stock_dict.items():
        _exchange_stock_dict[_exchange] = _exchange_stock_dict[_exchange][:input_file_count]

    new_csv_list = _exchange_stock_dict.values()
    new_csv_list = list(itertools.chain.from_iterable(new_csv_list))

    return new_csv_list


# Application entry point
def app():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(f"{APP_NAME}.log"),
            logging.StreamHandler()
        ]
    )

    parser = argparse.ArgumentParser(
        prog=APP_NAME,
        description="LSEG Stock Predictor"
    )

    parser.add_argument(
        '--input_file_count', type=int, default=2,
        help='Number of files to read [1-2]'
    )
    parser.add_argument(
        '--data_directory_path', type=str, default=DATA_DIRECTORY,
        help='Path of data directory, containing CSV files'
    )

    # # Switch
    # parser.add_argument('--switch', action='store_true',
    #                     help='A boolean switch')

    args = parser.parse_args()
    (valid, message) = _validate_arguments(args)
    if not valid:
        logger.error(message)
        return 
    else:
        logger.debug(message)

    csv_list = _read_csv_files_from_data()
    csv_list = _filter_csv_list(
        csv_list=csv_list,
        data_directory=args.data_directory_path ,
        input_file_count=args.input_file_count
    )

    if new_csv_list:
        logger.info(f"Valid CSV files found")
    
    for _csv in csv_list:
        logger.info(f"Running prediction for {_csv}...")
        stock_data_df = pd.read_csv(_csv)

        

        predictor = BasicPredictor()

if __name__ == "__main__":
    app()