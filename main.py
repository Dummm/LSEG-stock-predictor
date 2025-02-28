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
first predicted (n+1) data point is same as the 2nd highest value present in the 10 data points
n+2 data point has half the difference between n and n +1
n+3 data point has 1/4th the difference between n+1 and n+2

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
from random import randrange
from datetime import datetime, timedelta

from predictor import BasicPredictor

APP_NAME = "Stock-Predictor"
DATA_DIRECTORY_DEFAULT = "data/"
INPUT_FILE_COUNT_DEFAULT = 2
PREDICTION_DATA_POINTS = 10
DATE_FORMAT = '%d-%m-%Y'
TIMESTAMP = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

logger = logging.getLogger(__name__)

def _validate_arguments(args):
    if args.input_file_count < 1 or args.input_file_count > 2:
        return (False, f"Invalid file count [1-2]: {args.input_file_count}") 

    if args.data_directory_path:
        if not os.path.exists(args.data_directory_path):
            return (False), f"Data directory not found: {args.data_directory_path}"
    
    return (True, f"Arguments are valid: {args}") 

def _read_csv_files_from_data(data_directory):
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
        if "Prediction_" in csv_filename:
            continue
        _exchange_stock_dict[exchange].append(_csv)
    
    for _exchange, _csv_list in _exchange_stock_dict.items():
        _exchange_stock_dict[_exchange] = _exchange_stock_dict[_exchange][:input_file_count]

    new_csv_list = _exchange_stock_dict.values()
    new_csv_list = list(itertools.chain.from_iterable(new_csv_list))

    return new_csv_list

# Requirement 1
def extract_stock_data_subset(stock_data_df):
    stock_data_size = len(stock_data_df)

    random_df_position = randrange(0, stock_data_size - PREDICTION_DATA_POINTS)
    stock_data_subset = stock_data_df.iloc[
        random_df_position:random_df_position + PREDICTION_DATA_POINTS
    ].copy()
    stock_data_subset.reset_index(drop=True, inplace=True)
    
    return stock_data_subset

# Requirement 2
def prepare_prediction_df(stock_data_subset, prediction):
    prediction_length = len(prediction)

    last_timestamp = stock_data_subset['Timestamp'].iloc[-1]
    last_timestamp_dt = datetime.strptime(last_timestamp, DATE_FORMAT)
    new_timestamps = []
    new_tickers = []
    for i in range(prediction_length):
        next_timestamp = last_timestamp_dt + timedelta(days=i+1)
        new_timestamps.append(next_timestamp.strftime(DATE_FORMAT))
        new_tickers.append(stock_data_subset['Ticker'].iloc[0])

    prediction_df = pd.DataFrame({
        "Ticker": new_tickers,
        "Timestamp": new_timestamps,
        "Value": prediction
    },  index=None)
    
    final_result_df = pd.concat(
        [stock_data_subset, prediction_df], 
        axis=0
    )

    return final_result_df

def generate_basic_prediction(csv_path, stock_data_df):
    stock_data_subset = extract_stock_data_subset(stock_data_df)

    basic_predictor = BasicPredictor(
        input_data_df=stock_data_subset["Value"]
    )
    prediction = basic_predictor.predict()

    result_df = prepare_prediction_df(stock_data_subset, prediction)
    bp_csv_path = csv_path.replace(
        '.csv',
        f'_BasicPrediction_{TIMESTAMP}.csv'
    )
    result_df.to_csv(bp_csv_path, index=False)

def generate_numpy_prediction(csv_path, stock_data_df):
    stock_data_subset = extract_stock_data_subset(stock_data_df)

    numpy_predictor = NumPyPredictor(
        x=stock_data_subset["Timestamp"],
        y=stock_data_subset["Value"],
        degree=1
    )
    prediction = numpy_predictor.predict()

    result_df = prepare_prediction_df(stock_data_subset, prediction)
    bp_csv_path = csv_path.replace(
        '.csv',
        f'_NumPyPrediction_{TIMESTAMP}.csv'
    )
    result_df.to_csv(bp_csv_path, index=False)

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
        '--input_file_count', type=int, 
        default=INPUT_FILE_COUNT_DEFAULT, const=INPUT_FILE_COUNT_DEFAULT, nargs='?',
        help='Number of files to read [1-2]'
    )
    parser.add_argument(
        '--data_directory_path', type=str, 
        default=DATA_DIRECTORY_DEFAULT, const=DATA_DIRECTORY_DEFAULT, nargs='?',
        help='Path of data directory, containing CSV files'
    )

    args = parser.parse_args()
    (valid, message) = _validate_arguments(args)
    if not valid:
        logger.error(message)
        return 
    else:
        logger.debug(message)

    data_directory_path = args.data_directory_path
    input_file_count = args.input_file_count

    csv_list = _read_csv_files_from_data(
        data_directory=data_directory_path
    )
    csv_list = _filter_csv_list(
        csv_list=csv_list,
        data_directory=data_directory_path ,
        input_file_count=input_file_count
    )

    if csv_list:
        logger.info(f"Valid CSV files found")
    
    for _csv in csv_list:
        logger.info(f"Running prediction for {_csv}...")
        stock_data_df = pd.read_csv(
            _csv, 
            header=None, names=["Ticker", "Timestamp", "Value"]
        )
        stock_data_df['Timestamp'] = pd.to_datetime(
            stock_data_df['Timestamp'],
            format=DATE_FORMAT
        ).dt.strftime(DATE_FORMAT)
    
        stock_data_size = len(stock_data_df)
        if stock_data_size < PREDICTION_DATA_POINTS:
            logger.error("CSV File doesn't have enough data points")
    
        generate_basic_prediction(_csv, stock_data_df)
        # generate_numpy_prediction(_csv, stock_data_df)
        
        

if __name__ == "__main__":
    app()