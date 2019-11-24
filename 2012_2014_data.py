import os
import logging
from merge_csv import get_files
from cycling_data_downloader import make_output_dirs

logger = logging.getLogger(__name__)

def unzip_2012_2024_cycling_data(folder, output_folder):
    import re
    import zipfile

    make_output_dirs([folder, output_folder])
    files = get_files(folder, 'zip')
    for file in files:
        if not re.search(r'-201[2-4].zip$', file): continue
        logger.info(f'Unzipping {file} to {output_folder}')
        with zipfile.ZipFile(file, 'r') as zip_ref:
            zip_ref.extractall(output_folder)

def group_data():
    import time
    import pandas as pd
    from datetime import datetime
    chunk_size = 1000000
    date_mapper = lambda x: pd.to_datetime(datetime(year=x.year, month=x.month, day=x.day, hour=x.hour))

    bike_share_df = pd.DataFrame()
    print('Started loading merged_cycle_data_file.')
    iter_ = pd.read_csv('2012-2014-merged.csv', chunksize=chunk_size, iterator=True,
            index_col='Rental Id',
            parse_dates=['End Date', 'Start Date'])
    print('Finished loading merged_cycle_data_file.')

    r_start = time.time()
    for i, df in enumerate(iter_):
        r_end = time.time()
        print(f'{i+1}. Read rows {chunk_size*i}:{chunk_size*(i+1)} in {r_end-r_start:.3f}. ', end='')

        start = time.time()
        df = df.dropna()
        # leave only entries that have valid duration
        df = df[df['Duration'] > 0]

        diff = df['End Date'] - df['Start Date'] # compute the difference between the objects
        seconds = diff.map(lambda x: x.total_seconds()) # map to seconds
        df = df[(df['Duration'] == seconds) & (seconds >= 0)] # check if duration matches the result and if the result is positive

        # keep only year, month, day, hour information from the start date
        df['Start Date'] = df['Start Date'].map(date_mapper)

        share_df = df.groupby('Start Date').agg({'Start Date': 'count'}).rename(columns={'Start Date': 'share_count'})
        share_df = share_df.join(weather_df)
        share_df = share_df.reset_index()
        share_df = share_df.dropna()

        share_df['month'] = share_df['Start Date'].apply(lambda t: t.month)
        share_df['weekday'] = share_df['Start Date'].apply(lambda t: t.weekday())
        share_df['hour'] = share_df['Start Date'].apply(lambda t: t.hour)
        share_df['is_holiday'] = share_df['Start Date'].map(lambda x: x.date() in hol_set).map(lambda x: '1' if x else '0')
        # check if start date hits on a weekend
        # monday is 0, sunday is 6
        share_df['is_weekend'] = share_df['Start Date'].map(lambda x: x.weekday() > 4).map(lambda x: '1' if x else '0')
        share_df['weatherCode'] = share_df['weatherCode'].map(lambda x: str(int(x)))

        bike_share_df = bike_share_df.append(share_df)
        end = time.time()
        print(f'Completed cleaning & merging in {end-start:3.3f} seconds.')
        r_start = time.time()

    print('Finished reading!')
    bike_share_df = bike_share_df.reset_index().drop(columns=['index']) # fix the index
    # save the data to a file, so that we can load it faster next time
    bike_share_df.to_csv('2012-2014-shares-ungrouped.csv')

if __name__ == '__main__':
    import configs
    from cycle_data_cleaning import clean_data
    from merge_csv import merge_files

    configs.configure_logging()
    # unzip_2012_2024_cycling_data(configs.usage_stats, 'output/2012-2014-data')
    # clean_data('output/2012-2014-data', 'output/2012-2014-cleaned')
    # merge_files('output/2012-2014-cleaned', '2012-2014-merged.csv')
    group_data()
