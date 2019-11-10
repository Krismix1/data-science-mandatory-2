import os
import configs
import logging
import pandas as pd


logger = logging.getLogger(__name__)

def clean_df(df):
    # standardize names
    df = df.rename(columns={
        'Duration_Seconds': 'Duration',
        'End Station Id': 'EndStation Id',
        'End Station Name': 'EndStation Name',
        'Start Station Id': 'StartStation Id',
        'Start Station Name': 'StartStation Name',
        'EndStation Logical Terminal': 'EndStation Id',
        'StartStation Logical Terminal': 'StartStation Id'
    })
    df = df.drop(axis=1, labels=['EndStation Name', 'StartStation Name', 'Bike Id'])

    # drop columns that have no values, not even a name
    before = df.shape[0]
    df = df.dropna(axis=1,how='all')
    after = df.shape[0]
    if before != after:
        logging.warning('Dropped rows from: before %d, after %d', before, after)

    if 'endStationPriority_id' in df.columns: # no clue what this is
        df = df.drop(axis=1, labels=['endStationPriority_id'])
    return df

def clean_data(data_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    folder = os.path.join(data_dir, 'usage-stats')
    entries = os.listdir(folder)
    entries = [e for e in entries if os.path.isfile(os.path.join(folder, e)) and e.endswith('csv')]

    df = pd.read_csv(os.path.join(folder, entries[0]), index_col='Rental Id') # treat the Rental Id as the index
    df = clean_df(df)
    cols = set(df.columns)

    for file in entries[1:]:

        df_ = pd.read_csv(os.path.join(folder, file), index_col='Rental Id') # treat the Rental Id as the index
        df_ = clean_df(df_)

        if not cols == set(df_.columns):
            logging.warning(
                'Columns did not match in %s. Columns: %s',
                file, set(df_.columns) - cols
            )
            continue
        df_.to_csv(os.path.join(output_dir, file))

if __name__ == '__main__':
    configs.configure_logging()
    clean_data(configs.cycling_data_dl_dir, configs.restructured_data_dir)
