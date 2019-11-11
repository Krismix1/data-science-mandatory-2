import os

restructured_data_dir = os.path.join('output', 'restructured-data')
merged_cycle_data_file = os.path.join('output', 'cycle-data-merged.csv')

cycling_data_index = 'cycle-data-index.json'
cycling_data_dl_dir = os.path.join('data_download', 'cycling-data')

weather_data_dl_dir = os.path.join('data_download', 'weather')
weather_data_codes = os.path.join('output', 'weather-codes.json')
weather_data_csv = os.path.join('output', 'weather-data.csv')

weather_data_city = 'London'

EOL = '\n'

def configure_logging():
    import logging
    FORMAT = '%(asctime)s (%(filename)s:%(lineno)d): (%(levelname)s) %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)
    logging.getLogger('requests').propagate = False
    logging.getLogger('urllib3').propagate = False
