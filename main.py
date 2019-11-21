import configs
from merge_csv import merge_files
from cycle_data_cleaning import clean_data
from weather_data import write_weather_data
from weather_codes import save_weather_codes
from weather_download import init_weather_download
from cycling_data_downloader import download_cycle_data

def main(key):
    configs.configure_logging()

    download_cycle_data(configs.cycling_data_index, configs.cycling_data_dl_dir)
    clean_data(configs.cycling_data_dl_dir, configs.restructured_data_dir)
    merge_files(configs.restructured_data_dir, configs.merged_cycle_data_file)

    init_weather_download(key, configs.weather_data_dl_dir, configs.weather_data_city)
    save_weather_codes(configs.weather_data_dl_dir, configs.weather_data_codes)
    write_weather_data(configs.weather_data_dl_dir, configs.weather_data_csv, configs.EOL)

if __name__ == '__main__':
    import sys
    main(sys.argv[1])