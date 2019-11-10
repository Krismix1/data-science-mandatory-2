import os
import json
import logging

logger = logging.getLogger(__name__)

def get_codes(folder):
    items = os.listdir(folder)
    files = [i for i in items if os.path.isfile(os.path.join(folder, i))]
    codes = {}
    for file in files:
        path = os.path.join(folder, file)
        with open(path) as f:
            data = json.load(f)['data']

            if 'error' in data: continue
            for weather in data['weather']:
                for hour in weather['hourly']:
                    code = hour['weatherCode']
                    desc = hour['weatherDesc'][0]['value']
                    codes[desc] = code

    return codes

def save_weather_codes(folder, output_file):
    codes = get_codes(folder)
    with open(output_file, 'w') as f:
        json.dump(codes, f)


if __name__ == '__main__':
    import configs
    configs.configure_logging()
    save_weather_codes(configs.weather_data_dl_dir, configs.weather_data_codes)
