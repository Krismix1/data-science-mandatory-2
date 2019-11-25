import os
import json
import logging
import datetime

logger = logging.getLogger(__name__)

def write_weather_data(folder, output_file, eol='\n'):
    os.makedirs(folder, exist_ok=True)
    items = os.listdir(folder)
    files = [i for i in items if os.path.isfile(os.path.join(folder, i))]
    files = sorted(files)
    with open(output_file, 'w') as dataset:
        dataset.write('timestamp,temperature,feelsLike,wind,weatherCode')
        dataset.write(eol)
    for file in files:
        path = os.path.join(folder, file)
        with open(path) as f:
            data = json.load(f)['data']

            if 'error' in data: continue
            for weather in data['weather']:
                date = weather['date']
                for hour in weather['hourly']:
                    time = hour['time']
                    temperature = hour['tempC']
                    feels = hour['FeelsLikeC']
                    wind = hour['windspeedKmph']
                    weather_code = hour['weatherCode']
                    hour = int(time) / 100
                    d = datetime.time(hour=int(hour), minute=0, second=0)
                    final_date = f'{date} {d.isoformat()}'

                    with open(output_file, 'a') as dataset:
                        dataset.write(','.join([final_date, temperature, feels, wind, weather_code]))
                        dataset.write(eol)


if __name__ == '__main__':
    import configs
    configs.configure_logging()
    write_weather_data(configs.weather_data_dl_dir, configs.weather_data_csv, configs.EOL)
