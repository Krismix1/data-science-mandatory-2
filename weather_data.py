import datetime
import os
import json
import logging

logger = logging.getLogger(__name__)


def write_weather_data(folder):
    items = os.listdir(folder)
    files = [i for i in items if os.path.isfile(os.path.join(folder, i))]
    files = sorted(files)
    with open('dataset.csv', 'w', newline='') as dataset:
        dataset.write("timestamp,temperature,feelsLike,wind,weatherCode")
        dataset.write('\n')
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
                    final_date = date + ' ' + d.isoformat()
                    print(final_date + ',' + temperature + ',' + feels + ',' + wind)
                    with open('dataset.csv', 'a') as dataset:

                        dataset.write(','.join([final_date, temperature, feels, wind, weather_code]))
                        dataset.write('\n')


def main(folder):
    write_weather_data(folder)


if __name__ == '__main__':
    folder = os.path.join('data_download', 'weather')
    FORMAT = '%(asctime)s (`%(filename)s:%(lineno)d): (%(levelname)s) %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)
    main(folder)
