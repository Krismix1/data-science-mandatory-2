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


def main(folder):
    codes = get_codes(folder)
    with open('weather-codes.json', 'w') as f:
        json.dump(codes, f)
    print(codes)


if __name__ == '__main__':
    folder = os.path.join('data_download', 'weather')
    FORMAT = '%(asctime)s (`%(filename)s:%(lineno)d): (%(levelname)s) %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)
    main(folder)
