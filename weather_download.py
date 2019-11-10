import os
import logging
import requests
import threading
from queue import Queue, Empty
from datetime import date, timedelta

from cycling_data_downloader import download_file, make_output_dirs

logger = logging.getLogger(__name__)


def download_weather(dl_queue, key, dl_dir, city):
    URL = 'http://api.worldweatheronline.com/premium/v1/past-weather.ashx?key={}&q={}&format=json&tp=1&date={}&enddate={}'
    while not dl_queue.empty():
        try:
            start, end = dl_queue.get(block=False)
            start = start.isoformat()
            end = end.isoformat()
            url = URL.format(key, city, start, end)
            filename = f"{start.replace('-', '_')}-{end.replace('-', '_')}.json"
            path = os.path.join(dl_dir, filename)
            logger.info(f'Downloading from {url}')
            download_file(url, path)
            dl_queue.task_done()
        except Empty:
            break

def init_weather_download(api_key, dl_dir, city):
    start_date = date(year=2015, month=1, day=1)
    end_date = date(year=2019, month=11, day=1)
    interval = timedelta(days=35)
    day = timedelta(days=1)

    dl_queue = Queue()

    while start_date < end_date:
        dl_queue.put((start_date, start_date + interval))
        start_date += interval + day

    make_output_dirs([dl_dir])

    num_dl_threads = 6
    for i in range(num_dl_threads):
        t = threading.Thread(target=download_weather, args=(dl_queue, api_key, dl_dir, city))
        t.start()

    dl_queue.join()

if __name__ == '__main__':
    import sys
    import configs
    configs.configure_logging()
    key = sys.argv[1]
    init_weather_download(key, configs.weather_data_dl_dir, configs.weather_data_city)
