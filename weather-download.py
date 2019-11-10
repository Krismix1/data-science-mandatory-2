import os
import logging
import requests
import threading
from queue import Queue, Empty
from datetime import date, timedelta

from downloader import download_file


logging.getLogger('requests').propagate = False
logging.getLogger('urllib3').propagate = False
logger = logging.getLogger(__name__)

CITY = 'London'
URL = 'http://api.worldweatheronline.com/premium/v1/past-weather.ashx?key={}&q={}&format=json&tp=1&date={}&enddate={}'


def download_weather(dl_queue, key):
    while not dl_queue.empty():
        try:
            start, end = dl_queue.get(block=False)
            start = start.isoformat()
            end = end.isoformat()
            url = URL.format(key, CITY, start, end)
            filename = f"{start.replace('-', '_')}-{end.replace('-', '_')}.json"
            path = os.path.join('data_download', 'weather', filename)
            logger.info(f'Downloading from {url}')
            download_file(url, path)
            dl_queue.task_done()
        except Empty:
            break

def main(key):
    start_date = date(year=2015, month=1, day=1)
    end_date = date(year=2019, month=11, day=1)
    interval = timedelta(days=35)
    day = timedelta(days=1)

    dl_queue = Queue()

    while start_date < end_date:
        dl_queue.put((start_date, start_date + interval))
        start_date += interval + day

    num_dl_threads = 6
    for i in range(num_dl_threads):
        t = threading.Thread(target=download_weather, args=(dl_queue, key))
        t.start()

    dl_queue.join()

if __name__ == '__main__':
    import sys
    key = sys.argv[1]
    FORMAT = '%(asctime)s (%(filename)s:%(lineno)d): (%(levelname)s) %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)

    main(key)
