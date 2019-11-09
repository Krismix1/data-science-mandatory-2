import os
import json
import logging
import requests
import threading
from queue import Queue, Empty

logging.getLogger('requests').propagate = False
logging.getLogger('urllib3').propagate = False
logger = logging.getLogger(__name__)

SOURCE_JSON = 'data-index.json'
BASE_URL = 'https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk/{}'
OUTPUT_DIR = os.path.join('output', 'cycling-data')

def make_output_dirs(paths):
    for path in paths: os.makedirs(path, exist_ok=True)

def load_data(path):
    with open(path) as f:
        data = json.load(f)
    return data['Contents']

def download_file(url, output_path):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        if os.path.isfile(output_path): logger.warning('File %s exists', output_path)
        with open(output_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
    return True

def download_files(dl_queue):
    while not dl_queue.empty():
        try:
            left = dl_queue.qsize()
            res = dl_queue.get(block=False)
            url = BASE_URL.format(res['Key'])
            logger.info('Remaining %d files, downloading %s', left, url)
            download_file(url, os.path.join(OUTPUT_DIR, res['Key']))
            dl_queue.task_done()
        except Empty:
            break

def main():
    entry = load_data(SOURCE_JSON)
    folders = [os.path.join(OUTPUT_DIR, e['Key']) for e in entry if e['Size'] == '0'] # the size is stored as strings
    make_output_dirs(folders)
    dl_queue = Queue()
    for e in entry:
        if e['Size'] != '0': # the size is stored as strings
            dl_queue.put(e)

    num_dl_threads = 8
    for i in range(num_dl_threads):
        t = threading.Thread(name=f'DL thread {i}', target=download_files, args=(dl_queue,))
        t.start()
    dl_queue.join()

if __name__ == '__main__':
    FORMAT = '%(asctime)s (%(filename)s:%(lineno)d): (%(levelname)s) %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)
    main()
