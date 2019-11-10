import os
import json
import logging
import requests
import threading
from queue import Queue, Empty

logger = logging.getLogger(__name__)


def make_output_dirs(paths):
    for path in paths: os.makedirs(path, exist_ok=True)

def load_data(path):
    with open(path) as f:
        data = json.load(f)
    return data['Contents']

def download_file(url, output_path):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        if os.path.isfile(output_path): logger.warning('File %s already exists', output_path)
        with open(output_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
    return True

def download_files(dl_queue, output_dir):
    BASE_URL = 'https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk/{}'
    while not dl_queue.empty():
        try:
            left = dl_queue.qsize()
            filename = dl_queue.get(block=False)
            url = BASE_URL.format(filename)
            logger.info('Remaining %d files, downloading %s', left, url)
            download_file(url, os.path.join(output_dir, filename.replace(' ', '')))
            dl_queue.task_done()
        except Empty:
            break

def download_cycle_data(index_file, output_dir):
    entries = load_data(index_file)
    folders = [os.path.join(output_dir, e['Key']) for e in entries if e['Size'] == '0'] # the size is stored as strings
    make_output_dirs(folders)
    dl_queue = Queue()
    for entry in entries:
        if entry['Size'] != '0': # the size is stored as strings
            dl_queue.put(entry['Key']) # remove spaces from file names

    num_dl_threads = 8
    for i in range(num_dl_threads):
        t = threading.Thread(name=f'DL thread {i}', target=download_files, args=(dl_queue, output_dir))
        t.start()
    dl_queue.join()

if __name__ == '__main__':
    import configs
    configs.configure_logging()
    download_cycle_data(configs.cycling_data_index, configs.cycling_data_dl_dir)
