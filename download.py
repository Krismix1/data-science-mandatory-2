import os
import json
import logging
import requests

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

def main():
    entry = load_data(SOURCE_JSON)
    files = [e for e in entry if e['Size'] != '0'] # the size is stored as strings
    folders = [os.path.join(OUTPUT_DIR, e['Key']) for e in entry if e['Size'] == '0'] # the size is stored as strings
    make_output_dirs(folders)

    for index, res in enumerate(files):
        url = BASE_URL.format(res['Key'])
        logger.info('Downloading %d/%d, from %s', index+1, len(files), url)
        download_file(url, os.path.join(OUTPUT_DIR, res['Key']))

if __name__ == '__main__':
    FORMAT = '%(asctime)s (%(filename)s:%(lineno)d): (%(levelname)s) %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)
    main()
