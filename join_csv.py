import os
import logging

logger = logging.getLogger(__name__)
DATA_DIR = os.path.join('pre-processed', 'cycling-data', 'usage-stats')

def get_csv_files(data_dir):
    for item in os.listdir(data_dir):
        path = os.path.join(data_dir, item)
        if item.endswith('csv') and os.path.isfile(path):
            yield path

def main():
    files = get_csv_files(DATA_DIR)
    first = True
    with open('merged.csv', 'w') as out:
        for file in files:
            logging.debug(f'Merging file {file}')
            with open(file) as f:
                if first:
                    first = False
                    out.write(f.read())
                else:
                    lines = f.readlines()[1:]
                    out.write('\r\n'.join(lines))


if __name__ == '__main__':
    FORMAT = '%(asctime)s (`%(filename)s:%(lineno)d): (%(levelname)s) %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)
    main()
