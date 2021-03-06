import os
import logging

logger = logging.getLogger(__name__)

def get_files(data_dir, extension):
    for item in os.listdir(data_dir):
        path = os.path.join(data_dir, item)
        if item.endswith(extension) and os.path.isfile(path):
            yield path

def merge_files(source_dir, output_file):
    os.makedirs(source_dir, exist_ok=True)

    files = get_files(source_dir, 'csv')
    first = True
    with open(output_file, 'w') as out:
        for file in files:
            logger.debug(f'Merging file {file}')
            with open(file) as f:
                if first:
                    first = False
                    out.write(f.read())
                else:
                    lines = f.readlines()[1:]
                    out.writelines(lines)


if __name__ == '__main__':
    import configs
    configs.configure_logging()
    merge_files(configs.restructured_data_dir, configs.merged_cycle_data_file)
