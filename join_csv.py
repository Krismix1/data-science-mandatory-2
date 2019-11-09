import os
import logging

logger = logging.getLogger(__name__)

def main():
	pass

if __name__ == '__main__':
	FORMAT = '%(asctime)s (%(filename)s:%(lineno)d): (%(levelname)s) %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.DEBUG)
	main()