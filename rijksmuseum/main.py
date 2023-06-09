# https://data.rijksmuseum.nl/object-metadata/harvest/

from extract.download import download_xml_files
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

LOG = logging.getLogger(__name__)

if __name__ == "__main__":
    # Load the data in an ELT pattern
    LOG.info('Downloading XML data.')
    download_xml_files()