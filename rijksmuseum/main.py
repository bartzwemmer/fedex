# https://data.rijksmuseum.nl/object-metadata/harvest/

import requests
import os
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

LOG = logging.getLogger(__name__)

API_KEY = os.getenv('RIJKSSTUDIO_API')
BASE_URL =  'https://www.rijksmuseum.nl/api/oai/'
XML_FOLDER = '/xmls'

verb = 'ListRecords&set=subject:EntirePublicDomainSet&metadataPrefix=dc'

url = f'{BASE_URL}{API_KEY}?verb={verb}'
res = requests.get(url=url)

def clean_xml(data: str) -> str:
   '''
   Remove whitelines and garbage from XML string
   '''
   # Remove whitelines
   no_white_lines = os.linesep.join([s for s in data.splitlines() if s])
   # Remove XML prefixes, since they cause trouble creating column names
   no_dc_prefix = no_white_lines.replace("dc:", "")
   cleaned = no_dc_prefix.replace("oai_dc:", "")

   return cleaned

def write_xml(data: str) -> None:
  '''
  Write the string as an XML file
  '''
  filename = 'testdata/test.xml'
  print('saving: ' + filename)
  with open(filename, 'w', encoding="utf-8") as f:
    f.write(data)


if __name__ == "__main__":
    if os.path.isdir(XML_FOLDER) == False:
        os.mkdir(XML_FOLDER)

    if res.status_code == 200:
        write_xml(clean_xml(res.text))