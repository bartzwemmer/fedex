import io
import logging
import os
from typing import Optional

import requests

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

LOG = logging.getLogger(__name__)

API_KEY = os.getenv("RIJKSSTUDIO_API")
BASE_URL = "https://www.rijksmuseum.nl/api/oai/"
XML_FOLDER = "/xmls"


def _write_xml(data: str, id: int) -> None:
    """
    Write the string as an XML file
    https://docs.databricks.com/external-data/xml.html
    """
    filename = f"xmls/records_{id}.xml"
    LOG.info("saving: " + filename)
    with io.open(filename, "w", encoding="utf-8") as f:
        f.write(data)


def _download_folder() -> None:
    """
    Check if the download folder exist and create it if it doesn't
    """
    if os.path.isdir(XML_FOLDER) == False:
        os.mkdir(XML_FOLDER)


def get_resumption_token(data: str) -> Optional[str]:
    """
    Extract the resumption token from the response
    """
    # TODO proper XML parsing would be better off course
    d = data.split('<resumptionToken completeListSize="681464">')
    resumption_token = d[1].split("</resumptionToken>")

    return resumption_token[0]


def download_xml_files(id: int = 0, token: str = "", limit: int = 3) -> None:
    """
    Download XML files upto the limit,
    or until there is no more data to fetch
    """
    if id > limit or token is None:
        LOG.info(f"Finished downloading")
        return None

    # First request is different, only create folder once
    if id == 0:
        _download_folder()
        url = f"{BASE_URL}{API_KEY}?verb=ListRecords&set=subject:EntirePublicDomainSet&metadataPrefix=dc"

    if id > 0:
        url = f"{BASE_URL}{API_KEY}?verb=listrecords&resumptiontoken={token}"

    LOG.info(f"Downloading file {id} of maximum {limit + 1}")
    # Verify = False because of annoying VPN
    res = requests.get(url=url, verify=False)
    if res.status_code == 200:
        _write_xml(res.text, id=id)
        LOG.info(f"Finished downloading file {id}")
        token = get_resumption_token(res.text)

        download_xml_files(id=id + 1, token=token)
    else:
        raise ValueError(
            f"Unexpected HTTP response. Got {res.status_code} in stead of 200."
        )


if __name__ == "__main__":
    # https://data.rijksmuseum.nl/object-metadata/harvest/
    download_xml_files(id=0)
