# Rijksmuseum
For an assessment, this is a small demo project of scraping an API and loading into Databricks as an ELT.

## Intro
This is a demo app and not complete. It is used to download XML files from the Rijksmuseum API containing data about their collection.  
The XML files are loaded into Databricks and transformed into delta tables in a start-model for analytics. 

## Usage
1. Build a wheel file: python -m build -w
1. Upload to Databricks
1. Open an notebook, import rijksmusem and run

Alternatively, run `download.py` in a notebook, then run `loader.py` to convert the downloaded XML's in a rudimentary star model.