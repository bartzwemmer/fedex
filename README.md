# Fedex
Fedex assessment

## Intro
This is a demo app and not complete. It is used to download XML files from the Rijksmuseum API containing data about their collection.  
The XML files are loaded into Databricks and transformed into a start-model for analytics. 

## Usage
1. Build a wheel file: python -m build -w
1. Upload to Databricks
1. Open an notebook, import rijksmusem and run