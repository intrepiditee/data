# Importing U.S. Bureau of Labor Statistics Producer Price Index

This directory contains data and scripts for importing
[U.S. Bureau of Labor Statistics Producer Price Index](
https://www.bls.gov/ppi/data.htm) into Data Commons. Both industry specific
PPI series and commodity specific PPI series are imported.

## Directories
1. [industry](industry) contains data and scripts for industry specific series.
2. [commodity](commodity) contains data and scripts for commodity specific
   series.

## Raw Data
1. "raw" subdirectories in [industry](industry) and [commodity](commodity)
   contain xlsx files downloaded from BLS containing industry and commodity
   specific series respectively.

## Cleaned Data
1. "cleaned" subdirectories in [industry](industry) and
   [commodity](commodity) contain csv files converted from the xlsx files.

## Scripts
1. `xlsx2csv.py` scripts in [industry](industry) and [commodity](commodity)
   convert the xlsx files to csv files.
2. `generate_mcf.py` scripts in [industry](industry) and [commodity](commodity)
   generate the instance MCFs and template MCFs.
3. [`xlsx_utils.py`](xlsx_utils.py) has a helper function for extracting
   information from an xlsx file.
4. [`industry/naicsw2csv.py`](industry/naics2csv.py) converts the NAICS mapping
   xlsx file [industry/2017_NAICS_Structure.xlsx](
   industry/2017_NAICS_Structure.xlsx) to a csv file [industry/naics.csv](
   industry/naics.csv).

## MCFs
1. "mcf" subdirectories in [industry](industry) and [commodity](commodity)
   contain the instance MCFs and template MCFs.
