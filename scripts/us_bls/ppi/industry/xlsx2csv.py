'''
Converts industry specific PPI xlsx files downloaded from BLS to csv files in
"cleaned" directory.
Two csv files will be generated:
1) Combining all the industry specific PPI xlsx files to a
   single CSV file "us_ppi_industries.csv".
   The output table has a "date" column of the form "YYYY-MM" and 39 other
   columns each representing the series for an industry.
   The names of the 39 columns are of the form
   "{NAICS code}_{base date}_{industry name}", where "base date" is of the form
   "YYYY", "YYYY-MM", or "YYYY-YY.
   "NAICS code", "base date", and "industry name" do not have
   underscores in them.
2) "us_ppi_total.csv", containing a PPI series that is the average of all
   industries extracted from "SeriesReport-20200603112545_1fa5aa.xlsx".
   The output table has two columns: "date" and "ppi", where "date" is of the form
   "YYYY-MM" and "ppi" is numeric.

'''

import os
import re
import sys

# Adds parent directory to Python module search path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
from xlsx_utils import read_xlsx


def get_naics_dict():
    '''Reads in the csv file containing the official NAICS mapping converted
    from xlsx.

    Returns:
        A dict mapping NAICS codes to their industry names.
    '''
    return list(pd.read_csv("naics.csv", index_col=0).to_dict().values())[0]


def gen_industries():
    '''Generates the csv file containing all industry specific series'''
    naics_dict = get_naics_dict()
    out_df = pd.DataFrame()
    for filename in os.listdir("raw"):
        if filename.endswith(".xlsx"):
            in_df, series_id, base_date, _unused = read_xlsx("raw/" + filename)

            # Series IDs for PPIs are of the form "PCUXXXXXYYYYY", where
            # "XXXXX" == "YYYYY" is the numerical NAICS code.
            digits = "".join([c for c in series_id if c.isdigit()])
            naics = digits[:(len(digits) // 2)]
            # Special cases where the industry code in the series ID is
            # undefined and needs to be remapped.
            if series_id == "PCU4240004240002":
                # Drugs and Druggists' Sundries Merchant Wholesalers
                naics = "4242"
            elif series_id == "PCU4240004240004":
                # Grocery and Related Product Merchant Wholesalers
                naics = "4244"
            elif series_id == "PCU336110336110":
                # Automobile and Light Duty Motor Vehicle Manufacturing
                naics = "33611"
            elif series_id == "PCUOMFG--OMFG--":
                # This series is the aggregate of all industries and will be
                # handled separately
                continue

            industry = naics_dict.get(naics)
            if not industry:
                # These series need to be handled separatey
                print(filename + ":" + series_id +
                      " does not have corresponding NAICS", file=sys.stderr)
                continue
            industry = industry.strip()
            # T = Canadian, Mexican, and United States industries are comparable
            if industry.endswith("T"):
                industry = industry[:-1]
            # Remove non-alphanumeric characters
            industry = re.sub("[^\w]", "", industry.title())
            in_df.columns = [naics + "_" + base_date + "_" + industry]

            if out_df.empty:
                out_df = in_df
            else:
                out_df = out_df.join(in_df, how="outer")
        
    out_df.to_csv("cleaned/us_ppi_industries.csv")


def gen_total():
    '''Generates the csv file containing the average of all industries'''
    in_df = read_xlsx("raw/SeriesReport-20200603112545_1fa5aa.xlsx")[0]
    in_df.columns = ["ppi"]
    in_df.to_csv("cleaned/us_ppi_total.csv")


def main():
    gen_industries()
    gen_total()


if __name__ == "__main__":
    main()
