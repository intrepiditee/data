'''
Converts commodity specific PPI xlsx files downloaded from BLS to csv files in
"cleaned" directory.
Two csv files will be generated:
1) Combining all the commodity specific PPI xlsx files to a
   single CSV file "us_ppi_commodities.csv".
   The output table has a "date" column of the form "YYYY-MM" and 22 other
   columns each representing the series for a type of commodity.
   The names of the 22 columns are of the form
   "{commodity name}_{base date}", where "base date" is of the form
   "YYYY", "YYYY-MM", or "YYYY-YY".
   "commidity name" and "base date" do not have underscores in them.
2) "us_ppi_all.csv", containing a PPI series that is the average of all
   commodities extracted from "SeriesReport-20200605101403_71a53b.xlsx".
   The output table has two columns: "date" and "ppi", where "date" is of the
   form "YYYY-MM" and "ppi" is numeric.

'''

import os
import re
import sys

# Adds parent directory to Python module search path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
from xlsx_utils import read_xlsx


def gen_commodities():
    '''Generates the csv file containing all commodity specific series'''
    out_df = pd.DataFrame()
    for filename in os.listdir("raw"):
        if filename.endswith(".xlsx"):
            in_df, series_id, base_date, header_dict = read_xlsx(
                "raw/" + filename, ["Item:"])

            commodity = header_dict["Item:"].strip().title()
            # Remove non-alphanumeric characters
            commodity = re.sub("[^\w]", "", commodity)
            in_df.columns = [commodity + "_" + base_date]

            if out_df.empty:
                out_df = in_df
            else:
                out_df = out_df.join(in_df, how="outer")
        
    out_df.to_csv("cleaned/us_ppi_commodities.csv")


def gen_all():
    '''Generates the csv file containing the average of all commodities'''
    in_df = read_xlsx("raw/SeriesReport-20200605101403_71a53b.xlsx")[0]
    in_df.columns = ["ppi"]
    in_df.to_csv("cleaned/us_ppi_all.csv")


def main():
    gen_commodities()
    gen_all()


if __name__ == "__main__":
    main()
