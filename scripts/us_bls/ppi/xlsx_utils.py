'''
Utility functions for dealing with xlsx files downloaded from BLS.
'''

import numpy as np
import pandas as pd


def read_xlsx(path, extra_keys=[]):
    '''Reads in a BLS xlsx file containing time series data and converts it to
    a DataFrame of two columns: "date" and "value".

    "date" is of the form "YYYY", "YYYY-MM" or "YYYY-YY"
    and "value" is numeric.
    Returns the converted Dataframe, series ID, and base date of the dataset. 

    Xlsx files provided by BLS start with several rows of header information,
    such as series ID and base date, about the dataset, followed by
    the actual data.
    Column A is the index (year).
    Columns B-M are the 12 columns that correspond to the 12 months in a year,
    so in the data portion of the xlsx file, each row represents a year and
    contains the values for that year.

    Args:
        path: Path to a BLS xlsx file.
        extra_keys: List of extra header keys to extract from the xlsx. E.g.
                    "Series Title:".

    Returns:
        1. A DateFrame converted from the xlsx file.
        2. The series ID of the time series contained in the file.
        3. The base date of the time series contained in the file.
        4. dict mapping from extra header keys provided to their values. E.g.
           {"Series Title:": "PPI Commodity data, not seasonally adjusted"}

    '''
    xlsx_df = pd.read_excel(path, index_col=0, usecols="A:M")
    series_id = xlsx_df.loc["Series Id:"][0]
    base_date = xlsx_df.loc["Base Date:"][0]
    # Base date is of the form "YYYYMM" or "YYYY-YY=100" in the xlsx
    if base_date[4] != "-":
        # Month of "00" indicates that the base date is the whole year
        if base_date[4:] == "00":
            base_date = base_date[:4]
        else:
            base_date = base_date[:4] + "-" + base_date[4:]
    else:
        base_date = base_date[:-4]
    extras_dict = {}
    for key in extra_keys:
        extras_dict[key] = xlsx_df.loc[key][0]

    start_row = np.where(xlsx_df.index == "Year")[0][0] + 1
    # Remove the headers
    xlsx_df.drop(xlsx_df.index[:start_row], inplace=True)
    # The original table is "year" x "month".
    # We want a table that is "date" x "value".
    data = np.reshape(xlsx_df.values, (-1, 1))
    index = ["{}-{:02d}".format(year, month) for year in xlsx_df.index for month in range(1, 13)]
    csv_df = pd.DataFrame(data=data, index=index)
    # BLS does not have values for the most recent months in the current year, so the
    # last few rows will contain NaNs.
    csv_df.dropna(inplace=True)
    csv_df.index.rename("date", inplace=True)
    csv_df.columns = ["value"]

    return csv_df, series_id, base_date, extras_dict
