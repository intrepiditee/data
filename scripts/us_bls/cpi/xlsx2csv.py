'''
Converts .xlsx files containing BLS CPI data to .csv files of two columns:
date and cpi. "date" is of the form "YYYY-MM" and cpi is numeric.
'''
import sys
import os
import pandas as pd
import numpy as np

USAGE = '''
python3 xlsx2csv.py [input]
    "path": path to the xlsx file containing the data

Example of CPI-U: python3 xlsx2csv.py cpi_u_1913_2020.xlsx
    This will output "cpi_u_1913_2020.csv" in the current directory.
'''


def read_xlsx(path):
    '''
    Reads in the xlsx file and converts it to a DataFrame of two columns:
    "date" and "cpi".
    "date" is of the form "YYYY-MM" and "cpi" is numeric.

    In the xlsx files provided by BLS, header starts at Row 12 (counting from one).
    Rows 1-11 are descriptions of the dataset. Column A or 0 is the index (year).
    Columns B-M are the 12 columns that correspond to the 12 months in a year, so
    each row represents a year and contains the CPIs for that year. 
    '''
    xlsx_df = pd.read_excel(path, header=11, index_col=0, usecols="A:M")
    # The original table is "year" x "month".
    # We want a table that is "date" x "cpi".
    data = np.reshape(xlsx_df.values, (-1, 1))
    index = ["{}-{:02d}".format(year, month) for year in xlsx_df.index for month in range(1, 13)]
    csv_df = pd.DataFrame(data=data, index=index)
    csv_df.reset_index(inplace=True)
    # BLS does not have CPIs for the most recent months in the current year, so the
    # last few rows will contain NaNs.
    csv_df.dropna(inplace=True)
    csv_df.columns = ["date", "cpi"]
    return csv_df


def main():
    if len(sys.argv) != 2:
        print(USAGE, file=sys.stderr)
        return

    xlsx_path = sys.argv[1]
    filename = os.path.basename(xlsx_path).split(".")[0]

    table = read_xlsx(xlsx_path)
    table.to_csv(filename + ".csv", index=False)


if __name__ == "__main__":
    main()
