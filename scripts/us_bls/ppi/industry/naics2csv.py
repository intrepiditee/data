'''
Converts the official 22017 NAICS mapping from xlsx file to csv file.
The original xlsx file is "2017_NAICS_Structure.xlsx".
The output table will be saved in "naics.csv" and have two columns:
"2017 NAICS Code" and "2017 NAICS Title".
'''

import pandas as pd


def main():
    in_df = pd.read_excel("2017_NAICS_Structure.xlsx", header=2, usecols="B:C",
                          index_col=0)
    in_df.dropna(inplace=True)
    in_df.to_csv("naics.csv")
    

if __name__ == "__main__":
    main()
