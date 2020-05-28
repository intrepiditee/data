'''
Converts .xlsx files to .csv files and writes out instance and template MCFs.
'''
import sys
import os
import pandas as pd
import numpy as np

USAGE = '''
python3 cpi2mcf.py [path] [dcid] [adjusted] [chained] [consumer] [products] [unit]
    "path": path to the xlsx file containing the data
    "dcid": dcid of the new StatisticalVariable to create
    "adjusted": boolean indicating if the index is seasonally adjusted
    "chained": boolean indicating if the index is chained
    "consumer": one of "Urban", "Wage"
    "products": one of "ConsumerGoodsAndServices", ...
    "unit": unit of the index, e.g. "IndexPointBasePeriod1982-84=100"

Example of CPI-U:
python3 cpi2mcf.py cpi_u_1913_2020.xlsx CPI-U False False Urban \
ConsumerGoodsAndServices IndexPointBasePeriod1982-84=100
'''


def read_xlsx(path):
    '''
    Reads in the xlsx file and converts it to a table of two columns:
    "date" and "cpi".
    "date" is of the form "YYYY-MM" and "cpi" is numeric.
    '''
    xlsx_df = pd.read_excel(path, header=11, index_col=0, usecols="A:M")
    data = np.reshape(xlsx_df.values, (-1, 1))
    index = ["{}-{:02d}".format(year, month) for year in xlsx_df.index for month in range(1, 13)]
    csv_df = pd.DataFrame(data=data, index=index)
    csv_df.reset_index(inplace=True)
    csv_df.dropna(inplace=True)
    csv_df.columns = ["date", "cpi"]
    return csv_df


def write_template_mcf(filename, variable_name, location="country/USA"):
    '''
    Writes out the template MCF.
    "variable_name" is the name of the StatisticalVariable.
    '''
    with open(filename + ".template.mcf", "w") as out:
        out.write("Node: E:{}->E1\n".format(filename))
        out.write("typeOf: StatVarObservation\n")
        out.write("variableMeasured: {}\n".format(variable_name))
        out.write("observationAbout: {}\n".format(location))
        out.write("observationDate: C:{}->date\n".format(filename))
        out.write("value: C:{}->cpi\n".format(filename))


def get_measurement_method(adjusted, chained):
    method = "BLS_Chained" if chained else "BLS_Unchained"
    method += ", "
    method += "BLS_SeasonallyAdjusted" if adjusted else "BLS_SeasonallyUnadjusted"
    return method


def write_schema_mcf(filename, variable_name, adjusted, chained, consumer,
                     products, unit):
    '''
    Writes out MCF that specifies the schema.
    '''
    with open(filename + ".schema.mcf", "w") as out:
        out.write("Node: dcid:{}\n".format(variable_name))
        out.write("typeOf: dcs:StatisticalVariable\n")
        out.write("populationType: dcs:{}\n".format(products))
        out.write("measuredProperty: dcs:price\n")
        out.write("statType: dcs:measuredValue\n")
        out.write("measurementMethod: {}\n".format(get_measurement_method(adjusted, chained)))
        out.write("consumer: dcs:{}\n".format(consumer))
        out.write("unit: {}\n".format(unit))


def main():
    if len(sys.argv) != 8:
        print(USAGE, file=sys.stderr)
        return

    xlsx_path = sys.argv[1]
    filename = os.path.basename(xlsx_path).split(".")[0]
    variable_name = sys.argv[2]
    adjusted = True if sys.argv[3].lower() == "true" else False
    chained = True if sys.argv[4].lower() == "true" else False
    consumer = sys.argv[5]
    products = sys.argv[6]
    unit = sys.argv[7]

    df = read_xlsx(xlsx_path)
    df.to_csv(filename + ".csv", index=False)
    write_template_mcf(filename, variable_name)
    write_schema_mcf(filename, variable_name, adjusted, chained, consumer, products, unit)


if __name__ == "__main__":
    main()
