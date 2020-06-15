# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''
Generates:
    1) 22 commodity specific PPI series specified by "SERIES_IDS" and
       a commodity average PPI series to a single csv file
       "cleaned/us_ppi_commodities.csv".

       The output table has a "date" column of the form "YYYY-MM" and 23 other
       columns each representing the series for a type of commodity.
       The names of the 23 columns are of the form
       "{commodity name}_{base date}", where "base date" is of the form
       "YYYY" or "YYYY-MM".
    
    2) StatisticalVariable instance MCFs "us_ppi_commodities.mcf" and template
       MCFs "us_ppi_commodities.tmcf".

       Off by default. Must be run with "-commodities=true" or after the
       series have been generated.

Run "python3 generate_csv_and_mcf.py --help" for usage.
'''

from io import StringIO
import os
import re
import sys

# Adds parent directory to Python module search path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from absl import app
from absl import flags

FLAGS = flags.FLAGS
flags.DEFINE_boolean(
    "commodities", True, "Generates the commodity specific PPI series.")
flags.DEFINE_boolean(
    "mcf", False,
    "Generates the template and StatisticalVariable instance MCFs for the " \
    "commodity specific series.")

import requests
import pandas as pd
from xlsx_utils import read_xlsx, request_excel


# https://data.bls.gov/cgi-bin/surveymost?wp
SERIES_IDS = [
    "WPU1411", "WPU0638", "WPU0571", "WPU0221", "WPU061", "WPU081", "WPU1017",
    "WPU057303", "WPU029", "WPU0561", "WPU012", "WPU101211", "WPU5111",
    "WPU5121", "WPU5811", "WPU5831", "WPU3022", "WPU4011", "WPU3911", "WPU4511",
    "WPU3012", "WPU571", "WPU00000000"
]


def generate_csv_commodities():
    '''Downloads, combines, and stores commodity specific and
    commodity average PPI series.'''
    
    out_df = pd.DataFrame()

    for series_id in SERIES_IDS:  
        response = request_excel(series_id)
        in_df, series_id_in, base_date, header_dict = read_xlsx(
            response.content, ["Item:"])
        assert(series_id == series_id_in)

        commodity = header_dict["Item:"].strip().title()
        # Remove non-alphanumeric characters
        commodity = re.sub("[^\w]", "", commodity)
        in_df.columns = [commodity + "_" + base_date]

        if out_df.empty:
            out_df = in_df
        else:
            out_df = out_df.join(in_df, how="outer")
        
    out_df.to_csv("cleaned/us_ppi_commodities.csv")


def generate_mcf():
    '''Generates the StatisticalVariable instance and template MCFs.'''

    variable_template = ( 
        'Node: dcid:US_PPI_{commodity}\n'
        'name: "US_PPI_{commodity}"\n'
        'typeOf: dcs:StatisticalVariable\n'
        'populationType: dcs:ConsumerGoodsAndServices\n'
        'commodity: dcs:{commodity}\n'
        'measuredProperty: dcs:price\n'
        'statType: dcs:measuredValue\n'
        'unit: dcs:IndexPointBasePeriod{base_date}=100\n'
    )
    template_template = (
        'Node: E:us_ppi_commodities->E{index}\n'
        'typeOf: dcs:StatVarObservation\n'
        'variableMeasured: dcs:US_PPI_{commodity}\n'
        'observationAbout: dcid:country/USA\n'
        'observationDate: C:us_ppi_commodities->date\n'
        'value: C:us_ppi_commodities->{col}\n'
    )
    in_df = pd.read_csv("cleaned/us_ppi_commodities.csv", index_col=0)

    with open("mcf/us_ppi_commodities.mcf", "w") as mcf_out, \
         open("mcf/us_ppi_commodities.tmcf", "w") as tmcf_out:

        index = 1
        for col in in_df.columns:
            commodity, base_date = col.split("_")
            format_dict = {
                "index": index, "col": col,
                "commodity": commodity, "base_date": base_date,
            }
            mcf_out.write(variable_template.format_map(format_dict))
            mcf_out.write("\n")
            tmcf_out.write(template_template.format_map(format_dict))
            tmcf_out.write("\n")
            index += 1


def main(argv):
    del argv

    if FLAGS.commodities:
        generate_csv_commodities()
    if FLAGS.mcf:
        generate_mcf()


if __name__ == "__main__":
    app.run(main)
