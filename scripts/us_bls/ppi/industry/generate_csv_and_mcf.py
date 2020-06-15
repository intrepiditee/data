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
    1) Industry specific PPI series specified by "SERIES_IDS" to a single
       csv file "cleaned/us_ppi_industries.csv".

       The output table has a "date" column of the form "YYYY-MM" and 39 other
       columns each representing the series for an industry.
       The names of the 39 columns are of the form
       "{NAICS code}_{base date}_{industry name}", where "base date" is
       of the form "YYYY-MM".
       "NAICS code", "base date", and "industry name" do not have
       underscores in them.
        
    2) Industry average PPI series "PCUOMFG--OMFG--" to
       "cleaned/us_ppi_total.csv".

       The output table has two columns: "date" and "ppi", where "date" is
       of the form "YYYY-MM" and "ppi" is numeric.
    
    3) StatisticalVariable instance MCFs "us_ppi_industries.mcf" and template
       MCFs "us_ppi_industries.tmcf" for the industry specific PPI series in 
       in "mcf" subdirectory.

       Off by default. Must be run with "-industries=true" or after the industry
       specific series have been generated.

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
    "average", True, "Generates the industry average PPI series.")
flags.DEFINE_boolean(
    "industries", True, "Generates the industry specific PPI series.")
flags.DEFINE_boolean(
    "mcf", False,
    "Generates the template and StatisticalVariable instance MCFs for the " \
    "industry specific series.")

import requests
import pandas as pd
from xlsx_utils import read_xlsx


NAICS_URL = '''
https://www.census.gov/eos/www/naics/2017NAICS/2017_NAICS_Structure.xlsx
'''.strip()

# https://data.bls.gov/cgi-bin/surveymost?pc
SERIES_IDS = [
    "PCU221122221122", "PCU311421311421", "PCU324191324191", "PCU325120325120",
    "PCU325---325---", "PCU3251803251806", "PCU325199325199P",
    "PCU325211325211", "PCU325212325212P", "PCU325412325412", "PCU325510325510",
    "PCU331110331110", "PCU331315331315", "PCU332322332322", "PCU332510332510",
    "PCU332618332618", "PCU332911332911", "PCU332991332991", "PCU333120333120",
    "PCU333132333132", "PCU333611333611", "PCU33441K33441K", "PCU335311335311",
    "PCU335313335313", "PCU335931335931", "PCU336110336110", "PCU336412336412",
    "PCU336413336413", "PCU4240004240002", "PCU4240004240004",
    "PCU441110441110", "PCU481111481111", "PCU482111482111", "PCU445110445110",
    "PCU484---484---", "PCU4841214841212", "PCU532412532412", "PCU621111621111",
    "PCU622110622110"
]


def get_naics_dict():
    '''Downloads the official NAICS mapping and converts it to a dict mapping
    from NAICS codes to industry names.'''
    
    code_col = "2017 NAICS Code"
    name_col = "2017 NAICS Title"
    # Header is the third row
    in_df = pd.read_excel(NAICS_URL, header=2, index_col=None, dtype=str)[[
        code_col, name_col]]
    in_df.dropna(inplace=True)
    return pd.Series(
        data=in_df[name_col].values, index=in_df[code_col].values).to_dict()


def request_excel(series_id):
    '''Requests the PPI series and returns the Response.
    
    Args:
        series_id: The ID of the series as a string.
    '''
    form = {
        "request_action": "get_data",
        "reformat": "true",
        "from_results_page": "true",
        "years_option": "specific_years",
        "delimiter": "comma",
        "output_type": "multi",
        "periods_option": "all_periods",
        "output_view": "data",
        "from_year": "1000",
        "output_format": "excelTable",
        "original_output_type": "default",
        "annualAveragesRequested": "false",
        "series_id": f"{series_id}"
    }
    return requests.post(
        "https://data.bls.gov/pdq/SurveyOutputServlet", data=form)



def generate_csv_industries():
    '''Downloads, combines, and stores industry specific PPI series.'''
    out_df = pd.DataFrame()
    naics_dict = get_naics_dict()

    for series_id in SERIES_IDS:  
        response = request_excel(series_id)
        in_df, series_id_in, base_date, _ = read_xlsx(response.content)
        assert(series_id == series_id_in)

        # Series IDs for PPIs are of the form "PCUXXXXXXYYYYYY", where
        # "XXXXXX" is the numerical NAICS code.
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

        industry = naics_dict.get(naics)
        if not industry:
            # These series need to be handled separatey
            print(series_id + " does not have corresponding NAICS")
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


def generate_csv_average():
    '''Downloads and stores the industry average PPI series.'''
    series_id = "PCUOMFG--OMFG--"
    response = request_excel(series_id)
    in_df = read_xlsx(response.content)[0]
    in_df.columns = ["ppi"]
    in_df.to_csv("cleaned/us_ppi_total.csv")


def generate_mcf():
    '''Generates the StatisticalVariable instance and template MCFs.'''

    variable_template = ( 
        'Node: dcid:US_PPI_{naics_id}\n'
        'name: "US_PPI_{industry}"\n'
        'typeOf: dcs:StatisticalVariable\n'
        'populationType: dcs:ConsumerGoodsAndServices\n'
        'naics: dcid:NAICS/{naics_id}\n'
        'measuredProperty: dcs:price\n'
        'statType: dcs:measuredValue\n'
        'unit: dcs:IndexPointBasePeriod{base_date}=100\n'
    )
    template_template = (
        'Node: E:us_ppi_industries->E{index}\n'
        'typeOf: dcs:StatVarObservation\n'
        'variableMeasured: dcs:US_PPI_{naics_id}\n'
        'observationAbout: dcid:country/USA\n'
        'observationDate: C:us_ppi_industries->date\n'
        'value: C:us_ppi_industries->{col}\n'
    )
    in_df = pd.read_csv("cleaned/us_ppi_industries.csv", index_col=0)

    with open("mcf/us_ppi_industries.mcf", "w") as mcf_out, \
         open("mcf/us_ppi_industries.tmcf", "w") as tmcf_out:

        index = 1
        for col in in_df.columns:
            naics, base_date, industry = col.split("_")
            format_dict = {
                "index": index,
                "naics_id": naics, "base_date": base_date,
                "industry": industry, "col": col
            }
            mcf_out.write(variable_template.format_map(format_dict))
            mcf_out.write("\n")
            tmcf_out.write(template_template.format_map(format_dict))
            tmcf_out.write("\n")
            index += 1


def main(argv):
    del argv

    if FLAGS.industries:
        generate_csv_industries()
    if FLAGS.average:
        generate_csv_average()
    if FLAGS.mcf:
        generate_mcf()


if __name__ == "__main__":
    app.run(main)
