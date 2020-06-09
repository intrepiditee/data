'''
Generates the instance MCFs "us_ppi_industries.mcf" and template MCFs
"us_ppi_industries.tmcf" in "mcf" directory for the industry specific
PPI series.
'''


import pandas as pd


def main():
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


if __name__ == "__main__":
    main()
