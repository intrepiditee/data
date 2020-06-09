'''
Generates the instance MCFs "us_ppi_commodities.mcf" and template MCFs
"us_ppi_commodities.tmcf" in "mcf" directory for the commodity specific
PPI series.
'''


import pandas as pd


def main():
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


if __name__ == "__main__":
    main()
