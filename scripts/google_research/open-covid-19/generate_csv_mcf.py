# -*- coding: utf-8 -*-
"""Copy of covid_csv_tmcf.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/145j9qlV_TCWafqVnaVBtmFFsodYV4X5V
"""

import re

import pandas as pd
import numpy as np
import requests
from google.colab import files

locations = pd.read_csv(
    'https://raw.githubusercontent.com/google-research/open-covid-19-data/master/data/exports/locations/locations.csv'
)
df = pd.read_csv(
    'https://raw.githubusercontent.com/google-research/open-covid-19-data/master/data/exports/cc_by/aggregated_cc_by.csv'
)
with_id = pd.merge(df, locations, on='region_code')
"""# Remove rows where region_code_type = other"""

with_id = with_id[with_id['region_code_type'] != 'other']
"""# Convert data types of integral columns"""

cols = [
    'cases_cumulative', 'cases_new', 'deaths_cumulative', 'deaths_new',
    'tests_cumulative', 'tests_new', 'hospitalized_current', 'hospitalized_new',
    'hospitalized_cumulative', 'icu_current', 'icu_cumulative',
    'ventilator_current', 'school_closing', 'workplace_closing',
    'restrictions_on_gatherings', 'close_public_transit',
    'stay_at_home_requirements', 'restrictions_on_internal_movement',
    'income_support', 'public_information_campaigns', 'school_closing_flag',
    'workplace_closing_flag', 'restrictions_on_gatherings_flag',
    'close_public_transit_flag', 'stay_at_home_requirements_flag',
    'restrictions_on_internal_movement_flag', 'income_support_flag',
    'public_information_campaigns_flag', 'international_travel_controls',
    'debt_contract_relief', 'testing_policy', 'contact_tracing'
]

for col in cols:
    with_id[col] = with_id[col].astype('Int64')
"""# Add a column that has the observationAbout values"""


def get_observation_about(row):
    if pd.isna(row.datacommons_id):
        return f'l:{row.region_code}'
    return f'dcid:{row.datacommons_id}'


with_id['observationAbout'] = with_id[['region_code', 'datacommons_id'
                                      ]].apply(get_observation_about, axis=1)
assert not any(pd.isna(with_id['observationAbout']))
"""# Split each of tests_cumulative, tests_new into six columns, one for each test_units value. 12 columns are added."""

units = df['test_units'].dropna().unique()
units


def clean_col_name(col):
    return re.sub(r'[^A-Za-z0-9]', '_', str(col))


def get_unique_values(series):
    vals = sorted(series.dropna().unique())
    if any(pd.isna(series)):
        vals.append(pd.NA)
    return vals


def get_col_value_name(col, value):
    return f'{col}_{clean_col_name(value)}'


def split_col_by_col_values(df, col_to_split, col_by):
    values = get_unique_values(df[col_by])
    new_cols = []
    for value in values:
        new_col = get_col_value_name(col_to_split, value)
        if pd.isna(value):
            df[new_col] = df[pd.isna(df[col_by])][col_to_split]
        else:
            df[new_col] = df[df[col_by] == value][col_to_split]
        new_cols.append(new_col)
    return new_cols


def validate_split(df, col_splitted, col_by):
    values = get_unique_values(df[col_by])
    cols = [get_col_value_name(col_splitted, value) for value in values]
    for row in df[[col_splitted, col_by] + cols].to_dict(orient='records'):
        # If the original column is NaN, the created columns are NaNs
        if pd.isna(row[col_splitted]):
            assert all(pd.isna(row[col]) for col in cols)
        # If the original column is not NaN, only one of the created columns
        # is not NaN and has the same value
        else:
            col = get_col_value_name(col_splitted, row[col_by])
            other_cols = list(cols)
            other_cols.remove(col)
            assert all(pd.isna(row[col]) for col in other_cols)
            assert row[col_splitted] == row[col]


cols = ['tests_cumulative', 'tests_new']
cols_added = []
for col in cols:
    cols_added.extend(split_col_by_col_values(with_id, col, 'test_units'))
    validate_split(with_id, col, 'test_units')
cols_added

with_id.dtypes[-len(cols_added):]
"""# Split the policy columns that have enum values."""

cols = [
    'school_closing', 'workplace_closing', 'restrictions_on_gatherings',
    'close_public_transit', 'stay_at_home_requirements',
    'restrictions_on_internal_movement', 'income_support',
    'public_information_campaigns'
]
cols_added = []
for col in cols:
    by = f'{col}_flag'
    cols_added.extend(split_col_by_col_values(with_id, col, by))
    validate_split(with_id, col, by)
cols_added

with_id.columns[-len(cols_added):]
"""# Map Column Names to StatVars"""

col_to_statvar = {
    'cases_cumulative':
        'CumulativeCount_MedicalConditionIncident_COVID_19_ConfirmedCase',
    'cases_new':
        'IncrementalCount_MedicalConditionIncident_COVID_19_ConfirmedCase',
    'deaths_cumulative':
        'CumulativeCount_MedicalConditionIncident_COVID_19_PatientDeceased',
    'deaths_new':
        'IncrementalCount_MedicalConditionIncident_COVID_19_PatientDeceased',
    'tests_cumulative_people_tested':
        'CumulativeCount_Person_COVID_19_Tested_PCR',
    # 'tests_cumulative-people_tested__incl__non_PCR_': 'CumulativeCount_Person_COVID_19_Tested',
    'tests_cumulative_samples_tested':
        'CumulativeCount_MedicalTest_COVID_19_PCR',
    'tests_cumulative_tests_performed':
        'CumulativeCount_MedicalTest_COVID_19_PCR',
    'tests_cumulative_units_unclear':
        'CumulativeCount_MedicalTest_COVID_19_PCR',
    'tests_cumulative_units_unclear__incl__non_PCR_':
        'CumulativeCount_MedicalTest_COVID_19',
    'tests_new_people_tested':
        'IncrementalCount_Person_COVID_19_Tested_PCR',
    # 'tests_new-people_tested__incl__non_PCR_': 'IncrementalCount_Person_COVID_19_Tested',
    'tests_new_samples_tested':
        'IncrementalCount_MedicalTest_COVID_19_PCR',
    'tests_new_tests_performed':
        'IncrementalCount_MedicalTest_COVID_19_PCR',
    'tests_new_units_unclear':
        'IncrementalCount_MedicalTest_COVID_19_PCR',
    'tests_new_units_unclear__incl__non_PCR_':
        'IncrementalCount_MedicalTest_COVID_19',
    'hospitalized_current':
        'Count_MedicalConditionIncident_COVID_19_PatientHospitalized',
    'hospitalized_new':
        'IncrementalCount_MedicalConditionIncident_COVID_19_PatientHospitalized',
    'hospitalized_cumulative':
        'CumulativeCount_MedicalConditionIncident_COVID_19_PatientHospitalized',
    'icu_current':
        'Count_MedicalConditionIncident_COVID_19_PatientInICU',
    'icu_cumulative':
        'CumulativeCount_MedicalConditionIncident_COVID_19_PatientInICU',
    'ventilator_current':
        'Count_MedicalConditionIncident_COVID_19_PatientOnVentilator',
    'international_travel_controls':
        'PolicyExtent_Legislation_COVID19Pandemic_GovernmentOrganization_InternationalTravelRestriction',
    'debt_contract_relief':
        'PolicyExtent_Legislation_COVID19Pandemic_GovernmentOrganization_GovernmentBenefit_DebtOrContractRelief',
    'testing_policy':
        'PolicyExtent_Legislation_COVID19Pandemic_GovernmentOrganization_TestingEligibility',
    'contact_tracing':
        'PolicyExtent_Legislation_COVID19Pandemic_GovernmentOrganization_ContactTracing',
    'emergency_investment_in_healthcare':
        'Amount_Legislation_COVID19Pandemic_GovernmentOrganization_ShortTermSpending_HealthcareExpenditure',
    'investment_in_vaccines':
        'Amount_Legislation_COVID19Pandemic_GovernmentOrganization_ShortTermSpending_VaccineExpenditure',
    'fiscal_measures':
        'Amount_Legislation_COVID19Pandemic_GovernmentOrganization_ShortTermSpending_EconomicStimulusExpenditure',
    'international_support':
        'Amount_Legislation_COVID19Pandemic_GovernmentOrganization_ShortTermSpending_InternationalAidExpenditure',
    'confirmed_cases':
        'CumulativeCount_MedicalConditionIncident_COVID_19_ConfirmedCase',
    'confirmed_deaths':
        'CumulativeCount_MedicalConditionIncident_COVID_19_ConfirmedPatientDeceased',
    'stringency_index':
        'Covid19StringencyIndex_Legislation_COVID19Pandemic_GovernmentOrganization',
    'stringency_index_for_display':
        'Covid19StringencyIndex_Legislation_COVID19Pandemic_GovernmentOrganization',
    'government_response_index':
        'Covid19ResponseIndex_Legislation_COVID19Pandemic_GovernmentOrganization',
    'government_response_index_for_display':
        'Covid19ResponseIndex_Legislation_COVID19Pandemic_GovernmentOrganization',
    'containment_health_index':
        'Covid19ContainmentAndHealthIndex_Legislation_COVID19Pandemic_GovernmentOrganization',
    'containment_health_index_for_display':
        'Covid19ContainmentAndHealthIndex_Legislation_COVID19Pandemic_GovernmentOrganization',
    'economic_support_index':
        'Covid19EconomicSupportIndex_Legislation_COVID19Pandemic_GovernmentOrganization',
    'economic_support_index_for_display':
        'Covid19EconomicSupportIndex_Legislation_COVID19Pandemic_GovernmentOrganization',
}

len(col_to_statvar)

col_to_prefix = {
    'school_closing':
        'PolicyExtent_Legislation_COVID19Pandemic_GovernmentOrganization_SchoolClosure',
    'workplace_closing':
        'PolicyExtent_Legislation_COVID19Pandemic_GovernmentOrganization_WorkplaceClosure',
    'restrictions_on_gatherings':
        'PolicyExtent_Legislation_COVID19Pandemic_GovernmentOrganization_PrivateGatheringRestriction',
    'close_public_transit':
        'PolicyExtent_Legislation_COVID19Pandemic_GovernmentOrganization_PublicTransitClosure',
    'stay_at_home_requirements':
        'PolicyExtent_Legislation_COVID19Pandemic_GovernmentOrganization_StayAtHomeRequirement',
    'restrictions_on_internal_movement':
        'PolicyExtent_Legislation_COVID19Pandemic_GovernmentOrganization_InternalMovementRestriction',
    'income_support':
        'PolicyExtent_Legislation_COVID19Pandemic_GovernmentOrganization_GovernmentBenefit_IncomeSupport',
    'public_information_campaigns':
        'CampaignExtent_PublicInformationCampaign_COVID19Pandemic_GovernmentOrganization'
}


def add_ordinal_statvars(col_to_prefix, col_to_statvar):
    for col, prefix in col_to_prefix.items():
        col_to_statvar[f'{col}_0'] = f'{prefix}_SelectedAdministrativeAreas'
        col_to_statvar[f'{col}_1'] = f'{prefix}_AllAdministrativeAreas'
        col_to_statvar[f'{col}__NA_'] = f'{prefix}_SpatialCoverageUnknown'


add_ordinal_statvars(col_to_prefix, col_to_statvar)

len(col_to_statvar)
"""# Remove Irrelevant Columns"""

cols = ['observationAbout', 'date'] + list(col_to_statvar.keys())
with_id = with_id[cols]
"""# Download cleaned CSV"""

with_id.to_csv('open_covid_19_data_cleaned.csv', index=False)
files.download('open_covid_19_data_cleaned.csv')
"""# Generate MCFs for ISO Nodes"""

with open('open_covid_19_data_cleaned.tmcf', 'w') as tmcf_out:
    template = ('Node: {iso}\n' 'typeOf: schema:Place\n' 'isoCode: "{iso}"\n')
    for dcid in with_id['observationAbout'].unique():
        if dcid.startswith('l:'):
            tmcf_out.write(template.format_map({'iso': dcid[2:]}))
            tmcf_out.write('\n')
"""# Generate Template MCFs for Regular Columns (cases, deaths, tests, hospitalizations, ICUs, ventilators)"""

col_to_tmcf = {}


def add_regular_tmcfs(cols,
                      col_to_statvar,
                      starting_index,
                      col_to_tmcf,
                      extra_line=None):
    template = (
        'Node: E:open_covid_19_data_cleaned->E{index}\n'
        'typeOf: dcs:StatVarObservation\n'
        'variableMeasured: dcs:{statvar}\n'
        'measurementMethod: dcs:OpenCovid19Data\n'
        'observationAbout: C:open_covid_19_data_cleaned->observationAbout\n'
        'observationDate: C:open_covid_19_data_cleaned->date\n'
        'value: C:open_covid_19_data_cleaned->{column_name}\n')
    for col in cols:
        tmcf = template.format_map({
            'index': starting_index,
            'statvar': col_to_statvar[col],
            'column_name': col
        })
        if extra_line:
            tmcf += extra_line
        col_to_tmcf[col] = tmcf
        starting_index += 1
    return starting_index


cols = [
    'cases_cumulative',
    'cases_new',
    'deaths_cumulative',
    'deaths_new',
    'tests_cumulative_people_tested',
    # 'tests_cumulative-people_tested__incl__non_PCR_',
    'tests_cumulative_samples_tested',
    'tests_cumulative_tests_performed',
    'tests_cumulative_units_unclear',
    'tests_cumulative_units_unclear__incl__non_PCR_',
    'tests_new_people_tested',
    # 'tests_new-people_tested__incl__non_PCR_',
    'tests_new_samples_tested',
    'tests_new_tests_performed',
    'tests_new_units_unclear',
    'tests_new_units_unclear__incl__non_PCR_',
    'hospitalized_current',
    'hospitalized_new',
    'hospitalized_cumulative',
    'icu_current',
    'icu_cumulative',
    'ventilator_current',
]
index = add_regular_tmcfs(cols, col_to_statvar, 0, col_to_tmcf)
print(index)
col_to_tmcf
"""# Generate Template MCFs for Culumns that Have Units (investment, policy)"""


def add_unit_tmcfs(col_to_unit, col_to_statvar, starting_index, col_to_tmcf):
    template = (
        'Node: E:open_covid_19_data_cleaned->E{index}\n'
        'typeOf: dcs:StatVarObservation\n'
        'variableMeasured: dcs:{statvar}\n'
        'observationAbout: C:open_covid_19_data_cleaned->observationAbout\n'
        'observationDate: C:open_covid_19_data_cleaned->date\n'
        'value: C:open_covid_19_data_cleaned->{column_name}\n'
        'measurementMethod: dcs:OpenCovid19Data\n'
        'unit: {unit}\n'
        'statType: dcs:measuredValue\n')
    for col, unit in col_to_unit.items():
        col_to_tmcf[col] = template.format_map({
            'index': starting_index,
            'statvar': col_to_statvar[col],
            'column_name': col,
            'unit': unit
        })
        starting_index += 1
    return starting_index


col_to_unit = {
    'emergency_investment_in_healthcare':
        'schema:USDollar',
    'investment_in_vaccines':
        'schema:USDollar',
    'fiscal_measures':
        'schema:USDollar',
    'international_support':
        'schema:USDollar',
    'international_travel_controls':
        'dcs:ExtentOfPolicyInternationalTravelRestriction',
    'debt_contract_relief':
        'dcs:ExtentOfPolicyDebtOrContractRelief',
    'testing_policy':
        'dcs:ExtentOfPolicyTestingEligibility',
    'contact_tracing':
        'dcs:ExtentOfPolicyContactTracing'
}

ordinal_col_to_unit = {
    'school_closing':
        'dcs:ExtentOfPolicySchoolClosure',
    'workplace_closing':
        'dcs:ExtentOfPolicyWorkplaceClosure',
    'restrictions_on_gatherings':
        'dcs:ExtentOfPolicyPrivateGatheringRestriction',
    'close_public_transit':
        'dcs:ExtentOfPolicyPublicTransitClosure',
    'stay_at_home_requirements':
        'dcs:ExtentOfPolicyStayAtHomeRequirement',
    'restrictions_on_internal_movement':
        'dcs:ExtentOfPolicyInternalMovementRestriction',
    'income_support':
        'dcs:ExtentOfPolicyIncomeSupport',
    'public_information_campaigns':
        'dcs:ExtentOfPublicInformationCampaign',
}


def add_ordinal_col_units(col_to_unit, ordinal_col_to_unit):
    for col, unit in ordinal_col_to_unit.items():
        for suffix in ('_0', '_1', '__NA_'):
            col_to_unit[col + suffix] = unit


add_ordinal_col_units(col_to_unit, ordinal_col_to_unit)
index = add_unit_tmcfs(col_to_unit, col_to_statvar, index, col_to_tmcf)
print(index)
col_to_tmcf
"""# Generate Template MCFs for Index Columns (regular, for display)"""

index_cols = [
    'stringency_index', 'government_response_index', 'containment_health_index',
    'economic_support_index'
]
display_cols = [
    'stringency_index_for_display', 'government_response_index_for_display',
    'containment_health_index_for_display', 'economic_support_index_for_display'
]

index = add_regular_tmcfs(index_cols, col_to_statvar, index, col_to_tmcf)
index = add_regular_tmcfs(
    display_cols, col_to_statvar, index, col_to_tmcf,
    'measurementQualifer: dcs:SmoothedByRepeatingLatestPoint\n')
print(index)
col_to_tmcf
"""# Genearte Template MCFs for confirmed_cases and confirmed_deaths"""


def add_confirmed_tmcfs(cols, col_to_statvar, starting_index, col_to_tmcf):
    template = (
        'Node: E:open_covid_19_data_cleaned->E{index}\n'
        'typeOf: dcs:StatVarObservation\n'
        'variableMeasured: dcs:{statvar}\n'
        'measurementMethod: dcs:OxCGRTViaOpenCovid19Data\n'
        'observationAbout: C:open_covid_19_data_cleaned->observationAbout\n'
        'observationDate: C:open_covid_19_data_cleaned->date\n'
        'value: C:open_covid_19_data_cleaned->{column_name}\n')
    for col in cols:
        col_to_tmcf[col] = template.format_map({
            'index': starting_index,
            'statvar': col_to_statvar[col],
            'column_name': col
        })
        starting_index += 1
    return starting_index


cols = ['confirmed_cases', 'confirmed_deaths']
index = add_confirmed_tmcfs(cols, col_to_statvar, index, col_to_tmcf)
print(index)
col_to_tmcf

assert col_to_statvar.keys() == col_to_tmcf.keys()

with open('open_covid_19_data_cleaned.tmcf', 'a') as tmcf_out:
    for tmcf in col_to_tmcf.values():
        tmcf_out.write(tmcf)
        tmcf_out.write('\n')

files.download('open_covid_19_data_cleaned.tmcf')

cols = [
    'tests_new_people_tested', 'tests_new_samples_tested',
    'tests_new_tests_performed', 'tests_new_units_unclear',
    'tests_new_units_unclear__incl__non_PCR_'
]

for dcid, group in with_id[['date', 'observationAbout'] +
                           cols].groupby('observationAbout'):
    group = group.fillna(0)
    skip = True
    for col in cols:
        if any(group[col] != 0):
            skip = False
            break
    if skip:
        continue
    group.sort_values(by='date').plot(x='date', figsize=(10, 5), title=dcid)