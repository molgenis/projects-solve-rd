#'////////////////////////////////////////////////////////////////////////////
#' FILE: novelomics_templates.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-05-04
#' MODIFIED: 2021-05-04
#' PURPOSE: create csv templates
#' STATUS: working
#' PACKAGES: pandas
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

# import
import pandas as pd
df = pd.read_csv('data/novelomics/novelomics_attributes.csv')

# transform EMX attributes into Csv templates
df[(df['entity'] == 'shipment') & (~df['name'].isin(['processed','molgenis_id']))][['name']] \
    .transpose() \
    .to_csv('templates/rd3_portal_novelomics_shipment.csv', header=False, index=False)

df[(df['entity'] == 'experiment') & (~df['name'].isin(['processed','molgenis_id']))][['name']] \
    .transpose() \
    .to_csv('templates/rd3_portal_novelomics_experiment.csv', header=False, index=False)