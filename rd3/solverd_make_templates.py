#///////////////////////////////////////////////////////////////////////////////
# FILE: solverd_make_templates.py
# AUTHOR: David Ruvolo
# CREATED: 2022-12-09
# MODIFIED: 2022-12-09
# PURPOSE: generate portal table templates
# STATUS: stable
# PACKAGES: pandas, yaml, csv
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

import pandas as pd
import yaml
import csv

# open yaml-emx for portal
with open('model/portal/rd3_portal_novelomics.yaml', 'r') as file:
  portalYAML = yaml.safe_load(file)
  file.close()

# extract attributes for each and write headers to xlsx
for entity in portalYAML['entities']:
  attributes = [
    attr['name'] for attr in entity['attributes']
    if attr['name'] not in ['date_created', 'processed', 'molgenis_id']
  ]
  pkg_entity = f"rd3_portal_novelomics_{entity['name']}"
  filepath = f"templates/{pkg_entity}.csv"
  df = pd.DataFrame(columns=attributes)
  df.to_csv(filepath,index=False, quoting=csv.QUOTE_ALL)
