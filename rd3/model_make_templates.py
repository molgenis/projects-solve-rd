#'////////////////////////////////////////////////////////////////////////////
#' FILE: model_make_templates.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-05-30
#' MODIFIED: 2022-05-30
#' PURPOSE: produce templates for novelomics tables
#' STATUS: stable
#' PACKAGES: yaml
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import yaml

with open('model/rd3_portal_novelomics.yaml', 'r') as stream:
    emx=yaml.safe_load(stream)
    stream.close()
    del stream

tables=[row['name'] for row in emx['entities']]
columnsToIgnore=['date_created','processed','molgenis_id']

for table in tables:
    attributes=[entity['attributes'] for entity in emx['entities'] if entity['name']==table][0]
    columns=[row['name'] for row in attributes if row['name'] not in columnsToIgnore]

    filename=f'templates/rd3_portal_novelomics_{table}.csv'
    with open(filename, 'w') as file:
        file.write(','.join(columns))
        file.close()
        del file
