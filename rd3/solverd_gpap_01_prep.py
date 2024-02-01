"""GPAP Experiment Prep"""

import re
from os import environ
from datatable import dt, f
from rd3tools.molgenis import Molgenis, get_table_attribs
from rd3tools.utils import print2
from dotenv import load_dotenv
load_dotenv()

rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])


gpap_dt = get_table_attribs(
    client=rd3,
    pkg_entity='solverdportal_experiments',
    nested_columns='name'
)

erns_dt = get_table_attribs(
    client=rd3,
    pkg_entity='solverd_info_erns',
    columns='id,shortname',
)

# apply transforms to make validation/mapping a bit easier
# these transformations should only be run once

gpap_dt['seq_type'] = dt.Frame([
    value.strip() if bool(value) else value
    for value in gpap_dt['seq_type'].to_list()[0]
])

# create a second sample_id column to combine novelomics and virtual samples
gpap_dt['sample_id2'] = dt.Frame([
    f"VS{row[2]}" if row[0] is None and 'SolveDF' in row[1] else row[0]
    for row in gpap_dt[:, (f.sample_id, f.subproject, f.rdconnect_id)].to_tuples()
])

gpap_dt['erns_rd3'] = dt.Frame([
    None if value.lower() == 'not_applicable'
    else re.sub(r'([-]|\s+)', '_', value.lower())
    for value in gpap_dt['erns'].to_list()[0]
])

gpap_erns = dt.unique(gpap_dt['erns_rd3']).to_list()[0]
for value in gpap_erns:
    if value not in erns_dt['id'].to_list()[0] and value is not None:
        print2('ERNS', value, 'is not known')

rd3.import_dt('solverdportal_experiments', gpap_dt)
