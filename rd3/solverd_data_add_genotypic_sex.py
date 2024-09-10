"""Retrospectively add genotypic sex
FILE: solverd_data_add_genotypic_sex.py
AUTHOR: David Ruvolo
CREATED: 2024-09-10
MODIFIED: 2024-09-10
PURPOSE: add missing genotypic sex data from file
STATUS: in.progress
PACKAGES: **see below**
COMMENTS: NA
"""

from os import environ
import pandas as pd
from dotenv import load_dotenv
from rd3tools.molgenis import Molgenis
from rd3tools.utils import print2, flatten_data
from datatable import dt, f, as_type

print2('Connecting to RD3....')
load_dotenv()
rd3 = Molgenis(url=environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# ///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Retrieve metadata

# get samples
samples_raw = rd3.get('solverd_samples', batch_size=1000)
samples_flat = flatten_data(samples_raw, "subjectID|id|value")
samples_dt = dt.Frame(samples_flat)
samples_dt['doesExist'] = False
samples_dt['shouldImport'] = False
sample_ids = samples_dt['sampleID'].to_list()[0]

if 'sex2' not in samples_dt.names:
    samples_dt['sex2'] = samples_dt[:, as_type(None, dt.Type.str32)]

# get experiments
experiments_raw = rd3.get(
    'solverd_labinfo',
    batch_size=1000,
    attributes='experimentID,sampleID'
)

experiments_flat = flatten_data(experiments_raw, 'sampleID|id|value')
experiments_dt = dt.Frame(experiments_flat)
experiment_ids = experiments_dt['experimentID'].to_list()[0]

# load source dataset
source_df = pd.read_excel(
    '~/Desktop/2024-02-08_Solve-RD_WGS_3043_of_3126_SexCheck.xlsx')
source_dt = dt.Frame(source_df.to_dict('records'))


# ///////////////////////////////////////

# ~ 1a ~
# Merge sample ids to source data
for expr_id in source_dt['experiment_ID'].to_list()[0]:
    if expr_id in experiment_ids:
        expr_id_row = experiments_dt[f.experimentID == expr_id, :]
        source_dt[f.experiment_ID == expr_id,
                  'sampleID'] = expr_id_row['sampleID'][0][0]


# Identify records that do not exist in RD3
source_dt['doesExist'] = dt.Frame([
    id in sample_ids for id in source_dt['sampleID'].to_list()[0]
])

# check counts
source_dt[:, dt.count(), dt.by(f.doesExist)]

# reduce data to records that exist and select columns of interest
reduced_dt = source_dt[f.doesExist, {
    'sampleID': f.sampleID,
    'belongsToSubject': f.participant_subject,
    'sex1': f['declared sex in GPAP'],
    'sex2': f['experimentally determined sex based upon coverage of Chromosome Y']
}]

for row in reduced_dt.to_tuples():
    samples_dt[
        f.sampleID == row[0], (f.sex1, f.sex2, f.shouldImport)
    ] = (row[2], row[3], True)

# check
# samples_dt[f.shouldImport,(f.sex1, f.sex2)]

import_dt = samples_dt[f.shouldImport, :]
rd3.import_dt('solverd_samples', import_dt)

source_dt['doesExist'] = source_dt[:, as_type(f.doesExist, dt.Type.str32)]
source_dt.to_csv('~/Desktop/solverd_wgs_sex_check.csv')
