"""Retrospectively add coverage data
FILE: solverd_data_add_coverage.py
AUTHOR: David Ruvolo
CREATED: 2024-09-10
MODIFIED: 2024-09-10
PURPOSE: add missing coverage data from file
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

# when deployed
# for local dev
# from os import environ
print2('Connecting to RD3....')
load_dotenv()
rd3 = Molgenis(url=environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# ///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Retrieve metadata
experiments_raw = rd3.get('solverd_labinfo', batch_size=1000)
experiments_flat = flatten_data(experiments_raw, 'sampleID|id|value')
experiments_dt = dt.Frame(experiments_flat)
experiments_dt['shouldImport'] = False


source_df = pd.read_excel('~/Desktop/Novel_srWGS_sample_coverage_RD3.xlsx')
source_dt = dt.Frame(source_df.to_dict('records'))

# ///////////////////////////////////////

# ~ 1a ~
# Check to make sure all incoming records are in RD3

experiment_ids = experiments_dt['experimentID'].to_list()[0]
source_dt['doesExist'] = dt.Frame([
    id in experiment_ids for id in source_dt['RDConnect_ID'].to_list()[0]
])

# source_dt[:, dt.count(), dt.by(f.doesExist)]
# source_dt[f.doesExist == False, :]

# add missing columns
if 'median_cov' not in experiments_dt.names:
    experiments_dt['median_cov'] = experiments_dt[
        :, as_type(None, dt.Type.float32)]


# subset identifiers that exist and reduce datasets
reduced_dt = source_dt[f.doesExist, {
    'experimentID': f.RDConnect_ID,
    'mean_cov': f['mean.coverage'],
    'median_cov': f['median.coverage']
}]

# map values over to main dataset
for row in reduced_dt.to_tuples():
    experiments_dt[
        f.experimentID == row[0], (f.mean_cov, f.median_cov, f.shouldImport)
    ] = (row[1], row[2], True)


# reduce data to records to import and import
import_dt = experiments_dt[f.shouldImport, :]

rd3.import_dt('solverd_labinfo', import_dt)

# ///////////////////////////////////////

# export files
source_dt['doesExist'] = source_dt[:, as_type(f.doesExist, dt.Type.str32)]
source_dt.to_csv('~/Desktop/novel_srwgs_sample_coverage.csv')
