"""Solve-RD Summarise public datasets
FILE: solverd_public_datasets_summarise.py
AUTHOR: David Ruvolo
CREATED: 2024-09-26
MODIFIED: 2024-09-26
PURPOSE: summarise public datasets using RD3 data
STATUS: in.progress
PACKAGES: **see below**
COMMENTS: NA
"""

from os import environ
from dotenv import load_dotenv
from datatable import dt, f
from rd3tools.molgenis import Molgenis
from rd3tools.utils import print2, flatten_data

load_dotenv()

# ///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Connect to RD3 and retrieve metadata

print2('Connecting to RD3 and retrieving data....')

rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# get existing registered datasets and build dataset query
datasets_dt = dt.Frame(
    rd3.get(
        entity='solverd_info_datasets',
        attributes='id,datasetType'
    )
)[:, (f.id, f.datasetType)]

dataset_query = ','.join([
    f"includedInDatasets=={dataset_id}"
    for dataset_id in datasets_dt['id'].to_list()[0]
])

# pull subjects, samples, and experiments that are associated with a public dataset
subjects_raw = rd3.get('solverd_subjects', q=dataset_query)
samples_raw = rd3.get('solverd_samples', q=dataset_query)
labinfo_raw = rd3.get('solverd_labinfo', q=dataset_query)

# flatten datasets
subjects_flat = flatten_data(subjects_raw, 'subjectID|id|value')
samples_flat = flatten_data(samples_raw, 'subjectID|id|value')
labinfo_flat = flatten_data(labinfo_raw, 'sampleID|id|value')

# convert to datatable objects
subjects_dt = dt.Frame(subjects_flat)
samples_dt = dt.Frame(samples_flat)
labinfo_dt = dt.Frame(labinfo_flat)

# ///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Summarise data by dataset

summary_dt = datasets_dt.copy()

# ~ 2a ~
# Summarise the number of patients


# calculate the number of patients by sex
patients_by_sex_dt = dt.Frame(
    subjects_dt[:, dt.count(), dt.by(f.includedInDatasets, f.sex1)]
    .to_pandas()
    .pivot(index='includedInDatasets', columns='sex1', values='count')
    .reset_index()
)

# get the total number of patients
patients_by_sex_dt['numberOfPatients'] = dt.Frame([
    sum(row) if all(row) else None
    for row in patients_by_sex_dt[:, (f.F, f.M, f.U)].to_tuples()
])

# change names
patients_by_sex_dt.names = {
    'includedInDatasets': 'id',
    'F': 'numberOfFemales',
    'M': 'numberOfMales',
    'U': 'numberOfUnknown'
}

# join data
patients_by_sex_dt.key = 'id'
summary_dt = summary_dt[:, :, dt.join(patients_by_sex_dt)]

# ///////////////////////////////////////

# ~ 2b ~
# Summarise datasets by ERNs

subjects_dt[:, dt.count(), dt.by(f.includedInDatasets, f.ERN)]
