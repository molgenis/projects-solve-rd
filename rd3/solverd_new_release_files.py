"""Solve-RD New Experiment Releases
FILE: solverd_novelomics_new_experiments.py
AUTHOR: David Ruvolo
CREATED: 2023-02-07
MODIFIED: 2024-05-07
PURPOSE: process and import new experiment metadata
STATUS: stable
PACKAGES: **see below**
COMMENTS: NA
"""

from os import environ
import numpy as np
from dotenv import load_dotenv
from datatable import dt, f
from rd3tools.molgenis import Molgenis
from rd3tools.utils import print2, recode_value, flatten_data, timestamp, as_key_pairs
load_dotenv()


def dt_as_recordset(data: None):
    """Datatable to recordset"""
    if 'to_pandas' not in dir(data):
        raise AttributeError('Data is not a datatable frame')
    return data.to_pandas().replace({np.nan: None}).to_dict('records')


# set current release
# currentRelease = 'freeze3_original'

# ///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Retrieve metadata from RD3
print2('Connecting to RD3 and retrieving data...')

rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])


# retrieve subjects to validate incoming subjects and experiments
subjects_raw = rd3.get(
    entity='solverd_subjects',
    attributes='subjectID,partOfRelease,retracted',
    batch_size=1000
)
subjects = flatten_data(subjects_raw, 'subjectID|id|value')
subjects_dt = dt.Frame(subjects)
subjects_dt = subjects_dt[f.retracted != 'Y', :]


# retrieve samples to identify new samples
samples_raw = rd3.get(
    entity='solverd_samples',
    attributes='sampleID,belongsToSubject,partOfRelease',
    batch_size=1000
)
samples = flatten_data(samples_raw, 'subjectID|id')
samples_dt = dt.Frame(samples)


# retrieve experiments to identify new experiments
experiments_raw = rd3.get(
    entity='solverd_labinfo',
    attributes='experimentID,sampleID,partOfRelease',
    batch_size=1000
)

experiments = flatten_data(experiments_raw, 'sampleID|id')
experiments_dt = dt.Frame(experiments)


# retrieve new experiment manifest data
manifest_raw = rd3.get(
    entity='rd3_portal_release_experiments',
    q='processed==false;date_created=ge=2024-04-01T00:00:00%2B0200',
    batch_size=10000,
    num=5000
)

manifest_dt = dt.Frame(manifest_raw)
portal_dt = manifest_dt.copy()
del portal_dt['_href']

# isoloate identifiers
subject_ids = subjects_dt['subjectID'].to_list()[0]
sample_ids = samples_dt['sampleID'].to_list()[0]
experiment_ids = experiments_dt['experimentID'].to_list()[0]

# ///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Process new manifest data

# ~ 1a ~
# Recode columns
# Make sure values are real and do not have spaces
for column in ['library_strategy', 'library_layout', 'library_source']:
    if column in portal_dt.names:
        portal_dt[column] = dt.Frame([
            value if bool(value) and value != ' ' else None
            for value in portal_dt[column].to_list()[0]
        ])


# blanket recode 'unknown'
for column in portal_dt.names:
    portal_dt[column] = dt.Frame([
        None if value == 'unknown' else value
        for value in portal_dt[column].to_list()[0]
    ])


# ///////////////////////////////////////

# ~ 1b ~
# prepare seqTypes mappings

# get current seqtype reference values and convert to an object
seqtype_dt = dt.Frame(rd3.get('solverd_lookups_seqType'))
seqtype_mappings = as_key_pairs(dt_as_recordset(seqtype_dt), 'id', 'id')

# get unique seqtype values from new data to identify new values
if 'library_strategy' in portal_dt.names:
    new_seqtypes = dt.unique(
        portal_dt[f.library_strategy != None, f.library_strategy]
    ).to_list()[0]

    for value in new_seqtypes:
        if value not in seqtype_mappings:
            print2(f"Value '{value}' not in SeqTypes mapping dataset")

# update mappings and rerun until there are no more errors
# add variations here. If there are new values, add them to the lookup table
seqtype_mappings.update({
    'Bisulfite-Seq': 'RRBS'
})


# ///////////////////////////////////////

# ~ 1c ~
# Recode File Formats

file_formats = dt.Frame(rd3.get('solverd_lookups_typeFile'))
del file_formats['_href']

file_format_mappings = as_key_pairs(
    data=dt_as_recordset(file_formats),
    key_attr='id',
    value_attr='id'
)

new_file_formats = dt.unique(portal_dt['file_type']).to_list()[0]
for value in new_file_formats:
    if value not in file_format_mappings:
        print2(f'Value "{value}" not in file format mappings')

# if there are unknown/new file formats, update mappings and import into RD3
# file_format_mappings.update({})

# ///////////////////////////////////////

# ~ 1b ~
# Recode Experiment Data
# In this step, we will recode experiment data into RD3 terminology and clean
# the data for processing.

# ~ 1b.i ~
# make sure all P numbers are uppercase (we've had some lowercase values in the past)
portal_dt['subject_id'] = dt.Frame([
    value.strip().upper()
    for value in portal_dt['subject_id'].to_list()[0]
])

# ~ 1b.ii ~
# set release
rd3_releases = dt.Frame(
    rd3.get('solverd_info_datareleases', attributes='id')
)['id']

release_mappings = as_key_pairs(dt_as_recordset(rd3_releases), 'id', 'id')

new_release_values = dt.unique(portal_dt['project_batch_id']).to_list()[0]
for value in new_release_values:
    if value not in release_mappings:
        print2(f'Value "{value}" not a recognized release')

# update releaes accordingly
release_mappings.update({
    'DF3': 'freeze3_original'
})

# ///////////////////////////////////////

# manifestDT['partOfRelease'] = currentRelease

# ~ 1b.iii ~
# recode library layout
if 'library_layout' in portal_dt.names:
    portal_dt['library'] = dt.Frame([
        '1' if value == 'PAIRED' else (
            '2' if value == 'SINGLE' else value
        )
        for value in portal_dt['library_layout'].to_list()[0]
    ])
else:
    print2('Column "library_layout" not found. Initializing empty column...')
    portal_dt['library'] = None

# ~ 1b.iv ~
# library_source to title case
if 'library_source' in portal_dt.names:
    portal_dt['library_source'] = dt.Frame([
        value.title() if bool(value) else value
        for value in portal_dt['library_source'].to_list()[0]
    ])
else:
    print2('Column "library_source" not found. Initializing empty column...')
    portal_dt['library_source'] = None

# ~ 1b.v ~
# recode library strategy
if 'library_strategy' in portal_dt.names:
    portal_dt['library_strategy'] = dt.Frame([
        recode_value(
            mappings=seqtype_mappings,
            value=value,
            label='SeqTypes/Library Strategy'
        )
        if value else value
        for value in portal_dt['library_strategy'].to_list()[0]
    ])
else:
    print2('Column "library_strategy" not found. Initializing empty column...')
    portal_dt['library_strategy'] = None

# ~ 1b.vi ~
# create full path column (concat path and name)
portal_dt['name'] = dt.Frame([
    '/'.join(list(filter(None, row)))
    for row in portal_dt[:, (f.file_path, f.file_name)].to_tuples()
])

# ///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Prepare Datasets
# Here we will separate the experiment data and the file metadata. We will also
# check for new subjects, samples, and experiments. If there are any unknown
# records, these cases need to be manually resolved. Lastly, we will identify
# which subjects are new and validate existing records.

# ~ 2a ~
# Create flags for new subjects, samples, and experiments

# are there new subjects?
portal_dt['isNewSubject'] = dt.Frame([
    id not in subject_ids
    for id in portal_dt['subject_id'].to_list()[0]
])

# are there new samples?
portal_dt['isNewSample'] = dt.Frame([
    id not in sample_ids
    for id in portal_dt['sample_id'].to_list()[0]
])

# are there new experiments?
portal_dt['isNewExperiment'] = dt.Frame([
    id not in experiment_ids
    for id in portal_dt['project_experiment_dataset_id'].to_list()[0]
])

# portal_dt[:, dt.count(), dt.by(f.isNewSubject)]
# portal_dt[:, dt.count(), dt.by(f.isNewSample)]
# portal_dt[:, dt.count(), dt.by(f.isNewExperiment)]

# ///////////////////////////////////////

# ~ 2b ~
# Create labinfo table
labinfo_dt = portal_dt[
    :, dt.first(f[:]), dt.by(f.project_experiment_dataset_id)
][:, {
  'experimentID': f.project_experiment_dataset_id,
  'sampleID': f.sample_id,
  'subjectID': f.subject_id,
  'capture': f.library_selection,
  'libraryType': f.library_source,
  'library': f.library,
  'sequencingCenter': f.sequencing_center,
  'sequencer': f.platform_model,
  'seqType': f.library_strategy,
  'partOfRelease': f.partOfRelease,
  'isNewSubject': f.isNewSubject,
  'isNewSample': f.isNewSample,
  'isNewExperiment': f.isNewExperiment
  }]

# create files table
files_dt = portal_dt[:, dt.first(f[:]), dt.by(f.name)][:, {
    'EGA': f.file_ega_id,
    'name': f.name,
    'md5': f.unencrypted_md5_checksum,
    'fileFormat': f.file_type,
    'subjectID': f.subject_id,
    'sampleID': f.sample_id,
    'experimentID': f.project_experiment_dataset_id,
    'partOfRelease': f.partOfRelease,
    'isNewSubject': f.isNewSubject,
    'isNewSample': f.isNewSample,
    'isNewExperiment': f.isNewExperiment
}]

# Check counts
# Theoretically, there should be no new subjects and samples in this dataset.
# However, you may run into the situation where there are unknown records. If
# this happens, check the novelomics shipment staging table to see if all
# records where run (i.e., processed == false). If there are any records where
# processed is false, then run the novelomics subject-sample mapping script.
# You may also want to check to see if there are any samples that are marked
# processed, but aren't in RD3. Run the 'optional' step at the top of the script
# to find missing cases.
#
# If there are cases where `isNewExperiment` is false, then you will need
# to validate those cases.
# labinfo_dt[
#     :, dt.count(), dt.by(
#         f.isNewSubject, f.isNewSample, f.isNewExperiment
#     )
# ]

# files_dt[:, dt.count(), dt.by(f.isNewSubject, f.isNewSample, f.isNewExperiment)]

# ///////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Create and import new metadata
# It is apparent that there are duplicate records due to multiple re-imports
# or other import issues. It is better to identify new records, and import them
# accordingly.

# ~ 3a ~
# Create experiments and files datasets
labinfo_dt = labinfo_dt[f.isNewExperiment, :]
labinfo_dt['dateRecordCreated'] = timestamp()
labinfo_dt['recordCreatedBy'] = 'rd3-bot'


# filesDT = filesDT[(f.EGA != None), :]
files_dt['dateRecordCreated'] = timestamp()
files_dt['recordCreatedBy'] = 'rd3-bot'

# optional: remove IDs that aren't known
files_dt['subjectID'] = dt.Frame([
    None if row[1] is True else row[0]
    for row in files_dt[:, (f.subjectID, f.isNewSubject)].to_tuples()
])

files_dt['sampleID'] = dt.Frame([
    None if row[1] is True else row[0]
    for row in files_dt[:, (f.sampleID, f.isNewSample)].to_tuples()
])

files_dt['experimentID'] = dt.Frame([
    None if row[1] is True else row[0]
    for row in files_dt[:, (f.experimentID, f.isNewExperiment)].to_tuples()
])

# ~ 3b ~
# Update Portal Status
file_ids = files_dt[f.EGA != None, 'EGA'].to_list()[0]
portal_dt['processed'] = dt.Frame([
    value in file_ids
    for value in portal_dt['file_ega_id'].to_list()[0]
])

# portal_dt[:, dt.count(), dt.by(f.processed)]

# ///////////////////////////////////////////////////////////////////////////////

# ~ 4 ~
# Import

rd3.import_dt('solverd_labinfo', labinfo_dt)
rd3.import_dt('solverd_files', files_dt)
rd3.import_dt('rd3_portal_novelomics_experiment', portal_dt)

rd3.logout()
