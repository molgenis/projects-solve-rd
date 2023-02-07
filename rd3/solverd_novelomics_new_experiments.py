#///////////////////////////////////////////////////////////////////////////////
# FILE: solverd_novelomics_new_experiments.py
# AUTHOR: David Ruvolo
# CREATED: 2023-02-07
# MODIFIED: 2023-02-07
# PURPOSE: process and import new experiment metadata
# STATUS: in.progress
# PACKAGES: **see below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from datatable import dt, f, first, as_type
from dotenv import load_dotenv
from os import environ
from tqdm import tqdm
import functools
import operator
import re

from rd3.api.molgenis2 import Molgenis
from rd3.utils.utils import (
  dtFrameToRecords,
  timestamp,
  toKeyPairs,
  recodeValue,
  flattenDataset
)

# init connection to RD3
load_dotenv()
rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

rd3_acc = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3_acc.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

#///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~ 
# Retrieve data from RD3

# get subjects
rawsubjects = rd3_prod.get(
  entity='solverd_subjects',
  attributes='subjectID,partOfRelease',
  batch_size=10000
)
subjects = flattenDataset(rawsubjects, columnPatterns="id")
subjectsDT = dt.Frame(subjects)

# get samples
rawsamples = rd3_prod.get(
  entity='solverd_samples',
  attributes='sampleID,belongsToSubject,partOfRelease',
  batch_size=10000
)
samples = flattenDataset(rawsamples, columnPatterns="subjectID|id")
samplesDT = dt.Frame(samples)

# get experiments
rawexperiments = rd3_prod.get(
  entity='solverd_labinfo',
  attributes='experimentID,sampleID,partOfRelease',
  batch_size=10000
)
experiments = flattenDataset(rawexperiments, columnPatterns="sampleID|id")
experimentsDT = dt.Frame(experiments)

# get new experiment metadata
experimentDT = dt.Frame(
  rd3_prod.get(
    entity='rd3_portal_novelomics_experiment',
    q="processed==false"
  )
)

del experimentDT['_href']

# extract identifiers
subjectIDs = subjectsDT['subjectID'].to_list()[0]
sampleIDs = samplesDT['sampleID'].to_list()[0]
experimentIDs = experimentsDT['experimentID'].to_list()[0]

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Process New Shipment metadata

# ~ 1a ~
# Detect new releases
# Use the column `type_of_analysis` to determine if the incoming data is part
# of a new or existing release. As of now, all shipment mantifests will
# contain new "releases" or updates to an existing one. It isn't necessary to 
# indicate patches for novel omics releases when importing new sample data.
releaseinfo = dt.Frame(
  rd3_prod.get(
    entity='solverd_info_datareleases',
    attributes = 'id'
  )
)['id']

releases = dt.unique(experimentDT['experiment_type'])[:, {
  'id': f.experiment_type,
  'type': 'freeze',
  'name': f.experiment_type,
  'date': timestamp()
}]

# format IDs
releases['id'] = dt.Frame([
  f"novel{value.strip().lower().replace('-','')}_original"
  for value in releases['id'].to_list()[0]
])

releases['isNewRelease'] = dt.Frame([
  id not in releaseinfo['id'].to_list()[0]
  for id in releases['id'].to_list()[0]
])

releases['typeOfAnalysis'] = dt.Frame([
  value.lower().replace('-','')
  for value in releases['name'].to_list()[0]
])

releases['name'] = dt.Frame([
  f"Novel Omics {name}"
  for name in releases['name'].to_list()[0]
])

releaseIDs = toKeyPairs(
  data = dtFrameToRecords(data = releases),
  keyAttr = 'typeOfAnalysis',
  valueAttr = 'id'
)
 
# If there are new releases, import them 
if releases[f.isNewRelease, :].nrows > 0:
  print('There are new releases to import!!!!')
  # newReleases = releases[f.isNewRelease, (f.id, f.name, f.date)]
  # newReleases['createdBy'] = 'rd3-bot'
  # rd3_acc.importDatatableAsCsv('solverd_info_datareleases', newReleases)
  # rd3_prod.importDatatableAsCsv('solverd_info_datareleases', newReleases)

#///////////////////////////////////////

# ~ 1b ~
# Recode Experiment Data
# In this step, we will recode experiment data into RD3 terminology and clean
# the data for processing.

# ~ 1b.i ~
# make sure all P numbers are uppercase (we've had some lowercase values in the past)
experimentDT['subject_id'] = dt.Frame([
  value.strip().upper() for value in experimentDT['subject_id'].to_list()[0]
])

# ~ 1b.ii ~
# transform incoming analysis so that it can be mapped to a release
experimentDT['experiment_type'] = dt.Frame([
  value.strip().lower().replace('-','')
  for value in experimentDT['experiment_type'].to_list()[0]
])

experimentDT['partOfRelease'] = dt.Frame([
  recodeValue(mappings = releaseIDs, value = value, label = 'Release')
  for value in experimentDT['experiment_type'].to_list()[0]
])

# ~ 1b.iii ~
# recode library layout
experimentDT['library'] = dt.Frame([
  '1' if d == 'PAIRED' else d
  for d in experimentDT['library_layout'].to_list()[0]
])

# ~ 1b.iv ~
# library_source to title case
experimentDT['library_source'] = dt.Frame([
  d.title() for d in experimentDT['library_source'].to_list()[0]
])

# ~ 1b.v ~
# create full path column (concat path and name)
experimentDT['name'] = dt.Frame([
  '/'.join(d) for d in experimentDT[:, (f.file_path,f.file_name)].to_tuples()
])

#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Prepare Datasets
# Here we will separate the experiment data and the file metadata. We will also
# check for new subjects, samples, and experiments. If there are any unknown
# records, these cases need to be manually resolved. Lastly, we will identify
# which subjects are new and validate existing records.

# Create labinfo table
labinfoDT = experimentDT[
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
  'partOfRelease': f.partOfRelease
}]

# create files table
files = experimentDT[:, dt.first(f[:]), dt.by(f.name)][:, {
  'EGA': f.file_ega_id,
  'name': f.name,
  'md5': f.unencrypted_md5_checksum,
  'fileFormat': f.file_type,
  'subjectID': f.subject_id,
  'sampleID': f.sample_id,
  'experimentID': f.project_experiment_dataset_id,
  'partOfRelease': f.partOfRelease,
}]

# files[:, dt.count(), dt.by(f.EGA)][:, :, dt.sort(f.count)]

# are there new subjects? I would expect that all subjects exist in RD3
labinfoDT['isNewSubject'] = dt.Frame([
  id not in subjectIDs
  for id in labinfoDT['subjectID'].to_list()[0]
])

# are there new samples? I would expect that all samples exist in RD3
labinfoDT['isNewSample'] = dt.Frame([
  id not in sampleIDs
  for id in labinfoDT['sampleID'].to_list()[0]
])

# are there new experiments? (These should all be true)
labinfoDT['isNewExperiment'] = dt.Frame([
  id not in experimentIDs
  for id in labinfoDT['experimentID'].to_list()[0]
])

labinfoDT[:, dt.count(), dt.by(f.isNewSubject, f.isNewSample, f.isNewExperiment)]

labinfoDT[f.isNewSubject, :]