#///////////////////////////////////////////////////////////////////////////////
# FILE: solverd_novelomics_new_experiments.py
# AUTHOR: David Ruvolo
# CREATED: 2023-02-07
# MODIFIED: 2023-04-05
# PURPOSE: process and import new experiment metadata
# STATUS: in.progress
# PACKAGES: **see below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from dotenv import load_dotenv
from datatable import dt, f
from os import environ
# from tqdm import tqdm
# import functools
# import operator
# import re
from rd3.utils.utils import (
  dtFrameToRecords,
  timestamp,
  toKeyPairs,
  recodeValue,
  flattenDataset,
  timestamp
)

# init connection to RD3
load_dotenv()
rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# rd3_acc = Molgenis(environ['MOLGENIS_ACC_HOST'])
# rd3_acc.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

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
novelomicsraw = dt.Frame(
  rd3_prod.get(
    entity='rd3_portal_novelomics_experiment',
    q="processed==false",
    batch_size=10000
  )
)

novelomicsDT = novelomicsraw.copy()
del novelomicsDT['_href']

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

releases = dt.unique(novelomicsDT['experiment_type'])[:, {
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


# ~ 1b ~
# Define new library_strategy (seqType) mappings
seqTypes = dt.Frame(rd3_prod.get('solverd_lookups_seqType'))[:, (f.id, f.label)]

seqTypesMappings = toKeyPairs(
  data = dtFrameToRecords(seqTypes),
  keyAttr='id',
  valueAttr='id'
)

seqTypesMappings.update({'Bisulfite-Seq': 'RRBS'})

# check for new values
incomingSeqTypes = dt.unique(novelomicsDT['library_strategy'])
incomingSeqTypes['isNewMapping'] = dt.Frame([
  value not in seqTypesMappings
  for value in incomingSeqTypes['library_strategy'].to_list()[0]
])

if incomingSeqTypes[f.isNewMapping,:].nrows:
  print(f"New seqtypes found not in seqType mappings")


# import new seqTypes
# newSeqTypes = incomingSeqTypes[
#   f.isNewMapping, f[:].remove(f.isNewMapping)
# ][:, {'id': f.library_strategy, 'label': f.library_strategy}]
# rd3_acc.importDatatableAsCsv(pkg_entity='solverd_lookups_seqType', data=newSeqTypes)
# rd3_prod.importDatatableAsCsv(pkg_entity='solverd_lookups_seqType', data=newSeqTypes)


# ~ 1c ~
# Recode File Formats
fileFormats = dt.Frame(rd3_prod.get('solverd_lookups_typeFile'))
del fileFormats['_href']

incomingFileTypes = dt.unique(novelomicsDT['file_type'])
incomingFileTypes['isNewMapping'] = dt.Frame([
  value not in fileFormats['id'].to_list()[0]
  for value in incomingFileTypes['file_type'].to_list()[0]
])

if incomingFileTypes[f.isNewMapping,:].nrows:
  print('New file formats to import')

# newFileTypes = incomingFileTypes[f.isNewMapping,:]

#///////////////////////////////////////

# ~ 1b ~
# Recode Experiment Data
# In this step, we will recode experiment data into RD3 terminology and clean
# the data for processing.

# ~ 1b.i ~
# make sure all P numbers are uppercase (we've had some lowercase values in the past)
novelomicsDT['subject_id'] = dt.Frame([
  value.strip().upper() for value in novelomicsDT['subject_id'].to_list()[0]
])

# ~ 1b.ii ~
# transform incoming analysis so that it can be mapped to a release
novelomicsDT['experiment_type'] = dt.Frame([
  value.strip().lower().replace('-','')
  for value in novelomicsDT['experiment_type'].to_list()[0]
])

novelomicsDT['partOfRelease'] = dt.Frame([
  recodeValue(mappings = releaseIDs, value = value, label = 'Release')
  for value in novelomicsDT['experiment_type'].to_list()[0]
])

# ~ 1b.iii ~
# recode library layout
novelomicsDT['library'] = dt.Frame([
  '1' if d == 'PAIRED' else (
    '2' if d == 'SINGLE' else d
  )
  for d in novelomicsDT['library_layout'].to_list()[0]
])

# ~ 1b.iv ~
# library_source to title case
novelomicsDT['library_source'] = dt.Frame([
  d.title() for d in novelomicsDT['library_source'].to_list()[0]
])

# ~ 1b.v ~
# recode library strategy
novelomicsDT['library_strategy'] = dt.Frame([
  recodeValue(
    mappings = seqTypesMappings,
    value = value,
    label='SeqTypes/Library Strategy'
  )
  if value else value
  for value in novelomicsDT['library_strategy'].to_list()[0]
])

# ~ 1b.vi ~
# create full path column (concat path and name)
novelomicsDT['name'] = dt.Frame([
  '/'.join(list(filter(None, row)))
  for row in novelomicsDT[:, (f.file_path,f.file_name)].to_tuples()
])

#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Prepare Datasets
# Here we will separate the experiment data and the file metadata. We will also
# check for new subjects, samples, and experiments. If there are any unknown
# records, these cases need to be manually resolved. Lastly, we will identify
# which subjects are new and validate existing records.

# ~ 2a ~
# Create flags for new subjects, samples, and experiments

# are there new subjects?
novelomicsDT['isNewSubject'] = dt.Frame([
  id not in subjectIDs
  for id in novelomicsDT['subject_id'].to_list()[0]
])

# are there new samples?
novelomicsDT['isNewSample'] = dt.Frame([
  id not in sampleIDs
  for id in novelomicsDT['sample_id'].to_list()[0]
])

# are there new experiments?
novelomicsDT['isNewExperiment'] = dt.Frame([
  id not in experimentIDs
  for id in novelomicsDT['project_experiment_dataset_id'].to_list()[0]
])

# novelomicsDT[:, dt.count(), dt.by(f.isNewSubject)]
# novelomicsDT[:, dt.count(), dt.by(f.isNewSample)]
# novelomicsDT[:, dt.count(), dt.by(f.isNewExperiment)]

#///////////////////////////////////////

# ~ 2b ~
# Create labinfo table
labinfoDT = novelomicsDT[
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
filesDT = novelomicsDT[:, dt.first(f[:]), dt.by(f.name)][:, {
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
labinfoDT[:, dt.count(), dt.by(f.isNewSubject, f.isNewSample, f.isNewExperiment)]
filesDT[:, dt.count(), dt.by(f.isNewSubject, f.isNewSample, f.isNewExperiment)]

#///////////////////////////////////////////////////////////////////////////////

# ~ 3 ~ 
# Create and import new metadata
# It is apparent that there are duplicate records due to multiple re-imports
# or other import issues. It is better to identify new records, and import them
# accordingly.

# ~ 3a ~
# Create experiments and files datasets

newExperiments = labinfoDT[f.isNewExperiment, :]
newExperiments['dateRecordCreated'] = timestamp()
newExperiments['recordCreatedBy'] = 'rd3-bot'

newFiles = filesDT[(f.EGA != None), :]
newFiles['dateRecordCreated'] = timestamp()
newFiles['recordCreatedBy'] = 'rd3-bot'

# ~ 3b ~
# Update Portal Status
experimentIDs = labinfoDT['experimentID'].to_list()[0]
novelomicsraw['processed'] = dt.Frame([
  id in experimentIDs
  for id in novelomicsraw['project_experiment_dataset_id'].to_list()[0]
])

fileIDs = filesDT[f.EGA!=None, 'EGA'].to_list()[0]
novelomicsraw['processed'] = dt.Frame([
  value in fileIDs
  for value in novelomicsraw['file_ega_id'].to_list()[0]
])

novelomicsraw[:, dt.count(), dt.by(f.processed)]

# ~ 3b ~
# Import
rd3_prod.importDatatableAsCsv(pkg_entity='solverd_labinfo',data=newExperiments)
rd3_prod.importDatatableAsCsv(pkg_entity='solverd_files',data=newFiles)
rd3_prod.importDatatableAsCsv(
  pkg_entity='rd3_portal_novelomics_experiment',
  data = novelomicsraw
)

# rd3_acc.importDatatableAsCsv(pkg_entity='solverd_labinfo',data=newExperiments)
# rd3_acc.importDatatableAsCsv(pkg_entity='solverd_files',data=newFiles)

# logout
# rd3_acc.logout()
rd3_prod.logout()
