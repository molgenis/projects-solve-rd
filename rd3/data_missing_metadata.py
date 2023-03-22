#///////////////////////////////////////////////////////////////////////////////
# FILE: data_missing_metadata.py
# AUTHOR: David Ruvolo
# CREATED: 2023-03-22
# MODIFIED: 2023-03-22
# PURPOSE: find missing metadata and import
# STATUS: in.progress
# PACKAGES: **see below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from dotenv import load_dotenv
from datatable import dt, f
from os import environ
load_dotenv()

rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

#///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Retrieve metadata

# get portal data
allDT = dt.Frame(
  rd3.get(
    entity='rd3_portal_release_patches',
    batch_size=10000
  )
)

del allDT['_href']

# get subject info
subjectsDT = dt.Frame(
  rd3.get(
    entity='solverd_subjects',
    attributes='subjectID',
    batch_size=10000
  )
)['subjectID']

# get samples
samplesDT = dt.Frame(
  rd3.get(
    entity='solverd_samples',
    attributes='sampleID',
    batch_size=10000
  )
)['sampleID']

# get experiments
experimentsDT = dt.Frame(
  rd3.get(
    entity = 'solverd_labinfo',
    attributes='experimentID',
    batch_size=10000  
  )
)['experimentID']

# isolate IDs
subjectIDs = subjectsDT['subjectID'].to_list()[0]
sampleIDs = samplesDT['sampleID'].to_list()[0]
experimentIDs = experimentsDT['experimentID'].to_list()[0]

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Identify missing metadata

allDT['isNewSubject'] = dt.Frame([
  value not in subjectIDs
  for value in allDT['subject_id'].to_list()[0]
])

allDT['isNewSample'] = dt.Frame([
  (value not in sampleIDs) and (f'VS{value}' not in sampleIDs)
  for value in allDT['samples_id'].to_list()[0]
])

allDT['isNewExperiment'] = dt.Frame([
  value not in experimentIDs
  for value in allDT['samples_id'].to_list()[0]
])


allDT[:, dt.count(), dt.by(f.isNewSubject)]
allDT[:, dt.count(), dt.by(f.isNewSample)]
allDT[:, dt.count(), dt.by(f.isNewExperiment)]
allDT[:, dt.count(), dt.by(f.isNewSubject,f.isNewSample)]
allDT[:, dt.count(), dt.by(f.isNewSubject,f.isNewSample,f.isNewExperiment)]

allDT[(f.isNewSubject) | (f.isNewSample), :]
allDT[f.isNewSample, :]
allDT[f.isNewExperiment,:]

newDT = allDT[(f.isNewSubject) | (f.isNewSample) | (f.isNewExperiment), :]

rd3.importDatatableAsCsv(pkg_entity='rd3_portal_release_new', data=newDT)
