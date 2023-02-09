#///////////////////////////////////////////////////////////////////////////////
# FILE: solverd_fix_experiment_releases.py
# AUTHOR: David Ruvolo
# CREATED: 2023-02-02
# MODIFIED: 2023-02-06
# PURPOSE: general fixes
# STATUS: stable
# PACKAGES: **see below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.utils.utils import dtFrameToRecords,flattenDataset
from rd3.api.molgenis import Molgenis
# from rd3.api.molgenis2 import Molgenis
from dotenv import load_dotenv
from datatable import dt, f
from os import environ
load_dotenv()

# connect to RD3
rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

rd3_acc = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3_acc.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# fix release associations in the labinfo table
# Experiments were assigned to the wrong release. This script will fix the
# association by comparing experiment IDs that are in the portal table

# get data current experiments and releases
freeze = rd3_prod.get(
  entity = 'solverd_labinfo',
  attributes = 'experimentID,partOfRelease',
  batch_size=10000
)

for row in freeze:
  row['partOfRelease'] = ','.join([ d['id'] for d in row['partOfRelease'] ])

freezeDT = dt.Frame(freeze)
del freezeDT['_href']

dt.unique(freezeDT['partOfRelease'])
freezeDT[:, dt.count(), dt.by(f.partOfRelease)]

# get raw data from the portal

freezeTwo = dt.Frame(
  rd3_prod.get(
    entity = 'rd3_portal_release_freeze2',
    attributes = 'labinfo_sample,subject_id',
    batch_size=10000
  )
)[:,['labinfo_sample','subject_id']]

freezeThree = dt.Frame(
  rd3_prod.get(
    entity = 'rd3_portal_release_freeze3',
    attributes='labinfo_sample,subject_id',
    batch_size=10000
  )
)[:,['labinfo_sample','subject_id']]


# pull experiment IDs
freezeTwoIDs = freezeTwo['labinfo_sample'].to_list()[0]
freezeThreeIDs = freezeThree['labinfo_sample'].to_list()[0]

# update affiliation
freezeDT['newReleases'] = dt.Frame([
  'freeze3_original'
  if (row[1] == 'freeze2_original') and (row[0] in freezeThreeIDs) and (row[0] not in freezeTwoIDs) else row[1]
  for row in freezeDT[:, [f.experimentID, f.partOfRelease]].to_tuples()
])

# check mappings
freezeDT
dt.unique(freezeDT['newReleases'])
freezeDT[:, dt.count(), dt.by(f.partOfRelease)]
freezeDT[:, dt.count(), dt.by(f.newReleases)]

del freezeDT['partOfRelease']
freezeDT.names = { 'newReleases': 'partOfRelease' }

# import
importData = dtFrameToRecords(freezeDT)

rd3_acc.updateColumn(
  entity = 'solverd_labinfo',
  attr = 'partOfRelease',
  data = importData
)

rd3_prod.updateColumn(
  entity = 'solverd_labinfo',
  attr = 'partOfRelease',
  data = importData
)

#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Update processed status in the portal

# get samples and extract subjectID
samples = rd3_acc.get(
  entity = 'solverd_samples',
  attributes='sampleID,belongsToSubject',
  batch_size=10000
)

for row in samples:
  if 'belongsToSubject' in row:
    row['belongsToSubject'] = row['belongsToSubject']['subjectID']

samplesDT = dt.Frame(samples)
del samplesDT['_href']

sampleIDs = samplesDT['sampleID'].to_list()[0]
subjectIDs = samplesDT['belongsToSubject'].to_list()[0]

# pull data from the staging table
shipmentDT = dt.Frame(
  rd3_acc.get(
    entity = 'rd3_portal_novelomics_shipment',
    q='processed==false'
  )
)

del shipmentDT['_href']

# check each sample- and participant- ID to make sure they exist in RD3
shipmentDT['sampleExists'] = dt.Frame([
  value in sampleIDs
  for value in shipmentDT['sample_id'].to_list()[0]
])

shipmentDT['subjectExists'] = dt.Frame([
  value in subjectIDs
  for value in shipmentDT['participant_subject'].to_list()[0]
])

# make sure all records exist. Otherwise, import the data
shipmentDT[:, dt.count(), dt.by(f.sampleExists)]
shipmentDT[:, dt.count(), dt.by(f.subjectExists)]

# update status accordingly
shipmentDT['processed'] = True

rd3_acc.importDatatableAsCsv('rd3_portal_novelomics_shipment',shipmentDT)
rd3_prod.importDatatableAsCsv('rd3_portal_novelomics_shipment',shipmentDT)

#///////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Change a release of a specific type

# ~ 3a ~
# Fix Subjects
rawsubjects = rd3_acc.get(
  entity='solverd_subjects',
  q='partOfRelease==novelrnaseq_original',
  attributes='subjectID,partOfRelease',
  batch_size=1000
)

subjects = flattenDataset(rawsubjects,columnPatterns='subjectID|id|value')
for row in subjects:
  row['partOfRelease'] = row['partOfRelease'].replace('novelrnaseq_','novelsrrnaseq_')
del row

rd3_acc.updateColumn('solverd_subjects', attr='partOfRelease', data=subjects)
rd3_prod.updateColumn('solverd_subjects', attr='partOfRelease', data=subjects)

rd3_acc.updateColumn('solverd_subjectinfo', attr='partOfRelease', data=subjects)
rd3_prod.updateColumn('solverd_subjectinfo', attr='partOfRelease', data=subjects)

# ~ 3b ~
# Fix samples
rawsamples = rd3_acc.get(
  entity='solverd_samples',
  q='partOfRelease==novelrnaseq_original',
  attributes='sampleID,partOfRelease',
  batch_size=1000
)

samples = flattenDataset(rawsamples,columnPatterns='subjectID|id|value')
for row in samples:
  row['partOfRelease'] = row['partOfRelease'].replace('novelrnaseq_','novelsrrnaseq_')

rd3_acc.updateColumn('solverd_samples',attr='partOfRelease',data=samples)
rd3_prod.updateColumn('solverd_samples',attr='partOfRelease',data=samples)

#///////////////////////////////////////////////////////////////////////////////

# ~ 999 ~
# Logout

rd3_acc.logout()
rd3_prod.logout()
