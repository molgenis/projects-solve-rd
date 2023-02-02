#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_data_phenopacket_processing.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-08-02
#' MODIFIED: 2023-01-25
#' PURPOSE: process new phenopacket data
#' STATUS: stable
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from rd3.utils.utils import timestamp, dtFrameToRecords
from rd3.api.molgenis import Molgenis
from dotenv import load_dotenv
from datatable import dt, f
from os import environ
load_dotenv()

def uniqueStringValues(value, separator=','):
  """Split a comma-string and return unique values"""
  values = value.split(separator)
  return ','.join(list(set(values)))

#///////////////////////////////////////

# ~ 0 ~
# Get Data

# Connect to RD3: acc and prod
# By this point, the phenopacket staging table on the ACC and PROD databases
# are updated. We can, theoretically, update both databases in one go. 

rd3_acc = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3_acc.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# ~ 0a ~ 
# Retrieve phenopacket data
phenopacketsDT = dt.Frame(
  rd3_prod.get(
    entity = 'rd3_portal_cluster_phenopacket',
    batch_size = 10000
  )
)

phenopacketsDT.names = {
  'dateofBirth': 'dateOfBirth',
  'phenopacketsID': 'mostRecentPhenopacketFile'
}

del phenopacketsDT[:, ['clusterRelease', 'releasesWhereSubjectExists']]
del phenopacketsDT['_href']

# ~ 0b ~
# Retrieve subject IDs
# until all subjects have been added to RD3 run this
subjectsIDs = dt.Frame(
  rd3_prod.get(
    entity = 'solverd_subjects',
    attributes = 'subjectID',
    batch_size=10000
  )
)['subjectID'].to_list()[0]

phenopacketsDT['subjectExists'] = dt.Frame([
  id in subjectsIDs
  for id in phenopacketsDT['subjectID'].to_list()[0]
])

phenopacketsDT[:, dt.count(), dt.by(f.subjectExists)]
phenopacketsDT = phenopacketsDT[f.subjectExists, :]


# rename releases column to RD3 terminology
# phenopacketsDT.names = {'releasesWhereSubjectExists': 'partOfRelease'}

# set id: append subjectIDs with '_original' as it's the primary key
# phenopacketsDT['id'] = dt.Frame([
#   f'{subjectID}_original'
#   for subjectID in phenopacketsDT['subjectID'].to_list()[0]
# ])

# make sure current freeze is in the patch
# phenopacketsDT['partOfRelease'] = dt.Frame([
#   f"{row[0]}, {row[1]}" if row[1] not in row[0] else row[0]
#   for row in phenopacketsDT[:, ['partOfRelease', 'clusterRelease']].to_tuples()
# ])

# run one more check to make sure releases are unique
# phenopacketsDT['partOfRelease'] = dt.Frame([
#   uniqueStringValues(value) if value else value
#   for value in phenopacketsDT['partOfRelease'].to_list()[0]
# ])

# make sure dateOfBirth is renamed and formatted correctly
phenopacketsDT['dateOfBirth'] = dt.Frame([
  value.replace('.0','') if value else value
  for value in phenopacketsDT['dateOfBirth'].to_list()[0]
])

# check for duplicate rows
phenopacketsDT.nrows
dt.unique(phenopacketsDT['subjectID']).nrows == phenopacketsDT.nrows


# find unique phenopackets releases
# if there is more than one release
filesReleaseDates = phenopacketsDT[:, f.mostRecentPhenopacketFile]
filesReleaseDates['dates'] = dt.Frame([
  value.split('.')[1]
  for value in filesReleaseDates['mostRecentPhenopacketFile'].to_list()[0]
])

dt.unique(filesReleaseDates['dates'])
filesReleaseDates[:, dt.count(f.mostRecentPhenopacketFile), dt.by(f.dates)]

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Prepare Objects for import
# New phenopackets data will be imported into the `solverd_subjects` and
# `solverd_subjectinfo` tables. The columns that will be updated are listed
# below.
#
# Subjects:
#   - sex: `sex1`
#   - observed phenotypes: `phenotype`
#   - unobserved phenotypes: `hasNotPhenotype`
#   - diseases: `disease`
#   - most recent phenopackets data: `phenopacketsID`
#   - associated SolveRD releases: `partOfRelease`
#
# SubjectInfo
#   - Date of Birth: `dateOfBirth`
#   - Age of onset: `ageOfOnset`
#   - associated SolveRD releases: `partOfRelease`
#


# ~ 1a ~
# Init Import Object
# To make the import a bit easier, we will create an object that we can use to
# loop over when it is time to build and import the data.

datasetsToImport = {
  # subjects
  'sex1': None,
  'phenotype': None,
  'hasNotPhenotype': None,
  'disease': None,
  'mostRecentPhenopacketFile': None,
  # subject info
  'dateOfBirth': None,
  'ageOfOnset': None,
  # 'partOfRelease': None
}

# create datasets
for column in datasetsToImport:
  if column in phenopacketsDT.names:
    datasetsToImport[column] = phenopacketsDT[f[column] != None, (f.subjectID, f[column])]

# manually create columns
datasetsToImport['dateRecordUpdated'] = phenopacketsDT[
  :,
  {
    'subjectID': f.subjectID,
    'dateRecordUpdated': timestamp()
  }
]

datasetsToImport['wasUpdatedBy'] = phenopacketsDT[
  :,
  {
    'subjectID': f.subjectID,
    'wasUpdatedBy': 'rd3-bot'
  }
]

# check
datasetsToImport['sex1']
datasetsToImport['phenotype']
datasetsToImport['hasNotPhenotype']
datasetsToImport['disease']
datasetsToImport['dateOfBirth']
datasetsToImport['ageOfOnset']
datasetsToImport['mostRecentPhenopacketFile']

#///////////////////////////////////////

# ~ 1b ~
# Import data into ACC AND PROD
# You can automate this, but it's best to import each dataset one-by-one in case
# there are any errors.

# Import sex1
# rd3_acc.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'sex1',
#   data = dtFrameToRecords(datasetsToImport['sex1'])
# )

# rd3_prod.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'sex1',
#   data = dtFrameToRecords(datasetsToImport['sex1'])
# )

# Import phenotype
# rd3_acc.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'phenotype',
#   data = dtFrameToRecords(datasetsToImport['phenotype'])
# )

# rd3_prod.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'phenotype',
#   data = dtFrameToRecords(datasetsToImport['phenotype'])
# )

# Import hasNotPhenotype
# rd3_acc.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'hasNotPhenotype',
#   data = dtFrameToRecords(datasetsToImport['hasNotPhenotype'])
# )

# rd3_prod.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'hasNotPhenotype',
#   data = dtFrameToRecords(datasetsToImport['hasNotPhenotype'])
# )

# Import disease
# rd3_acc.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'disease',
#   data = dtFrameToRecords(datasetsToImport['disease'])
# )

# rd3_prod.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'disease',
#   data = dtFrameToRecords(datasetsToImport['disease'])
# )

# Import dateOfBirth
# rd3_acc.updateColumn(
#   entity = 'solverd_subjectinfo',
#   attr = 'dateOfBirth',
#   data = dtFrameToRecords(datasetsToImport['dateOfBirth'])
# )

# rd3_prod.updateColumn(
#   entity = 'solverd_subjectinfo',
#   attr = 'dateOfBirth',
#   data = dtFrameToRecords(datasetsToImport['dateOfBirth'])
# )

# Import ageOfOnset (if available)
# rd3_acc.updateColumn(
#   entity = 'solverd_subjectinfo',
#   attr = 'ageOfOnset',
#   data = dtFrameToRecords(datasetsToImport['ageOfOnset'])
# )

# rd3_prod.updateColumn(
#   entity = 'solverd_subjectinfo',
#   attr = 'ageOfOnset',
#   data = dtFrameToRecords(datasetsToImport['ageOfOnset'])
# )

# import phenopacketsID
# rd3_acc.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'mostRecentPhenopacketFile',
#   data = dtFrameToRecords(datasetsToImport['mostRecentPhenopacketFile'])
# )

# rd3_prod.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'mostRecentPhenopacketFile',
#   data = dtFrameToRecords(datasetsToImport['mostRecentPhenopacketFile'])
# )

# import dateRecordUpdated
# rd3_acc.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'dateRecordUpdated',
#   data = dtFrameToRecords(datasetsToImport['dateRecordUpdated'])
# )

# rd3_prod.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'dateRecordUpdated',
#   data = dtFrameToRecords(datasetsToImport['dateRecordUpdated'])
# )

# import wasUpdatedBy
# rd3_acc.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'wasUpdatedBy',
#   data = dtFrameToRecords(datasetsToImport['wasUpdatedBy'])
# )

# rd3_prod.updateColumn(
#   entity = 'solverd_subjects',
#   attr = 'wasUpdatedBy',
#   data = dtFrameToRecords(datasetsToImport['wasUpdatedBy'])
# )

# logout
rd3_acc.logout()
rd3_prod.logout()
