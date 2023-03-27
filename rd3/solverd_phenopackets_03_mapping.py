#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_data_phenopacket_processing.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-08-02
#' MODIFIED: 2023-03-27
#' PURPOSE: process new phenopacket data
#' STATUS: stable
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from rd3.utils.utils import timestamp, flattenDataset
from datatable import dt, f, as_type
from dotenv import load_dotenv
from os import environ
from tqdm import tqdm
load_dotenv()

def uniqueStringValues(value, separator=','):
  """Split a comma-string and return unique values"""
  values = value.split(separator)
  return ','.join(list(set(values)))


rd3_acc = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3_acc.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Retrieve and prepare datasets

# ~ 1a ~
# Get input datasets

# get subjects
rawsubjects = rd3_prod.get('solverd_subjects', batch_size=1000)
# rawsubjects = rd3_acc.get('solverd_subjects', batch_size=1000)
subjects= flattenDataset(rawsubjects, columnPatterns='subjectID|id|value')
subjectsDT = dt.Frame(subjects)
subjectsDT[:, dt.update(clinical_status=as_type(f.clinical_status, 'str'))]
subjectsIDs = subjectsDT['subjectID'].to_list()[0]


# get subjectinfo
rawsubjectinfo = rd3_prod.get('solverd_subjectinfo', batch_size=1000)
# rawsubjectinfo = rd3_acc.get('solverd_subjectinfo', batch_size=1000)
subjectinfo = flattenDataset(rawsubjectinfo, columnPatterns='subjectID|id|value')
subjectinfoDT = dt.Frame(subjectinfo)
subjectinfoDT[:, dt.update(dateOfBirth=as_type(f.dateOfBirth,str))]


# get new phenopackets
rawPhenopacketsDT = dt.Frame(rd3_prod.get('rd3_portal_cluster_phenopacket',batch_size=1000))
phenopacketsDT = rawPhenopacketsDT.copy()
phenopacketsDT.names = {
  'dateofBirth': 'dateOfBirth',
  'phenopacketsID': 'mostRecentPhenopacketFile',
  # 'releasesWhereSubjectExists': 'partOfRelease'
}

del phenopacketsDT[:, ['clusterRelease', 'releasesWhereSubjectExists']]
del phenopacketsDT['_href']


# ~ 1b ~
# Make sure all subjects exist
# Note subjects that do not exist in RD3 and remove them from the import
phenopacketsDT['subjectExists'] = dt.Frame([
  id in subjectsIDs for id in phenopacketsDT['subjectID'].to_list()[0]
])


# get counts, create a subset of missing subjects for later use
phenopacketsDT[:, dt.count(), dt.by(f.subjectExists)]
unknownSubjects = phenopacketsDT[f.subjectExists==False,:]
unknownSubjects.to_csv(f"data/rd3_unknown_subjects_{timestamp()}.csv")

# remove unknown subjects for now
phenopacketsDT = phenopacketsDT[f.subjectExists, :]


# make sure dateOfBirth is renamed and formatted correctly
phenopacketsDT['dateOfBirth'] = dt.Frame([
  value.replace('.0','') if value else value
  for value in phenopacketsDT['dateOfBirth'].to_list()[0]
])

# check for duplicate rows
phenopacketsDT.nrows
dt.unique(phenopacketsDT['subjectID']).nrows == phenopacketsDT.nrows


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

# find unique phenopackets releases
# if there is more than one release
# filesReleaseDates = phenopacketsDT[:, f.mostRecentPhenopacketFile]
# filesReleaseDates['dates'] = dt.Frame([
#   value.split('.')[1]
#   for value in filesReleaseDates['mostRecentPhenopacketFile'].to_list()[0]
# ])

# dt.unique(filesReleaseDates['dates'])
# filesReleaseDates[:, dt.count(f.mostRecentPhenopacketFile), dt.by(f.dates)]

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

phenopacketIDs = phenopacketsDT['subjectID'].to_list()[0]

for id in tqdm(phenopacketIDs):
  newData = phenopacketsDT[f.subjectID == id, :]
  
  subjectsDT[f.subjectID==id, 'sex1'] = newData['sex1'].to_list()[0]
  subjectsDT[f.subjectID==id, 'phenotype'] = newData['phenotype'].to_list()[0]
  subjectsDT[f.subjectID==id, 'hasNotPhenotype'] = newData['hasNotPhenotype'].to_list()[0]
  subjectsDT[f.subjectID==id, 'disease'] = newData['disease'].to_list()[0]
  subjectsDT[
    f.subjectID==id,
    'mostRecentPhenopacketFile'
  ] = newData['mostRecentPhenopacketFile'].to_list()[0]
  
  if 'dateOfBirth' in newData.names:
    if newData['dateOfBirth'].to_list()[0] != [None]:
      subjectinfoDT[f.subjectID==id, 'dateOfBirth'] = newData['dateOfBirth'].to_list()[0]
  
  if 'ageOfOnset' in newData.names:
    if newData['ageOfOnset'].to_list()[0] != [None]:
      subjectinfoDT[f.subjectID==id, 'ageOfOnset'] = newData['ageOfOnset'].to_list()[0]

  subjectsDT[f.subjectID==id, 'dateRecordUpdated'] = timestamp()
  subjectsDT[f.subjectID==id, 'wasUpdatedBy'] = 'rd3-bot'
  subjectinfoDT[f.subjectID==id, 'dateRecordUpdated'] = timestamp()
  subjectinfoDT[f.subjectID==id, 'wasUpdatedBy'] = 'rd3-bot'

#///////////////////////////////////////

# ~ 2 ~
# Update processed status and import datasets

# update processed data
rawPhenopacketsDT['processed'] = 'true'
if unknownSubjects:
  for id in unknownSubjects['mostRecentPhenopacketFile'].to_list()[0]:
    rawPhenopacketsDT[f.phenopacketsID==id,'processed'] = 'false'
# rawPhenopacketsDT[:, dt.count(),dt.by(f.processed)]

# import
rd3_acc.importDatatableAsCsv('solverd_subjects', subjectsDT)
rd3_acc.importDatatableAsCsv('solverd_subjectinfo', subjectinfoDT)
rd3_acc.importDatatableAsCsv('rd3_portal_cluster_phenopacket', rawPhenopacketsDT)


rd3_prod.importDatatableAsCsv('solverd_subjects', subjectsDT)
rd3_prod.importDatatableAsCsv('solverd_subjectinfo', subjectinfoDT)
rd3_prod.importDatatableAsCsv('rd3_portal_cluster_phenopacket', rawPhenopacketsDT)

# logout
rd3_acc.logout()
rd3_prod.logout()
