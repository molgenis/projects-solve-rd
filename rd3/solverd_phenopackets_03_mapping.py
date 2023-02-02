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

from rd3.api.molgenis2 import Molgenis
from rd3.utils.utils import timestamp
from datatable import dt, f, as_type
from dotenv import load_dotenv
from os import environ
from tqdm import tqdm
load_dotenv()

def uniqueStringValues(value, separator=','):
  """Split a comma-string and return unique values"""
  values = value.split(separator)
  return ','.join(list(set(values)))

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Get Data

# Connect to RD3: acc and prod
# By this point, the phenopacket staging table on the ACC and PROD databases
# are updated. We can, theoretically, update both databases in one go. 

rd3_acc = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3_acc.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])


# ~ 1a ~
# Retrieve subject IDs

subjects = rd3_prod.get('solverd_subjects', batch_size=10000)

# collapse everything
for row in subjects:
  del row['_href']
  del row['includedInStudies']
  del row['includedInCohorts']
  del row['includedInDatasets'] 
  del row['variant']
  if row.get('sex1'):
    row['sex1'] = row['sex1']['id']
  
  if row.get('mid'):
    row['mid'] = row['mid']['subjectID']
  
  if row.get('pid'):
    row['pid'] = row['pid']['subjectID']
  
  if bool(row.get('disease')):
    row['disease'] = ','.join([ d['id'] for d in row['disease'] ])
  else:
    row['disease'] = None
    
  if bool(row.get('phenotype')):
    row['phenotype'] = ','.join([ d['id'] for d in row['phenotype'] ])
  else:
    row['phenotype'] = None
  
  if bool(row.get('hasNotPhenotype')):
    row['hasNotPhenotype'] = ','.join([ d['id'] for d in row['hasNotPhenotype'] ])
  else:
    row['hasNotPhenotype'] = None
  
  if row.get('partOfRelease'):
    row['partOfRelease'] = ','.join([ d['id'] for d in row['partOfRelease'] ])
    
  if bool(row.get('ERN')):
    row['ERN'] = ','.join([ d['id'] for d in row['ERN'] ])
  else:
    row['ERN'] = None
    
  if bool(row.get('organisation')):
    row['organisation'] = ','.join([ d['value'] for d in row['organisation'] ])
  else:
    row['organisation'] = None
  
  if row.get('recontact'):
    row['recontact'] = row['recontact']['id']
  
  if row.get('retracted'):
    row['retracted'] = row['retracted']['id']
  
  if row.get('recordCreatedBy'):
    row['recordCreatedBy'] = row['recordCreatedBy']['id']
    
  if row.get('wasUpdatedBy'):
    row['wasUpdatedBy'] = row['wasUpdatedBy']['id']


# convert to datatable object
subjectsDT = dt.Frame(subjects)
subjectsDT[:, dt.update(clinical_status=as_type(f.clinical_status, 'str'))]

# pull subjectIDs
subjectsIDs = subjectsDT['subjectID'].to_list()[0]

#///////////////////////////////////////

# ~ 1b ~
# Get Subject INfo data

subjectinfo = rd3_prod.get('solverd_subjectinfo', batch_size=10000)

for row in subjectinfo:
  del row['_href']
  
  if bool(row.get('ageOfOnset')):
    row['ageOfOnset'] = row['ageOfOnset']['id']
  
  if row.get('partOfRelease'):
    row['partOfRelease'] = ','.join([ d['id'] for d in row['partOfRelease'] ])

  if row.get('recordCreatedBy'):
    row['recordCreatedBy'] = row['recordCreatedBy']['id']
    
  if row.get('wasUpdatedBy'):
    row['wasUpdatedBy'] = row['wasUpdatedBy']['id']

subjectinfoDT = dt.Frame(subjectinfo)
subjectinfoDT[:, dt.update(dateOfBirth=as_type(f.dateOfBirth,str))]

#///////////////////////////////////////

# ~ 1c ~ 
# Retrieve phenopacket data
phenopacketsDT = dt.Frame(
  rd3_prod.get(
    entity = 'rd3_portal_cluster_phenopacket',
    batch_size = 10000
  )
)

phenopacketsDT.names = {
  'dateofBirth': 'dateOfBirth',
  'phenopacketsID': 'mostRecentPhenopacketFile',
  # 'releasesWhereSubjectExists': 'partOfRelease'
}

del phenopacketsDT[:, ['clusterRelease', 'releasesWhereSubjectExists']]
del phenopacketsDT['_href']


# make sure all subjects exist. Remove (temporarily) non-existent subjects
phenopacketsDT['subjectExists'] = dt.Frame([
  id in subjectsIDs for id in phenopacketsDT['subjectID'].to_list()[0]
])
phenopacketsDT[:, dt.count(), dt.by(f.subjectExists)]
phenopacketsDT = phenopacketsDT[f.subjectExists, :]

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


# import
rd3_acc.importDatatableAsCsv('solverd_subjects', subjectsDT)
rd3_acc.importDatatableAsCsv('solverd_subjectinfo', subjectinfoDT)

rd3_prod.importDatatableAsCsv('solverd_subjects', subjectsDT)
rd3_prod.importDatatableAsCsv('solverd_subjectinfo', subjectinfoDT)

# logout
rd3_acc.logout()
rd3_prod.logout()
