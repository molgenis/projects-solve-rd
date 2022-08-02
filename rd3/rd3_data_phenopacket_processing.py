#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_data_phenopacket_processing.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-08-02
#' MODIFIED: 2022-08-02
#' PURPOSE: process new phenopacket data
#' STATUS: in.progress
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis import Molgenis
from datatable import dt, f
from dotenv import load_dotenv
from os import environ
from rd3.utils.utils import statusMsg,dtFrameToRecords
import re

# connect to RD3
load_dotenv()
rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

# rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
# rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# Set release
# This is the Phenopacket release as indicated by the original file path
# (i.e., location on the cluster)
currentRelease = 'freeze2_original'

# get subject patch information
subjects = rd3.get(
  entity = 'rd3_overview',
  attributes='subjectID,patch',
  batch_size=10000
)

for row in subjects:
  del row['_href']
  if 'patch' in row:
    patches = []
    for patch in row['patch']:
      patches.append(patch['id'])
    row['patch'] = ','.join(patches)

subjects = dt.Frame(subjects)


#///////////////////////////////////////

# ~ 1 ~
# Get and prepare source data for importing into RD3


# ~ 1a ~
statusMsg('Pulling latest phenopacket data....')
phenopacketDT = dt.Frame(
  rd3.get(
    entity = 'rd3_portal_cluster_phenopacket',
    batch_size = 10000,
    q='subjectExists==true;processed==false'
  )
)

del phenopacketDT['_href']

# ~ 1b ~
# Transform columns
statusMsg('Formatting columns....')

phenopacketDT['dateofBirth'] = dt.Frame([
  d.split('-')[0] if d else None
  for d in phenopacketDT['dateofBirth'].to_list()[0]
])


# ~ 1c ~
# Merge Patch information
subjects['shouldMerge'] = dt.Frame([
  d in phenopacketDT['subjectID'].to_list()[0]
  for d in subjects['subjectID'].to_list()[0]
])

subjectPatchInfo = subjects[f.shouldMerge==True,:]
if subjectPatchInfo.nrows != phenopacketDT.nrows:
  raise ValueError('Warning dimensions of join dataset does not match target dataset')
  
subjectPatchInfo.key = 'subjectID'
phenopacketDT.key = 'subjectID'
phenopacketDT = phenopacketDT[:, :, dt.join(subjectPatchInfo['patch'])]


# ~ 1c ~
# Find releases
statusMsg('Finding unique RD3 releases and preparing import object....')

uniqueReleases = dt.unique(phenopacketDT['releasesWhereSubjectExists']).to_list()[0]

existingReleases = []
for release in uniqueReleases:
  values = release.split(',')
  if len(values) > 1:
    for value in values:
      if (value not in existingReleases) and (value != 'novelomics_original'):
        existingReleases.append(value)
  else:
    if value != 'novelomics_original':
      existingReleases.append(value)


datasetsToImport = {}
for release in existingReleases:
  datasetsToImport[release] = {
    'subject': {
      'sex1': None,
      'phenotype': None,
      'hasNotPhenotype': None,
      'disease': None,
      'phenopacketsID': None,
      'patch': None
    },
    'subjectinfo': {
      'dateofBirth': None,
      'ageOfOnset': None,
      'patch': None
    }
  }

# for each detected release prepare datasets
for dataset in datasetsToImport:
  statusMsg('Preparing data for',dataset,'....')
  phenopacketDT['tmpfilter'] = dt.Frame([
    bool(re.search(dataset, d))
    for d in phenopacketDT['releasesWhereSubjectExists'].to_list()[0]
  ])

  tmpDT = phenopacketDT[f.tmpfilter,:]
  tmpDT['subjectID'] = dt.Frame([f'{d}_original' for d in tmpDT['subjectID'].to_list()[0]])
  tmpDT['patch'] = dt.Frame([
    f"{d},{currentRelease}" if currentRelease not in d else d
    for d in tmpDT['patch'].to_list()[0]
  ])
  
  datasetsToImport[dataset]['subject']['patch'] = tmpDT[:, (f.subjectID, f.patch)] 
  datasetsToImport[dataset]['subject']['sex1'] = tmpDT[:, (f.subjectID, f.sex1)]
  datasetsToImport[dataset]['subject']['phenotype'] = tmpDT[:, (f.subjectID, f.phenotype)]
  datasetsToImport[dataset]['subject']['hasNotPhenotype'] = tmpDT[:, (f.subjectID, f.hasNotPhenotype)]
  if 'disease' in tmpDT.names:
    datasetsToImport[dataset]['subject']['disease'] = tmpDT[:, (f.subjectID, f.disease)]
  
  datasetsToImport[dataset]['subjectinfo']['dateofBirth'] = tmpDT[:, (f.subjectID, f.dateofBirth)]
  datasetsToImport[dataset]['subjectinfo']['patch'] = tmpDT[:, (f.subjectID, f.patch)]
  if 'ageOfOnset' in tmpDT.names:
    datasetsToImport[dataset]['subjectinfo']['ageOfOnset'] = tmpDT[:, (f.subjectID, f.ageOfOnset)]
  

# import data
for dataset in datasetsToImport:
  statusMsg('Importing dataset',dataset,'....')
  for table in datasetsToImport[dataset]:
    statusMsg('Updating table',table,'....')
    columns = datasetsToImport[dataset][table]
    for column in columns:
      pkg_entity = f"rd3_{dataset.replace('_original','')}_{table}"
      statusMsg('Updating column',column,'in table',pkg_entity)
      columnData = dtFrameToRecords(data=columns[column])
      rd3.updateColumn(
        entity=pkg_entity,
        attr = column,
        data = columnData
      )
      