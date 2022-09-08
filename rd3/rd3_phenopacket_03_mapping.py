#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_data_phenopacket_processing.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-08-02
#' MODIFIED: 2022-09-08
#' PURPOSE: process new phenopacket data
#' STATUS: in.progress
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from rd3.utils.utils import statusMsg, dtFrameToRecords
from rd3.api.molgenis import Molgenis
from dotenv import load_dotenv
from datatable import dt, f
from os import environ
import re
load_dotenv()

currentRelease = 'freeze3_original'

# Connect to RD3: acc and prod
# By this point, the phenopacket staging table on the ACC and PROD databases
# are updated. We can, theoretically, update both databases in one go. 

rd3_acc = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3_acc.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])


# get phenopacket data
newPhenopacketDT = dt.Frame(
  rd3_acc.get(entity = 'rd3_portal_cluster_phenopacket',batch_size = 10000)
)

del newPhenopacketDT['_href']

# rename releases column to RD3 terminology
newPhenopacketDT.names = {'releasesWhereSubjectExists': 'patch'}

# set id: append subjectIDs with '_original' as it's the primary key
newPhenopacketDT['id'] = dt.Frame([
  f'{subjectID}_original'
  for subjectID in newPhenopacketDT['subjectID'].to_list()[0]
])

# make sure current freeze is in the patch
newPhenopacketDT['patch'] = dt.Frame([
  f"{patch},{currentRelease}" if currentRelease not in patch else patch
  for patch in newPhenopacketDT['patch'].to_list()[0]
])

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Prepare Objects for import
# The goal here is to update not only the phenopacket data for the current
# release, but also the records in other releases. For example, if we have a
# freeze N phenopacket file that is linked to a participant that also exists
# in freeze X and Y, then we need to update the records in freeze X and Y.
# To do this, the validation script already merged release data. Using this data,
# we can find the unique releases and build the datasets accordingly.


# ~ 1a ~
# Find Unique Releases

# get unique release strings (comma-separated strings as it's an mref)
uniqueReleaseStrings = dt.unique(newPhenopacketDT['patch']).to_list()[0]


# split release strings to get unique releases (exclude patches)
existingReleases = []
for releaseString in uniqueReleaseStrings:
  values = releaseString.split(',')
  for value in values:
    if (value not in existingReleases) and (value != 'novelomics_original') and ('patch' not in value):
      existingReleases.append(value)


# ~ 1a ~
# Init import object
# For each unique release, initialize an object that will be used to update
# columns in the relevant freezes. For each release, we will update only
# update the phenopacket columns in the `rd3_<release>_subject` and
# `rd3_<release>_subjectinfo` tables. I'm structuring it this way as I can
# loop over each object and use the property names to build the endpoints.
datasetsToImport = {}
for release in existingReleases:
  statusMsg('Creating import dataset for',release)
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

statusMsg('Will update',len(datasetsToImport.keys()),'releases')


# ~ 1b ~
# Prepare datasets
# Here we will select the columns from the main datasets by release and add
# them to the datasetsToImportObject. Make sure all NA/None values are removed
# as we do not want to inadvertenly overwrite records with 'None'.
for dataset in list(datasetsToImport.keys()):
  statusMsg('Preparing data for',dataset,'....')
  
  # filter dataset for current releases
  releaseDT = newPhenopacketDT[dt.re.match(f.patch, f".*{dataset}.*"), :]
  
  # prepare patch datasets
  statusMsg('Preparing patch data....')
  releasePatchDT = releaseDT[f.patch != None, (f.id, f.patch)]
  datasetsToImport[dataset]['subject']['patch'] = releasePatchDT
  datasetsToImport[dataset]['subjectinfo']['patch'] = releasePatchDT
  
  # prepare `sex1` dataset
  statusMsg('Preparing sex data....')
  datasetsToImport[dataset]['subject']['sex1'] = releaseDT[
    f.sex1 != None, (f.id, f.sex1)
  ]
    
  # prepare date of birth
  if 'dateofBirth' in releaseDT.names:
    statusMsg('Preparing date of birth....')
    datasetsToImport[dataset]['subjectinfo']['dateofBirth'] = releaseDT[
      (f.dateofBirth != None) & (f.dateofBirth != 'NA'),
      (f.id, f.dateofBirth)
    ]
  
  # prepare phenotype data
  if 'phenotype' in releaseDT.names:
    statusMsg('Preparing observed phenotype data....')
    datasetsToImport[dataset]['subject']['phenotype'] = releaseDT[
      f.phenotype != None, (f.id, f.phenotype)
    ]
  
  if 'hasNotPhenotype' in releaseDT.names:
    statusMsg("Preparing unobserved phenotype data....")
    datasetsToImport[dataset]['subject']['hasNotPhenotype'] = releaseDT[
      f.hasNotPhenotype != None, (f.id, f.hasNotPhenotype)
    ]
  
  # prepare disease data
  if 'disease' in releaseDT.names:
    statusMsg('Preparing disease data....')
    datasetsToImport[dataset]['subject']['disease'] = releaseDT[
      f.disease != None, (f.id, f.disease)
    ]
  
  # process age of onset  
  if 'ageOfOnset' in releaseDT.names:
    statusMsg('Prearing Age of onset data.....')
    datasetsToImport[dataset]['subjectinfo']['ageOfOnset'] = releaseDT[
      f.ageOfOnset != None, (f.id, f.ageOfOnset)
    ]
  
  # process phenopacketID data
  if 'phenopacketsID' in releaseDT.names:
    statusMsg('Preparing phenopacketID data....')
    datasetsToImport[dataset]['subject']['phenopacketsID'] = releaseDT[
      f.phenopacketsID != None, (f.id, f.phenopacketsID)
    ]
  
  
# ~ 1c ~
# import data into both ACC and PROD 
for dataset in datasetsToImport:
  statusMsg('Importing dataset',dataset,'....')
  for table in datasetsToImport[dataset]:
    statusMsg('Updating table',table,'....')
    attributes = datasetsToImport[dataset][table]
    for attr in attributes:
      pkg_entity = f"rd3_{dataset.replace('_original','')}_{table}"
      attrData = datasetsToImport[dataset][table][attr]
      if bool(attrData):
        if attrData.nrows > 0:
          statusMsg('Updating column',attr,'in table',pkg_entity)
          attrDataToImport = dtFrameToRecords(attrData)
        
          # statusMsg('Importing into ACC....')
          # rd3_acc.updateColumn(entity=pkg_entity, attr=attr, data=attrDataToImport)
          
          statusMsg('Importing to PROD....')
          rd3_prod.updateColumn(entity=pkg_entity, attr=attr, data=attrDataToImport)
        del attrData

rd3_acc.logout()
rd3_prod.logout()
