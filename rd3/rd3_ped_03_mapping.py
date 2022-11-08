#///////////////////////////////////////////////////////////////////////////////
# FILE: rd3_ped_03_mapping.py
# AUTHOR: David Ruvolo
# CREATED: 2022-09-21
# MODIFIED: 2022-09-21
# PURPOSE: Map PED data into RD3
# STATUS: stable
# PACKAGES: dotenv, datatable, os, rd3.utils, rd3.api
# COMMENTS: 
#///////////////////////////////////////////////////////////////////////////////

from rd3.utils.utils import dtFrameToRecords
from rd3.api.molgenis import Molgenis
from dotenv import load_dotenv
from datatable import dt, f, as_type
from os import environ
import requests
load_dotenv()

# set current release
currentRelease = 'freeze3_original'

# Connect to RD3: acc and prod
# By this point, the staging table on the ACC and PROD databases
# are updated. We can, theoretically, update both databases in one go. 

rd3_acc = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3_acc.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# get current release subjects to make sure rows are properly imported
freezeSubjects = dt.Frame(
  rd3_acc.get(
    f"rd3_{currentRelease.replace('_original', '')}_subject",
    attributes='id'
  )
)['id']
freezeSubjectIDs = freezeSubjects['id'].to_list()[0]

# get data
newPedDT = dt.Frame(rd3_acc.get('rd3_portal_cluster_ped', batch_size=10000))
del newPedDT['_href']

# rename releases column to RD3 terminology
newPedDT.names = {
  'releasesWhereSubjectExists': 'patch',
  'id': 'stagingTableId'
}

# set ID: append subject IDs with '_original' as it's the primary key
newPedDT['id'] = dt.Frame([
  f"{id}_original" for id in newPedDT['subjectID'].to_list()[0]
])


newPedDT['mid'] = dt.Frame([
  f"{id}_original"
  if id is not None else id
  for id in newPedDT['mid'].to_list()[0]
])

newPedDT['pid'] = dt.Frame([
  f"{id}_original"
  if id is not None else id
  for id in newPedDT['pid'].to_list()[0]
])

# change clinical_status to string
newPedDT['clinical_status'] = as_type(newPedDT['clinical_status'], dt.Type.str32)
newPedDT['clinical_status'] = dt.Frame([
  value.lower() if value is not None else value
  for value in newPedDT['clinical_status'].to_list()[0]
])


# make sure current freeze is in the 'patch' column
newPedDT['patch'] = dt.Frame([
  f"{tuple[0]},{currentRelease}" if tuple[1] in freezeSubjectIDs else tuple[0]
  for tuple in newPedDT[:, ['patch', 'id']].to_tuples()
])

newPedDT[dt.re.match(f.patch, '.*freeze3.*'), :]
newPedDT[dt.re.match(f.patch, '.*freeze3.*'), ['pedID', 'id', 'fid','mid', 'pid']]

newPedDT[dt.re.match(f.id, '.*1067.*'),:]

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Prepare import objects
# The goal of the PED data import is to not only import data into the current
# release, but also update records in other releases where the subject exists.
# For example, let's say we are working on importing Freeze Y data. If a subject
# in Freeze Y also exists in Freeze M and N, those records need to be updated.
# Make sure you run the validation script first as it will merge patch information.

# ~ 1a ~
# Find unique releases

uniqueReleaseStrings = dt.unique(newPedDT['patch']).to_list()[0]

# split release strings to get unique releases (exclude patches as those are
# table specific)
existingReleases = []
for releaseString in uniqueReleaseStrings:
  values = releaseString.split(',')
  for value in values:
    if (value not in existingReleases) and (value != 'novelomics_original') and ('patch' not in value):
      existingReleases.append(value)
      
      
# ~ 1b ~
# Init import object
# Using the existingReleases object, create a new object that will hold the
# datasets that will be imported into RD3 by release. For each release, we will
# create a series of datasets for each column that needs to be updated in the
# subjects table. These columns are: familyID (fid), maternal ID (mid),
# paternal ID (pid), and clinical status (clinical_status).

datasetsToImport = {}
for release in existingReleases:
  datasetsToImport[release] = {
    'subject': {
      'fid': None,
      'mid': None,
      'pid': None,
      'clinical_status': None,
      'patch': None
    }
  }

  
# ~ 1c ~
# Build datasets
for dataset in list(datasetsToImport.keys()):
  print('Building datasets for', dataset, '....')
  releaseDT = newPedDT[dt.re.match(f.patch, f".*{dataset}.*"), :]
  
  datasetsToImport[dataset]['subject']['fid'] = releaseDT[f.fid != None, (f.id, f.fid)]
  datasetsToImport[dataset]['subject']['mid'] = releaseDT[f.mid != None, (f.id, f.mid)]
  datasetsToImport[dataset]['subject']['pid'] = releaseDT[f.pid != None, (f.id, f.pid)]
  datasetsToImport[dataset]['subject']['clinical_status'] = releaseDT[f.clinical_status != None, (f.id, f.clinical_status)]
  datasetsToImport[dataset]['subject']['patch'] = releaseDT[f.patch != None, (f.id, f.patch)]
  

# view datasets
# datasetsToImport.keys()
# datasetsToImport['novelsrwgs_original']['subject']['fid']
# datasetsToImport['novelsrwgs_original']['subject']['mid']
# datasetsToImport['novelsrwgs_original']['subject']['pid']
# datasetsToImport['novelsrwgs_original']['subject']['patch']
# datasetsToImport['novelsrwgs_original']['subject']['clinical_status']
    
# ~ 1c ~
# Import datasets
for dataset in datasetsToImport:
  print('Importing dataset', dataset, '.....')
  for table in datasetsToImport[dataset]:
    print('Updating table', table, '....')
    attributes = datasetsToImport[dataset][table]
    for attr in attributes:
      pkg_entity = f"rd3_{dataset.replace('_original','')}_{table}"
      attrData = datasetsToImport[dataset][table][attr]
      if bool(attrData):
        if attrData.nrows > 0:
          attrData = attrData[:, dt.first(f[:]), dt.by(f.id)]
          print('Updating column', attr, 'in table', pkg_entity)
          attrDataToImport = dtFrameToRecords(attrData)
          
          # import into ACC/PROD
          rd3_acc.updateColumn(
            entity = pkg_entity,
            attr = attr,
            data = attrDataToImport
          )
          
          # rd3_prod.updateColumn(
          #   entity = pkg_entity,
          #   attr = attr,
          #   data = attrDataToImport
          # )
        
        
# or import data manuallly
rd3_acc.updateColumn(
  entity = 'rd3_freeze3_subject',
  attr = 'patch',
  data = dtFrameToRecords(
    data = datasetsToImport['freeze3_original']['subject']['patch']
  )
)
        
        
# disconnect
rd3_acc.logout()
rd3_prod.logout()
