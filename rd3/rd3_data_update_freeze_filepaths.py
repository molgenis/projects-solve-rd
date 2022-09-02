#//////////////////////////////////////////////////////////////////////////////
# FILE: rd3_data_update_filepaths.py
# AUTHOR: David Ruvolo
# CREATED: 2022-09-01
# MODIFIED: 2022-09-02
# PURPOSE: Update "Phenopacket Patch" in the subject and file tables by release
# STATUS: in.progress
# PACKAGES: **see below**
# COMMENTS: The purpose of this script is to add location of the files on the 
# cluster to the files table. This script is designed to run by release and
# RD3 instance. 
#//////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from dotenv import load_dotenv
from datatable import dt, f
from os import environ, path
import re
load_dotenv()

# set env vars
clusterRelease = 'freeze2'  # name of the package in `rd3_portal_cluster`
currentRelease = 'freeze2'  # name of the RD3 package

rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

# rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
# rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

#///////////////////////////////////////

# ~ 1 ~
# Source data

# ~ 1a ~
# get existing file metadata for the chosen RD3 release
currentReleaseFilesTable = f"rd3_{currentRelease}_file"
files = rd3.get(entity=currentReleaseFilesTable, batch_size=10000)

for row in files:
  if 'typeFile' in row:
    row['typeFile'] = row.get('typeFile', {}).get('identifier')
  
  if 'samples' in row:
    row['samples'] = ','.join([sample.get('id') for sample in row.get('samples')])
  
  if 'subjectID' in row:
    row['subjectID'] = ','.join([subject.get('id') for subject in row.get('subjectID')])
    
  if 'patch' in row:
    row['patch'] = ','.join([patch.get('id') for patch in row.get('patch')])
    

# convert to datatable object
filesDT = dt.Frame(files)
del filesDT['_href']

# create basename column from `name` (which is filepath)

def getBaseName(file, type):
  basename = path.basename(file)
  if type in ['json', 'ped']:
    basename = ''.join(re.split('(.json|.ped)', basename)[:2])
  if type == 'vcf':
    basename = ''.join(re.split('(.vcf.gz)', basename)[:2])
  return basename

filesDT['filename'] = dt.Frame([
  getBaseName(file=row[0], type=row[1])
  for row in filesDT[:, (f.name, f.typeFile)].to_tuples()
])

# ~ 1b ~
# Source file overtable tables from the portal
clusterFiles = rd3.get(
  entity = f"rd3_portal_cluster_{clusterRelease}",
  batch_size = 10000
)

# convert to datatable object and select columns of interest
clusterFilesDT = dt.Frame(clusterFiles)[:, (f.filename, f.filepath)]
clusterFilesDT.names = {'filepath': 'filepath_sandbox'}
del clusterFiles

#///////////////////////////////////////

# ~ 2 ~
# Join tables

# set keys for both objects
clusterFilesDT.key = 'filename'
filesDT.key = 'filename'

# merge
filesDT = filesDT[:, :, dt.join(clusterFilesDT)]

# check merge
# NOTE: I think most of the missing cases will be the bam files as these
# aren't stored on the cluster at the moment
filesDT['filepath_sandbox']
filesDT[f.filepath_sandbox==None,:]
dt.unique(filesDT[f.filepath_sandbox==None,'typeFile'])

# check patch
dt.unique(filesDT['patch'])


#///////////////////////////////////////

# ~ 3 ~ 
# Import

rd3.importDatatableAsCsv(pkg_entity = currentReleaseFilesTable, data = filesDT)

rd3.logout()