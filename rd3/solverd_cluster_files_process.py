#///////////////////////////////////////////////////////////////////////////////
# FILE: solverd_cluster_files_process.py
# AUTHOR: David Ruvolo
# CREATED: 2023-03-24
# MODIFIED: 2023-03-27
# PURPOSE: process file metadata for a specific release
# STATUS: stable
# PACKAGES: **see below**
# COMMENTS: Login into Fender and run the script 'solverd_cluster_files_list.py'
# Save the file to the $HOME directory and download the file to you locally
# machine using the following command.
#
#   rsync -av corridor+fender:<csv-file> .
#
# It was decided to split this job into two scripts as it is 1) faster to
# compile metadata on the cluster and 2) it is easier to process and import
# datasets locally as there are issues setting up the venv.
#///////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from dotenv import load_dotenv
from datetime import datetime
from os import environ
from datatable import dt, f, as_type, fread
import re

def getFileType(value):
  search = re.search(r'(.json|.vcf.|.ped)', value)
  if search:
    return search.group().replace('.','')
  else:
    print(f'Unknown file format in {value}')

# connect to molgenis
load_dotenv()
rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
# rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Process data

filemeta = fread('data/rd3_cluster_files_freeze.csv')

# format filetype
filemeta['filetype'] = dt.Frame([
  getFileType(value)
  for value in filemeta['filetype'].to_list()[0]
])

# set date created
filemeta['created'] = str(datetime.now()).replace(' ','T') + 'Z'

# set release
filemeta['release'] = 'freeze3'

# set subjectID

def getID(value, type):
  """Extract ID by Type
  Types are: 'P', 'FAM', 'E'
  """
  search = re.search(r"^("+type+"[0-9]{1,})", value)
  if search:
    return search.group()
  return None

# extract subject identifier
filemeta['subjectID'] = dt.Frame([
  getID(value=value, type='P')
  for value in filemeta['filename'].to_list()[0]
])

# extract sample and experiment identifier
filemeta[['sampleID','experimentID']] = dt.Frame([
  getID(value = value, type='E')
  for value in filemeta['filename'].to_list()[0]
])

# prefix sample ID with VS-
filemeta['sampleID'] = dt.Frame([
  f"VS{value}" if value else value
  for value in filemeta['sampleID'].to_list()[0]
])

# extract family identifier
filemeta['familyID'] = dt.Frame([
  getID(value=value, type='FAM')
  for value in filemeta['filename'].to_list()[0]
])

#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Merge SolveRD data
# Retrieve subject metadata in order to merge family ID with existing subjectIDs
# and to assign subject ID(s) to FAMILYID cases

# pull existing metadata
subjects = dt.Frame(
  rd3.get(
    entity='solverd_subjects',
    attributes='subjectID,fid',
    batch_size=1000
  )
)[:, ['subjectID','fid']]


# merge subject ID if missing
filemeta['subjectID'] = dt.Frame([
  ','.join(subjects[f.fid==row[0], 'subjectID'].to_list()[0])
  if (row[0] is not None) and (row[1] is None) else row[1]
  for row in filemeta[:, ['familyID', 'subjectID']].to_tuples()
])

# merge family ID where missing
filemeta['familyID'] = dt.Frame([
  subjects[f.subjectID==row[1], 'fid'].to_list()[0][0]
  if (row[0] is None) and (row[1] is not None) else row[0]
  for row in filemeta[:, ['familyID', 'subjectID']].to_tuples()
])

#///////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Import data

rd3.importDatatableAsCsv(
  pkg_entity='rd3_portal_cluster_freeze3',
  data=filemeta
)
