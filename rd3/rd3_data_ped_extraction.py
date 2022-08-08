#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_data_ped_extraction.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-08-04
#' MODIFIED: 2022-08-08
#' PURPOSE: PED file extraction
#' STATUS: stable
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from dotenv import load_dotenv
from datatable import dt, join
from os import environ
from tqdm import tqdm
import re
import sys

load_dotenv()
sys.path.append(environ['SYS_PATH'])

from rd3.api.molgenis2 import Molgenis
from rd3.utils.clustertools import clustertools
from rd3.utils.pedtools import parseFileContents

# set latest release
currentRelease = 'freeze2'

rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

# rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
# rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

#//////////////////////////////////////////////////////////////////////////////

# ~ 1 ~ 
# Start Molgenis Session and Pull Required Data
#
# For processing the PED files, we will need the following attributes from the
# RD3 `rd3_freeze[x]_subject` where `[x]` is the freeze that the new PED files
# are tied to (e.g., `rd3_freeze2_subject`).
#
#   - `id`: the molgenis row ID; a concatenation of subject ID and release
#   - `subjectID`: RD3 P number
#   - `sex`: patient's sex
#   - `fid`: family ID
#
# It isn't necessary to run extensive checks that compare PED file data with
# the values that are in RD3 as PED files should be considered the most
# up to date.

rd3._print('Fetching existing subject metadata....')

# ~ 1a ~
# Compile subject metadata
# pull subject metadata for the current freeze
freeze = rd3.get(entity='rd3_overview',attributes='subjectID,patch', batch_size=10000)

# isolate subjectIDs and patch
rd3._print('Extracting patch information and creating a list of subject IDs....')

subjects = []
for row in tqdm(freeze):
  subject = {
    'subjectID': row['subjectID'],
    'release': row.get('patch')
  }
  if bool(subject.get('release')):
    subject['release'] = ','.join([
      item['id'] for item in subject['release'] if 'patch' not in item['id']
    ])
  subjects.append(subject)

del subject, row
subjectsDT = dt.Frame(subjects)

# get list of IDs only
subjectIDs = [row['subjectID'] for row in freeze]

#//////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Process PED files and extract content
#
# Compile a lost of all available PED files on the cluster and process each
# file individually. The processing step parses and restructures the contents
# into RD3 terminology. PED files are essentially tab separated text files
# that have the following columns. Each file is a family of a RD3 subject where
# each line is an individual. Files should have at least one row (the subject).
# Other lines contain data on family members if data was supplied by the data
# submitter.
#
# PED files contain six columns. 
#
#   0. Family ID
#   1. Individual ID
#   2. Paternal ID
#   3. Maternal ID
#   4. Sex: options are "1" (M), "2" (F), or "OTHER" (U)
#   5. Affected Status: "-9" (None), "0" (None), "1" (False), or "2" (True)
#
# See https://zzz.bwh.harvard.edu/plink/data.shtml for more information.
#
# By default all rows are given an "upload" status. If any of the validation
# steps fail, the upload status is set to FALSE. See the script `rd3tools.py`
# for more information on how values are validated.

# Create a list of all PED files that are stored at a specific path on the
# cluster. Make sure all non-PED files are removed.

# Create a list of all PED files that are stored at a specific path on the
# cluster. Make sure all non-PED files are removed.

# create list of all available JSON files
basePath = f"{environ['CLUSTER_BASE']}/{currentRelease}/ped"
allFiles = clustertools.listFiles(path=basePath)

availablePedFiles = [
  file for file in allFiles
  if re.search(r'(\.ped|\.ped.cip)$', file.get('filename'))
]

peddata = []
for index,file in enumerate(availablePedFiles):
  rd3._print('Processing',file['filename'],'(',index,'of',len(availablePedFiles),')')
  contents = clustertools.readTextFile(path = file['filepath'])
  data = parseFileContents(contents=contents, ids=subjectIDs, filename=file['filename'])
  for row in data:
    if row['upload']:
      row['pedID'] = file['filename']
      peddata.append(row)
      
# convert to DT object
pedDT = dt.Frame(peddata)

# set release path
pedDT['clusterRelease'] = basePath

# determine if subjectID is valid (they all should be)
pedDT['subjectExists'] = dt.Frame([
  d in subjectIDs
  for d in pedDT['subjectID'].to_list()[0]
])


# join release data
subjectsDT.key = 'subjectID'
pedDT.key = 'subjectID'
pedDT = pedDT[:, :, join(subjectsDT)]

# rename columns
pedDT.names = {'error_mid': 'unknownMID', 'error_pid': 'unknownPID', 'release': 'releasesWhereSubjectExists'}


# check for void columns and remove any unecessary columns
rd3._print('Checking data for void columns....')
for column in pedDT.names:
  if pedDT[column].types[0] == dt.Type.void:
    rd3._print('Changing class for column:', column)
    pedDT[column] = pedDT[column][:, dt.as_type(dt.f[column], dt.str32)]
    
del pedDT[:, ['id']]

#//////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Import Data into Portal

rd3._print('Importing data....')
rd3.importDatatableAsCsv(pkg_entity = 'rd3_portal_cluster_ped', data = pedDT)
