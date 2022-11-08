#///////////////////////////////////////////////////////////////////////////////
# FILE: rd3_ped_01_extraction.py
# AUTHOR: David Ruvolo
# CREATED: 2022-09-20
# MODIFIED: 2022-09-20
# PURPOSE: read and extract contents of PED files
# STATUS: in.progress
# PACKAGES: **see below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from turtle import up
from dotenv import load_dotenv
from datatable import dt, f
from os import environ
from tqdm import tqdm
import re
import sys

load_dotenv()
sys.path.append(environ['SYS_PATH'])

from rd3.api.molgenis2 import Molgenis
from rd3.utils.clustertools import clustertools
from rd3.utils.pedtools import parseFileContents

# set release
currentRelease = 'freeze3'
cluster = clustertools('corridor+fender')

# connect to RD3
rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# read in list of files to ignore (i.e., corrupt, invalid, etc)
excludedFiles = []
with open('data/files_ped_corrupt.txt', 'r') as file:
  excludedFiles = [line.replace('\n', '') for line in file.readlines()]
  file.close()
del file
  

#//////////////////////////////////////////////////////////////////////////////

# ~ 1 ~ 
# Start Molgenis Session and Pull Required Data
#
# For processing the PED files, we will need the following attributes from the
# RD3 `rd3_freeze[x]_subject` where `[x]` is the freeze that the new PED files
# are tied to (e.g., `rd3_freeze2_subject`).
#
#   - `subjectID`: RD3 P number
# 
# It isn't necessary to run extensive checks that compare PED file data with
# the values that are in RD3 as PED files should be considered the most
# up to date.

subjects = dt.Frame(
  rd3.get('rd3_overview', attributes='subjectID',batch_size = 10000)
)['subjectID']

# get list of identifiers only
subjectIDs = subjects['subjectID'].to_list()[0]

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

basePath=f"{environ['CLUSTER_BASE']}/{currentRelease}/ped"
allFiles = cluster.listFiles(path=basePath)

# remove files that aren't true ped and any file that are in the list of
# excluded files
pedFiles = [
  file for file in allFiles
  if (re.search(r'(.ped|.ped.cip)$', file['filename'])) and (file['filename'] not in excludedFiles)
]

rd3._print('Will process', len(pedFiles), 'ped files....')

pedDT = dt.Frame()
for file in tqdm(pedFiles):
  rawPedData = cluster.readTextFile(path=file['filepath'])
  parsedPedData = dt.Frame(parseFileContents(rawPedData, subjectIDs, file['filepath']))
  parsedPedData['clusterRelease'] = basePath
  parsedPedData['pedID'] = file['filename']
  pedDT = dt.rbind(pedDT, parsedPedData, force=True)


# create a unique identifier for each row
pedDT['id'] = dt.Frame([
  f"{row[1]}.{row[0]}"
  for row in pedDT[:, (f.pedID, f.subjectID)].to_tuples()
])

# check to see if all rows are distinct records
dt.unique(pedDT['id']).nrows == pedDT.nrows

# you may have to delete duplicate records as some PED files may have two
# entries for the same subject. If this is the case, manually view the rows
# and keep the rows that have the most data. Use the following commands
# pedDT[:, dt.count(), dt.by(f.rowID)][f.count > 1, :]
# pedDT[f.rowID=='', :]
# pedDT[(f.rowID=='') & (f.mid == None), :]
# del pedDT[(f.rowID=='') & (f.mid == None), :]

# create 
uploadablePedDT = pedDT[f.upload,:]
dt.unique(uploadablePedDT['id']).nrows == uploadablePedDT.nrows

#///////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Import Raw Data for Validation

# check for void columns
rd3._print("Checking data for void columns.....")
for column in uploadablePedDT.names:
  if uploadablePedDT[column].types[0] == dt.Type.void:
    rd3._print('Chaning class for column', column)
    uploadablePedDT[column] = uploadablePedDT[column][:, dt.as_type(f[column], dt.str32)]

# import
# rd3.delete('rd3_portal_cluster_ped')
# rd3_prod.delete('rd3_portal_cluster_ped')
rd3._print('Importing into rd3_portal_cluster_ped')
rd3.importDatatableAsCsv('rd3_portal_cluster_ped',uploadablePedDT)
rd3_prod.importDatatableAsCsv('rd3_portal_cluster_ped',uploadablePedDT)
