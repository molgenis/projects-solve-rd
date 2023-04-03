#///////////////////////////////////////////////////////////////////////////////
# FILE: solverd_verify_releases.py
# AUTHOR: David Ruvolo
# CREATED: 2023-04-03
# MODIFIED: 2023-04-03
# PURPOSE: script to verify all individual releases 
# STATUS: complete
# PACKAGES: **see below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.utils.utils import flattenDataset
from rd3.api.molgenis2 import Molgenis
from dotenv import load_dotenv
from datatable import dt, f,fread
from os import environ
import pandas as pd
import re
load_dotenv()

rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# get subjects info
subjects = rd3.get('solverd_subjects', batch_size=10000)
subjectsDT = flattenDataset(data=subjects,columnPatterns='subjectID|id|value')
subjectsDT = dt.Frame(subjectsDT)

subjectIDs = subjectsDT['subjectID'].to_list()[0]

# import reference dataset
releaseDT = fread('data/gpap_solverd_experiments_subprojects_20230323.csv')

releaseDT.names = {
  'RDConnect ID': 'sampleID',
  'Subproject': 'release',
  'Participant ID': 'subjectID',
  'EGA ID': 'egaID'
}

def recodeRelease(value):
  match = re.search(r'^(SolveDF[0-9]|NovelWGS)', value)
  if match:
    if 'SolveDF' in match.group():
      num = re.search(r'[0-9]', match.group()).group()
      return f"freeze{num}_original"
    if 'NovelWGS' in match.group():
      return 'novelwgs_original'
  return None

# recode releases
releaseDT['partOfRelease'] = dt.Frame([
  recodeRelease(value) if value is not None else value 
  for value in releaseDT['release'].to_list()[0]
])

# init status column
releaseDT['shouldUpdate'] = None
# del releaseDT['shouldUpdate']

# check to see if all SolveRD subjects are missing the 'official' release info
releaseSubjectIDs = releaseDT['subjectID'].to_list()[0]

for id in releaseSubjectIDs:
  if id in subjectIDs:
    officialRelease = releaseDT[f.subjectID==id,'partOfRelease'].to_list()[0][0]
    existingRelease = subjectsDT[f.subjectID==id,'partOfRelease'].to_list()[0][0]
    if bool(officialRelease) and bool(existingRelease):
      releaseDT[f.subjectID==id,'shouldUpdate'] = officialRelease not in existingRelease
  else:
    print(f"Warning: subjectID '{id}' does not exist in RD3")
    releaseDT[f.subjectID==id,'isUnknown'] = True


# review mappings
releaseDT[:, dt.count(), dt.by(f.shouldUpdate)]
releaseDT[f.shouldUpdate==False, :]
releaseDT[f.shouldUpdate, :]
releaseDT[f.shouldUpdate, :][:, dt.count(), dt.by(f.partOfRelease)]
releaseDT[f.isUnknown, :]

# update solverd_subjects with missing release info
updateIDs = releaseDT[f.shouldUpdate, 'subjectID'].to_list()[0]

for id in updateIDs:
  releases = subjectsDT[f.subjectID==id,'partOfRelease'].to_list()[0][0].split(',')
  newReleases = releaseDT[f.subjectID==id,'partOfRelease'].to_list()[0]
  for rel in newReleases:
    releases.append(rel)
  releases.sort()
  subjectsDT[f.subjectID==id,'partOfRelease'] = ','.join(list(set(releases)))
  
# import  
rd3.importDatatableAsCsv(pkg_entity='solverd_subjects', data=subjectsDT)