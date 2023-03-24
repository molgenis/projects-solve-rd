#'////////////////////////////////////////////////////////////////////////////
#' FILE: cluster_listfiles.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-10-11
#' MODIFIED: 2022-05-13
#' PURPOSE: list and push phenopacket/ped files to RD3
#' STATUS: stable
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis import Molgenis
from rd3.utils.clustertools import clustertools, statusMsg
from dotenv import load_dotenv
from datetime import datetime
from os import environ, path
import pandas as pd
import re
load_dotenv()

rd3 = Molgenis(url=environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# rd3 = Molgenis(url=environ['MOLGENIS_ACC_HOST'])
# rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Compile File metadata

fender = clustertools('corridor+fender')

# list available directories 
directories=[
  row for row in fender.listFiles(path=environ['CLUSTER_BASE'])
  # if row['filename'] != 'master'
  if row['filename'] == 'freeze3'
]

# collate file metadata: release, name, path, type, etc.
files = []
for dir in directories:
  statusMsg('Processing files in', dir['filepath'])
  
  # gather list of pedigree (.ped) and phenopacket files (.json)
  raw_ped = fender.listFiles(dir['filepath'] + '/ped/')
  raw_json = fender.listFiles(dir['filepath'] + '/phenopacket/')
  ped_files = [
    f['filepath'] for f in raw_ped
    if re.search(r'((.ped)|(.ped.cip))$', f['filename'])
  ]
  json_files = [
    f['filepath'] for f in raw_json
    if re.search(r'(.json)$', f['filename'])
  ]
  cluster_files = ped_files + json_files
  
  # prep files
  for file in cluster_files:
    if re.search(r'((.ped)|(.ped.cip))$', file): folder = '/ped/'
    if re.search(r'(.json)$', file): folder = '/phenopacket/'
    fileMetadata = {
      'release': path.basename(dir['filepath']),
      'path': file,
      'name': path.basename(file),
      'type': folder.replace('/',''),
      'created': str(datetime.now()).replace(' ', 'T') + 'Z'
    }
    fileMetadata['md5sum'] = fender.md5sum(path=fileMetadata['path'])
    files.append(fileMetadata)

# filter files - remove duplicates
data = pd.DataFrame(files).drop_duplicates(subset='name', keep='first').to_dict('records')


# import into RD3
rd3.delete('rd3_portal_cluster')
rd3.importData(entity='rd3_portal_cluster', data=files)
