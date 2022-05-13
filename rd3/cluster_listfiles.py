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
from datetime import datetime
import pandas as pd
import re
import os

# for local dev only
from dotenv import load_dotenv
load_dotenv()

# host=os.environ['MOLGENIS_PROD_HOST']
host=os.environ['MOLGENIS_ACC_HOST']
rd3 = Molgenis(url=host)
rd3.login(
    username=os.environ['MOLGENIS_ACC_USR'],
    password=os.environ['MOLGENIS_ACC_PWD']
)



# list available directories 
directories=[
    row for row in clustertools.listFiles(path=os.environ['CLUSTER_BASE'])
    if row['filename'] != 'master'
]

# collate file metadata: release, name, path, type, etc.
files = []
for dir in directories:
    statusMsg('Processing files in', dir['filepath'])
    
    # gather list of pedigree (.ped) and phenopacket files (.json)
    raw_ped = clustertools.listFiles(dir['filepath'] + '/ped/')
    raw_json = clustertools.listFiles(dir['filepath'] + '/phenopacket/')
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
            'release': os.path.basename(dir['filepath']),
            'path': file,
            'name': os.path.basename(file),
            'type': folder.replace('/',''),
            'created': str(datetime.now()).replace(' ', 'T') + 'Z'
        }
        fileMetadata['md5sum'] = clustertools.md5sum(path=fileMetadata['path'])
        files.append(fileMetadata)

# filter files - remove duplicates
data = pd.DataFrame(files).drop_duplicates(subset='name', keep='first').to_dict('records')


# import into RD3
rd3.delete('rd3_portal_cluster')
rd3.importData(entity='rd3_portal_cluster', data=files)
