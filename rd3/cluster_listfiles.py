#'////////////////////////////////////////////////////////////////////////////
#' FILE: cluster_listfiles.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-10-11
#' MODIFIED: 2021-10-12
#' PURPOSE: list and push phenopacket/ped files to RD3
#' STATUS: working
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////


from rd3.api.molgenis import Molgenis, status_msg
from datetime import datetime
import pandas as pd
import hashlib
import re
import os

# for local dev only
from dotenv import load_dotenv
from os import environ
load_dotenv()

host=environ['MOLGENIS_HOST_PROD']
token=environ['MOLGENIS_TOKEN_PROD']
# host=environ['MOLGENIS_HOST_ACC']
# token=environ['MOLGENIS_TOKEN_ACC']
# host="http://localhost/api" 
# token="${molgenisToken}" 

rd3 = Molgenis(url=host,token=token)

# list available directories 
wd = environ['CLUSTER_BASE']
directories = [wd + '/' + x for x in os.listdir(wd) if x != 'master']

# collate file metadata: release, name, path, type, etc.
files = []
for dir in directories:
    status_msg('Processing files in', dir)
    
    # gather list of pedigree (.ped) and phenopacket files (.json)
    raw_ped = os.listdir(dir + '/ped/')
    raw_json = os.listdir(dir + '/phenopacket/')
    ped_files = [f for f in raw_ped if re.search(r'((.ped)|(.ped.cip))$', f)]
    json_files = [f for f in raw_json if re.search(r'(.json)$', f)]
    cluster_files = ped_files + json_files
    
    # prep files
    for file in cluster_files:
        if re.search(r'((.ped)|(.ped.cip))$', file): folder = '/ped/'
        if re.search(r'(.json)$', file): folder = '/phenopacket/'
        files.append({
            'release': os.path.basename(dir),
            'path': dir + folder + file,
            'name': file,
            'type': folder.replace('/',''),
            'created': str(datetime.now()).replace(' ', 'T') + 'Z',
            'md5sum': hashlib.md5(file.encode('utf-8')).hexdigest()
        })

# filter files - remove duplicates
data = pd.DataFrame(files).drop_duplicates(subset='name', keep='first').to_dict('records')


# import into RD3
rd3.delete('rd3_portal_cluster')
rd3.importData(entity='rd3_portal_cluster', data=files)
