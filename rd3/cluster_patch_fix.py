#'////////////////////////////////////////////////////////////////////////////
#' FILE: cluster_patch1_fix.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-10-12
#' MODIFIED: 2022-05-13
#' PURPOSE: pull data from the cluster filemetadata table in RD3
#' STATUS: stable
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////


from rd3.api.molgenis import Molgenis
from rd3.utils.utils import statusMsg
import re

# for local dev, set credentials
from dotenv import load_dotenv
from os import environ
load_dotenv()

# host=environ['MOLGENIS_PROD_HOST']
host=environ['MOLGENIS_ACC_HOST']
rd3=Molgenis(url=host)
rd3.login(
    username=environ['MOLGENIS_ACC_USR'],
    password=environ['MOLGENIS_ACC_PWD']
)

# pull RD3 data
files=rd3.get(
    entity='rd3_portal_cluster',
    q='type=="phenopacket"',
    attributes='release,name,type',
    batch_size=10000
)

subjects=rd3.get(
    entity='rd3_freeze1_subject',
    attributes='id,subjectID,patch',
    batch_size=10000
)

statusMsg('File metadata entries pulled: {}'.format(len(files)))
statusMsg('Subject metadata entries pulled: {}'.format(len(subjects)))

# extract subject ID
for file in files:
    file['subject']=re.sub(
        pattern=r'((.[0-9]{4}-[0-9]{2}-[0-9]{2})?(.json))$',
        repl='',
        string=file['name']
    )
ids = [file['subject'] for file in files]

# update ptch
for s in subjects: 
    if s['subjectID'] in ids:
        patches = ['freeze1_patch1']
        for patch in s.get('patch'):
            patches.append(patch.get('id', None))
        s['patch'] = ','.join(list(set(patches)))
        

data = list(map(lambda x: {k: v for k, v in x.items() if k in ['id','patch']}, subjects))

# import into RD3
rd3.updateColumn(entity='rd3_freeze1_subject',attr='patch', data=data)
rd3.updateColumn(entity='rd3_freeze1_subjectinfo', attr='patch', data=data)
