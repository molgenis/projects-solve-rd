#'////////////////////////////////////////////////////////////////////////////
#' FILE: data_update_alt_ids.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-01-18
#' MODIFIED: 2022-01-18
#' PURPOSE: Update alternative identifiers from file 
#' STATUS: stable
#' PACKAGES: 
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import python.rd3tools as rd3tools
from dotenv import load_dotenv
from datatable import dt, fread, rbind, f, join, first
from os import environ

# set vars and init session
load_dotenv()
host = environ['MOLGENIS_HOST_ACC']
token = environ['MOLGENIS_TOKEN_ACC']
# host = environ['MOLGENIS_HOST_PROD']
# token = environ['MOLGENIS_TOKEN_PROD']

rd3 = rd3tools.molgenis(url=host, token=token)

#//////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Map Updated Alternative IDs
# 
# For each release, alternative Identifiers are stored in the samples table.
# However, in the mapping file, alternative identifiers are mapped to
# experiment IDs (EID). In order to update the samples table, pull records
# from the LabInfo table. This will give you experiment to sample mappings,
# which you can use to map the alternative IDs.
#


# ~ 1a ~ 
# Pull Required Data

# Pull Freeze 1 Lab Info
labinfo_freeze1 = rd3.get('rd3_freeze1_labinfo',attributes='id,experimentID,sample')
for d in labinfo_freeze1:
    d['sampleID'] = d.get('sample',{})[0].get('id')
    del d['sample'], d['_href']
    
# Pull Freeze 2 Lab Info
labinfo_freeze2 = rd3.get('rd3_freeze2_labinfo',attributes='id,experimentID,sample')
for d in labinfo_freeze2:
    d['sampleID'] = d.get('sample',{})[0].get('id')
    del d['sample'], d['_href']

# Merge objects
labinfo = rbind(
    dt.Frame(labinfo_freeze1)[:, {
        # 'id': f.id,
        'experimentID': f.experimentID,
        'sampleID': f.sampleID,
        'release': 'freeze1'
    }],
    dt.Frame(labinfo_freeze2)[:, {
        # 'id': f.id,
        'experimentID': f.experimentID,
        'sampleID': f.sampleID,
        'release': 'freeze2'
    }]
)[
    :, first(f[:]), dt.by(f.experimentID,f.sampleID)
]

labinfo.key = 'experimentID'

# ~ 1b ~
# Map New Compound IDs
newAltIDs = fread('data/2022-01-11.NovelWGS.NewIDs.tsv', header=False)
newAltIDs.names = [
    'experimentID',
    'subjectID',
    'alternativeIdentifier',
    'Organization',
    'batch',
    'ERN'
]

del newAltIDs[:, ['Organization','batch','ERN']]

newAltIDs.key = ['experimentID']

x = newAltIDs[:, :, join(labinfo)]