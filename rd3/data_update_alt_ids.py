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
from datatable import dt, fread, rbind, f, join, first, as_type
from os import environ

# set vars and init session
load_dotenv()
# host = environ['MOLGENIS_HOST_ACC']
# token = environ['MOLGENIS_TOKEN_ACC']
host = environ['MOLGENIS_HOST_PROD']
token = environ['MOLGENIS_TOKEN_PROD']

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
# Pull LabInfo Data

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

#//////////////////////////////////////

# ~ 1b ~
# Pull Sample Data

samples = []
releases = ['novelomics'] #['freeze1', 'freeze2']
for r in releases:
    print(f'Pulling RD3 {r} samples...')
    tmp_entity = f'rd3_{r}_sample'
    rawData = rd3.get(tmp_entity, attributes='id,sampleID,subject,alternativeIdentifier')
    data = []
    for d in rawData:
        if 'subject' in d:
            d['subjectKey'] = d.get('subject',{}).get('id')
            d['subjectID'] = d.get('subject',{}).get('subjectID')
            d['release'] = r
            del d['subject'], d['_href']
            data.append(d)
    samples = rbind(
        samples,
        dt.Frame(data)[:, {
            'id': f.id,
            'sampleID': f.sampleID,
            'subjectID': f.subjectID,
            'subjectKey': f.subjectKey,
            'release': f.release
        }]
    )
    print('Done!')
    
# set key as subject
samples = samples[:, first(f[:]), dt.by(f.subjectID)]
samples.key = 'subjectID'
    
    
#//////////////////////////////////////

# ~ 1c ~
# Map New Compound IDs

newData = fread('data/2022-01-11.NovelWGS.NewIDs.tsv', header=False)

# set column names, delete columns, and set key
newData.names = [
    'experimentID',
    'subjectID',
    'alternativeIdentifier',
    'Organization',
    'batch',
    'ERN'
]
del newData[:, ['Organization','batch','ERN']]
newData.key = ['experimentID']

newData[:, :, join(samples)]

# newData['EID_EXISTS'] = dt.Frame([
#     d in labinfo['experimentID'].to_list()[0]
#     for d in newData['experimentID'].to_list()[0]
# ])
#
# newData['PID_EXISTS'] = dt.Frame([
#     d in samples['subjectID'].to_list()[0]
#     for d in newData['subjectID'].to_list()[0]
# ])
#
# newData[:, dt.update(
#     EID_EXISTS = as_type(f.EID_EXISTS, str),
#     PID_EXISTS = as_type(f.PID_EXISTS, str)
# )]
#
# del newData.key
#
# newData[
#     f.PID_EXISTS == False,
#     ['experimentID','subjectID','alternativeIdentifier','PID_EXISTS']
# ].to_pandas().to_csv('data/2022-01-19_novelwgs_new_ids_mapping_results.tsv',index=False,sep='\t')