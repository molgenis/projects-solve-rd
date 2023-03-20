#//////////////////////////////////////////////////////////////////////////////
# FILE: data_update_percentage.py
# AUTHOR: David Ruvolo
# CREATED: 2022-02-23
# MODIFIED: 2022-05-13
# PURPOSE: Update tumor percentages for novel omics data
# STATUS: stable
# PACKAGES: **see below**
# COMMENTS: NA
#//////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis import Molgenis
from rd3.utils.utils import dtFrameToRecords
from datatable import dt, f, fread, as_type


# for local dev
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

#///////////////////////////////////////

# ~ 1 ~
# Merge Datasets
# Join the external xlsx file with the RD3 data. 

# ~ 1a ~
# Load data from external file
# Download the latest file and import the contents. Select the columns of
# interest and rename them to align with the RD3 EMX. Set the key as well.

newData = fread('')[
    :, [
        'sample_id',
        'participant_subject',
        'pathological state',
        'tumor cell fraction'
    ]
]

newData.names = {
    'sample_id': 'sampleID',
    'participant_subject': 'subjectID',
    'pathological state': 'pathologicalState',
    'tumor cell fraction': 'percentageTumorCells'
}

newData[:, dt.update(sampleID = as_type(f.sampleID, str))]

newData.key = 'sampleID'


# ~ 1b ~
# Pull the deepwes data from RD3
# Unnest reference attributes and set key

samples = rd3.get(
    entity = 'rd3_noveldeepwes_sample',
    attributes = 'id,sampleID,subject',
    batch_size = 10000
)

for row in samples:
    row['subject'] = row['subject']['subjectID']

samples = dt.Frame(samples)
del samples['_href']

samples.key = 'sampleID'


# ~ 1c ~
# Join datasets
newSamplesData = samples[:, :, dt.join(newData)]

# recode attribute
newSamplesData['percentageTumorCells'] = dt.Frame([
    None if d == 'UK' else d
    for d in newSamplesData['percentageTumorCells'].to_list()[0]
])

# newSamplesData[:, dt.update(
#     percentageTumorCells = as_type(f.percentageTumorCells, dt.Type.int8)
# )]

#///////////////////////////////////////

# ~ 2 ~
# Import data

# prep data for import
pathologicalState = dtFrameToRecords(newSamplesData[:, ['id', 'pathologicalState']])
percentageTumorCells = dtFrameToRecords(newSamplesData[:, ['id','percentageTumorCells']])

# import data
rd3.updateColumn(
    entity = 'rd3_noveldeepwes_sample',
    attr = 'pathologicalState',
    data = pathologicalState
)

rd3.updateColumn(
    entity = 'rd3_noveldeepwes_sample',
    attr = 'percentageTumorCells',
    data = percentageTumorCells
)
