#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_data_patient_tree_mapping.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-06-20
#' MODIFIED: 2022-11-17
#' PURPOSE: mapping script for patient tree dataset
#' STATUS: stable
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from dotenv import load_dotenv
from os import environ
import sys
load_dotenv()
sys.path.append(environ['SYS_PATH'])

from datatable import dt, f
from rd3.api.molgenis2 import Molgenis
from tqdm import tqdm
import json

# connect to database
# rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
# rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

#//////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Prepare Tree Data
# Since most of the data is available in the "Solve-RD Experiments" table, we
# can use it as the source for the patient tree dataset. For the UI, we would like
# the data to be searchable for subject- and family identifiers rather than
# everything. It is also better to do as much preprocessing to eliminate the
# need to transform the data in javascript. The structure of the data should
# look like the following.
#
#   | subjectID | familyID | json     |
#   |-----------|----------|----------|
#   | P12345    | F12345   | "{...}"  |
#
# The value in the json column should contain everything that is needed in
# Vue component. The structure of the json object should look like this.
#
# - Patient
#   - Sample n
#     - Experiment n
#     ....
#   ....
#
# {
#   id: "<some-identifier>",
#   subjectID: "<rd3-subject-identifier>",
#   familyID: "<rd3-family-identifier>",
#   group: "patient",
#   href: "<url-to-subject-record>",
#   children: [
#     {
#       id: "<some-identifier>+n",
#       name: "sample-idenfier",
#       group: "sample",
#       href: "<url-to-sample-record>",
#       children: [
#         {
#           id: "<some-identifier>+n",
#           name: "<experiment-idenfier>",
#           group: "experiment",
#           href: "<url-to-experiment-record>",
#         },
#         ....
#       ]
#     },
#     ....
#   ]
# }
#
# Entries should be rendered for all subjects regardless if they have sample
# or experiment metadata (as this is important to know). Build a list of 
# subjects from the Subjects table, and then add samples and experiments.
#

# ~ 1a ~
# Get Data

# pull all available subjects
subjects = dt.Frame(
  rd3.get(
    'solverd_subjects',
    attributes = 'subjectID,fid',
    batch_size=10000
  )
)
subjectIDs = subjects['subjectID'].to_list()[0]

# get sample metadata
solverdSamples = rd3.get(
  'solverd_samples',
  attributes='sampleID,belongsToSubject',
  batch_size=10000
)

# flatten ref attributes
for row in solverdSamples:
  if 'belongsToSubject' in row:
    if bool(row['belongsToSubject']):
      row['belongsToSubject'] = row['belongsToSubject']['subjectID']
      

# get experiment metadata
solverdExperiments = rd3.get(
  'solverd_labinfo',
  attributes="experimentID,sampleID",
  batch_size=10000
)

# flatten ref attributes
for row in solverdExperiments:
  if 'sampleID' in row:
    if bool(row['sampleID']):
      row['sampleID'] = ','.join([
        item['sampleID'] for item in row['sampleID']
      ])
    else:
      row['sampleID'] = None

# as datatable objects
samples = dt.Frame(solverdSamples)
experiments = dt.Frame(solverdExperiments)

del subjects['_href']
del samples['_href']
del experiments['_href']

#///////////////////////////////////////

# ~ 1b ~
# Create Tree data object
# Data will be joined in a few steps. It is easier to start with the experiments
# dataset, as this gives us a large number of samples and experiments to work with.
# Then, bind samples and subjects that aren't in the dataset.

# ~ 1b.i ~
# Merge subjectIDs with experiments using the samples datatable
samples.key = 'sampleID'
experiments.key = 'sampleID'

summarizedDT = experiments[:, :, dt.join(samples)][
  f.belongsToSubject!=None,
  (f.belongsToSubject, f.sampleID, f.experimentID)
][:, :, dt.sort(f.belongsToSubject)]


# ~ 1b.ii ~
# identify samples that are not yet in treedata
sampleIDs = summarizedDT['sampleID'].to_list()[0]
samples['missing'] = dt.Frame([
  id not in sampleIDs
  for id in tqdm(samples['sampleID'].to_list()[0])
])

# join
summarizedDT = dt.rbind(
  summarizedDT,
  samples[
    f.missing, (f.belongsToSubject, f.sampleID)
  ][f.belongsToSubject != None, :],
  force=True
)

# ~1b.iii ~
# merge family IDs
subjectFamilyIDs = subjects[:, (f.subjectID, f.fid)][f.fid != None, :]
subjectFamilyIDs.names = {'subjectID': 'belongsToSubject' }
subjectFamilyIDs.key = 'belongsToSubject'
summarizedDT = summarizedDT[:, :, dt.join(subjectFamilyIDs)]


# ~ 1b.iv ~
# identify subjects that are not yet in the treedata
summarizedSubjectIDs = summarizedDT['belongsToSubject'].to_list()[0]
subjects['missing'] = dt.Frame([
  value not in summarizedSubjectIDs
  for value in tqdm(subjects['subjectID'].to_list()[0])
])

# subjects[f.missing, :]
# join and update names
summarizedDT = dt.rbind(
  summarizedDT,
  subjects[f.missing, {
    'belongsToSubject': f.subjectID,
    'fid': f.fid
  }],
  force = True
)

# sort data
summarizedDT = summarizedDT[
  :, :, dt.sort(f.belongsToSubject, f.sampleID, f.experimentID)
]


def initJson(index, subjectID, familyID):
  baseUrl='/menu/plugins/dataexplorer?entity=solverd_subjects'
  url = f"{baseUrl}&filter=subjectID%3Dq%3D{subjectID}"
  return {
    'id': f"RD-{index}",
    'subjectID': subjectID,
    'familyID': familyID,
    'group': 'patient',
    'href': url
  }
  
def initSubJson(index, id, group, table, tableAttr):
  baseUrl = f"/menu/plugins/dataexplorer?entity=solverd_{table}"
  url=f"{baseUrl}&filter={tableAttr}%3Dq%3D{id}"
  return {
    'id': f"RD-{index}",
    'name': id,
    'group': group,
    'href': url
  }
   
# ~ 1b.v ~ 
# Compile JSON
treeDataSubjects = dt.unique(summarizedDT['belongsToSubject']).to_list()[0]
treedata = dt.Frame([{'id': '', 'belongsToSubject': '', 'fid': '', 'json': ''}])

for (index, id) in enumerate(tqdm(treeDataSubjects)):
  # filter data for subject and init json
  subjectData = summarizedDT[f.belongsToSubject == id, :]
  newRow = subjectData[0, (f.belongsToSubject, f.fid)]
  newRow['id'] = f"RD-{index}"
  subjectJson = initJson(
    index = index,
    subjectID = id,
    familyID = newRow['fid'].to_list()[0][0]
  )
  
  # compile json if there are sampleIDs
  subjectSamples = dt.unique(subjectData['sampleID']).to_list()[0]
  if bool(subjectSamples):
    subjectJson['children'] = []
    
    # loop through sampleIDs
    sampleIDs = subjectData['sampleID'].to_list()[0]
    for (sampleIndex, sampleID) in enumerate(sampleIDs):
      sampleJson = initSubJson(
        index = f"{index}.{sampleIndex}",
        id = sampleID,
        group = 'sample',
        table = 'samples',
        tableAttr = 'sampleID'
      )
      
      # detect experiment IDs
      experimentIDs = subjectData[f.sampleID == sampleID, 'experimentID'].to_list()[0]
      if bool(experimentIDs):
        sampleJson['children'] = []
        for (experimentIndex, experimentID) in enumerate(experimentIDs):
          experimentJson = initSubJson(
            index = f"{index}.{sampleIndex}.{experimentIndex}",
            id = experimentID,
            group = 'experiment',
            table = 'labinfo',
            tableAttr = 'experimentID'
          )
          sampleJson['children'].append(experimentJson)
      
      # add sample json object to subject json
      subjectJson['children'].append(sampleJson)
  
  # add subject json to treedata record for this subject
  newRow['json'] = json.dumps(subjectJson)
  treedata = dt.rbind(treedata, newRow, force=True)

# prepare data
treedata.names = {
  'belongsToSubject': 'subjectID',
  'fid': 'familyID'
}

treedata = treedata[f.id != '', :]

treedata.to_csv('data/rd3stats_treedata.csv')

#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Import Data

# rd3.delete('rd3stats_treedata')
rd3.importDatatableAsCsv('rd3stats_treedata', treedata)