#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_data_patient_tree_mapping.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-06-20
#' MODIFIED: 2022-06-27
#' PURPOSE: mapping script for patient tree dataset
#' STATUS: experimental
#' PACKAGES: datatable, json
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from datatable import dt
from rd3.api.molgenis2 import Molgenis
from tqdm import tqdm
import json

def getExperiment(table,sample):
  labEndpoint = table.replace('_sample', '_labinfo')
  result = rd3.get(
    entity=labEndpoint,
    q =f'sample=={sample}'
  )
  if result:
    return result[0]['id']
  else:
    return None

# connect to DB --- local dev only
from os import environ
from dotenv import load_dotenv
load_dotenv()
rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

#//////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Prepare Tree Data

# pull example data (a more complex example)
# remove `q` and `num` when compiling the full batch
data = rd3.get(
  entity = 'rd3_overview',
  attributes = "subjectID,fid,samples"
)

# extract sample identifiers
patientdata = []
columnsToIgnore = ['_href', 'subjectID', 'fid', 'numberOfSamples', 'numberOfExperiments']

for row in data:
  for column in row.keys():
    if column not in columnsToIgnore:
      for entry in row[column]:
        newrecord = {
          'subjectID': row['subjectID'],
          'fid': row.get('fid'),
          'table': entry['_href'].split('/')[3], # get entity id
          'sampleID': entry["id"]
        }
        patientdata.append(newrecord)


for row in tqdm(patientdata):
  row['experiment'] = getExperiment(row['table'], row['sampleID'])


patientsamples = dt.Frame(patientdata)


# compile dataset
# For each subject, create row-level data for the table (i.e., search attributes)
# and the JSON stringified object. To make things easier, but all information
# in the json object so you have access to it in the front end

subjectidentifiers = dt.unique(patientsamples[:,'subjectID']).to_list()[0]
jsonData = []
counter = 0

for id in tqdm(subjectidentifiers):
  patientEntries = patientsamples[dt.f.subjectID == id,:]
  patientJson = {
    'id': f"RDI-{counter}",
    'subjectID': patientEntries[0,'subjectID'],
    'familyID': patientEntries[0,'fid'],
    'group': 'patient'
  }
  
  # process sample IDs if they exist
  patientSampleIds = dt.unique(patientEntries[:, 'sampleID']).to_list()[0]
  if patientSampleIds:
    samplecounter = 0
    patientJson['children'] = []
    for sampleID in patientSampleIds:
      sample = patientEntries[dt.f.sampleID == sampleID, :]

      patientSampleJson = {
        'id':  f"RDI-{counter}.{samplecounter}",
        'name': sampleID,
        'group': 'sample' 
      }
      
      # process experiment IDs if they exist
      patientExperimentIDs = sample['experiment'].to_list()[0]
      if patientExperimentIDs != [None]:
        patientSampleJson['children'] = []
        experimentcounter = 0
        for experimentID in patientExperimentIDs:
          if experimentID is not None:
            patientSampleJson['children'].append({
              "id": f"RDI-{counter}.{samplecounter}.{experimentcounter}",
              "name": experimentID,
              "group": 'experiment'
            })
            experimentcounter += 1
      samplecounter += 1
    patientJson['children'].append(patientSampleJson)

  patientRow = {
    'id': f"RDI-{counter}",
    'subjectID': patientEntries[0,'subjectID'],
    'familyID': patientEntries[0,'fid'],
    'group': 'patient',
    'json': json.dumps(patientJson)
  }

  counter += 1
  jsonData.append(patientRow)
  

#//////////////////////////////////////

# ~ 1 ~
# Import Data

patientTreeData = dt.Frame(jsonData)
rd3.importDatatableAsCsv(pkg_entity='rd3stats_treedata',data = patientTreeData)
