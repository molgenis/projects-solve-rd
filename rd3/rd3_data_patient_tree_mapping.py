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

# connect to DB --- local dev only
from os import environ
from dotenv import load_dotenv
load_dotenv()

rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

def getExperiment(table,sample):
  """Get Experiment
  Find all corresponding experiment identifiers from a specific table by
  a sample identifier
  
  @param table name of a table in the database
  @param sample sample identifier
  """
  labEndpoint = table.replace('_sample', '_labinfo')
  result = rd3.get(entity=labEndpoint, q =f'sample=={sample}')
  if result:
    return result[0]['id']
  else:
    return None
    
def cleanIdentifier(value, patterns=None):
  """Clearn Identifier
  For sample and experiment IDs, format IDs by parsing the internal
  identifier in order to extract the experiment type
  
  @param value string containing a sample or experiment identifier
  @param patterns an object of values to determine if the first
    element post-split is an experiment type or an ID. To preserve the
    structure of the incoming data, some experiment IDs are structured
    like `<experiment_type>_<identifier>` where the reverse pattern is
    used for all other experiments.
  
  @examples
  cleanIdentifier('12345_original')
  cleanIdentifier('12345_experiment_original')
  
  @return string
  """
  valueSplit = value.split('_')
  id = valueSplit[0]
  if (patterns) and (id in patterns):
    experiment = [pattern for pattern in patterns if pattern == id][0]
    id = valueSplit[1].upper()
  else:
    experiment = ','.join([val.upper() for val in valueSplit[1:] if val != 'original'])
  if experiment:
    return f"{id} ({experiment})"
  else:
    return id
    
def createUrlFilter(entity, attribute, value):
  """Create url filter
  Generate dataexplorer url with attribute filter
  
  @param entity name of the table
  @param attribute name of the column
  @param value 
  """
  return f"/menu/plugins/dataexplorer?entity={entity}&filter={attribute}%3Dq%3D{value}"

#//////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Prepare Tree Data

data = rd3.get(entity = 'rd3_overview', attributes = "subjectID,fid,samples")

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

# generate hrefs
patientsamples = dt.Frame(patientdata)

patientsamples['subjectHref'] = dt.Frame([
  createUrlFilter(d[0].replace('_sample','_subject'), 'subjectID', d[1])
  if d[0] and d[1] else None
  for d in patientsamples[:, ['table', 'subjectID']].to_tuples()
])

patientsamples['sampleHref'] = dt.Frame([
  createUrlFilter(d[0], 'id', d[1])
  if d[0] and d[1] else None
  for d in patientsamples[:,['table','sampleID']].to_tuples()
])

patientsamples['experimentHref'] = dt.Frame([
  createUrlFilter(d[0].replace('_sample', '_labinfo'), 'id', d[1])
  if d[0] and d[1] else None
  for d in patientsamples[:, ['table', 'experiment']].to_tuples()
])

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
    'group': 'patient',
    'href': patientEntries[0,'subjectHref']
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
        'name': cleanIdentifier(sampleID),
        'group': 'sample',
        'href': sample[0, 'sampleHref']
      }
      
      # process experiment IDs if they exist
      patientExperimentIDs = sample['experiment'].to_list()[0]
      if patientExperimentIDs != [None]:
        patientSampleJson['children'] = []
        experimentcounter = 0
        for experimentID in patientExperimentIDs:
          experiment = patientEntries[dt.f.experiment == experimentID, :]
          if experimentID is not None:
            patientSampleJson['children'].append({
              'id': f"RDI-{counter}.{samplecounter}.{experimentcounter}",
              'name': cleanIdentifier(experimentID, ['SR-WGS']),
              'group': 'experiment',
              'href': experiment[0, 'experimentHref']
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
rd3.importDatatableAsCsv(pkg_entity='rd3stats_treedata', data = patientTreeData)
