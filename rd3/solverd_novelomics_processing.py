#///////////////////////////////////////////////////////////////////////////////
# FILE: solverd_novelomics_processing.py
# AUTHOR: David Ruvolo
# CREATED: 2022-11-15
# MODIFIED: 2022-11-15
# PURPOSE: Import new novelomics data
# STATUS: in.progress
# PACKAGES: NA
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from datatable import dt, f, as_type
from dotenv import load_dotenv
from os import environ
from tqdm import tqdm
import pandas as pd
import numpy as np
import functools
import operator
import re

def getWrappedValues(value):
  search = re.search(r'([\(].*?[\)])', value)
  if search:
    match=search.group()
    return re.sub(r'[\(\)]', '', match)
  else:
    return None

def datableToRecordset(data):
  return data.to_pandas().replace({ np.nan: None }).to_dict('records')

# ~ 0 ~
# Connect to DB and get data

load_dotenv()
rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

data = dt.Frame(pd.read_excel('data/shipment_metadata.xlsx'))
data[:, dt.update(sample_id=as_type(f.sample_id, str))]
data.names = { 'sample_id': 'sampleID', 'tissue_types': 'tissueType' }

solverdSamples = rd3.get('solverd_samples', batch_size=1000)
solverdSamples[:1]

# flatten all nested attributes (xref, mref)
columnPatterns = r'id|subjectID|value'
for row in tqdm(solverdSamples):
  if '_href' in row:
    del row['_href']
  for column in row.keys():
    if isinstance(row[column], dict):
      if bool(row[column]):
        columnMatch = re.search(columnPatterns, ','.join(row[column].keys()))
        if bool(columnMatch):
          row[column] = row[column][columnMatch.group()]
        else:
          print(f'Variable {column} is type "dict", but no target column found')
      else:
        row[column] = None
    if isinstance(row[column], list):
      if bool(row[column]):
        values = []
        for nestedrow in row[column]:
          columnMatch = re.search(columnPatterns, ','.join(nestedrow.keys()))
          if bool(columnMatch):
            values.append(nestedrow[columnMatch.group()])
          else:
            print(f'Variable {column} is type "list", but no target column found')
        if bool(values):
          row[column] = ','.join(values)
      else:
        row[column] = None

samples = dt.Frame(solverdSamples)

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Update columns

# find matching samples only
matchingSamples = samples[
  functools.reduce(
    operator.or_,
    (f.sampleID == str(item) for item in data['sampleID'].to_list()[0])
  ), :
]

# do matching samples match new data?
matchingSamples.nrows == data.nrows

# ~ 1a ~
# update tissueTypes -- make sure there are no unique values
dt.unique(matchingSamples['tissueType'])
del matchingSamples['tissueType']

# join data
tissueTypes = data[:, (f.sampleID, f.tissueType)]
tissueTypes.key = 'sampleID'
matchingSamples.key = 'sampleID'
matchingSamples = matchingSamples[:, :, dt.join(tissueTypes)]


# ~ 1b ~
# Update materialTypes
dt.unique(matchingSamples['materialType'])
matchingSamples[:, dt.count(), dt.by(f.partOfRelease, f.materialType)]

materialTypes = data[:, (f.sampleID, f.materialType, f.pathological_state)]
materialTypes.names = {'materialType': 'materialType2'}
materialTypes.key = 'sampleID'
matchingSamples.key = 'sampleID'
matchingSamples = matchingSamples[:, :, dt.join(materialTypes)]

materialTypeMatches = matchingSamples[
  :, {
    'sampleID': f.sampleID,
    'partOfRelease': f.partOfRelease,
    'materialType_Original': f.materialType,
    'materialType_Updated': f.materialType2,
    'pathologicalState_Original': f.pathologicalState,
    'pathologicalState_Updated': f.pathological_state,
  }
]

materialTypeMatches['match'] = dt.Frame([
  ( row[0] == row[1] ) if (row[0] is not None) and (row[1] is not None) else None
  for row in materialTypeMatches[:, (f.materialType_Original, f.materialType_Updated)].to_tuples()
])

materialTypeMatches \
  .to_pandas() \
  .replace({ np.nan: None }) \
  .to_excel('data/novelomics_material_type_discrepancies.xlsx',index=False)
  
del matchingSamples['materialType2']

# dt.unique(matchingSamples['pathologicalState'])
# matchingSamples[:, dt.count(), dt.by(f.partOfRelease, f.pathologicalState)]

# ~ 1c ~
# Update batch

batchData = data[:, (f.sampleID, f.batch)]
batchData.key = 'sampleID'
matchingSamples.key = 'sampleID'
matchingSamples = matchingSamples[:, :, dt.join(batchData)]

matchingSamples['batch'] = dt.Frame([
  ','.join(row) if not (row[1] in row[0]) else row[0]
  for row in matchingSamples[:, ['batch', 'batch.0']].to_tuples()
])

del matchingSamples['batch.0']

# ~ 1d ~
# Update alternativeIdentifiers
dt.unique(matchingSamples['alternativeIdentifier'])
del matchingSamples['alternativeIdentifier']

altIDs = data[:, (f.sampleID, f.external_sample_ID)]
altIDs.names = { 'external_sample_ID': 'alternativeIdentifier' }
altIDs.key = 'sampleID'
matchingSamples.key = 'sampleID'

matchingSamples = matchingSamples[:, :, dt.join(altIDs)]


# ~ 1d ~
# Add extractedProtocol based on tissue type
# If the material is "Tissue (*)"", the extracted protocol should receive the
# the content in the parentheses (e.g., 'FFPE', 'FROZEN', etc.). If the material
# is different than the what already exists in RD3, then the material type should
# remain as existing value.
extractedProtocol = data[:, (f.sampleID, f.materialType)]
extractedProtocol['extractedProtocol'] = dt.Frame([
  getWrappedValues(value)
  if value is not None else value
  for value in extractedProtocol['materialType'].to_list()[0]
])

del extractedProtocol['materialType']
extractedProtocol.key = 'sampleID'
matchingSamples.key = 'sampleID'

matchingSamples=matchingSamples[:, :, dt.join(extractedProtocol)]


#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Import data

# importData = matchingSamples[:, (f.sampleID, f.tissueType)] \
# importData = matchingSamples[:, (f.sampleID, f.alternativeIdentifier)] \
importData = matchingSamples[:, (f.sampleID, f.extractedProtocol)] \
  .to_pandas() \
  .replace({ np.nan: None }) \
  .to_dict('records')

for row in tqdm(importData):
  print(f"Updating {row['sampleID']}....")
  response=rd3.update_one(
    entity = 'solverd_samples',
    id_=row['sampleID'],
    attr="extractedProtocol",
    value = row['extractedProtocol']
  )