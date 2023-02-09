#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_views_overview.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-05-16
#' MODIFIED: 2023-02-09
#' PURPOSE: generate dataset for rd3_overview
#' STATUS: stable
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from dotenv import load_dotenv
from os import environ
import sys
load_dotenv()
sys.path.append(environ['SYS_PATH'])

from rd3.api.molgenis2 import Molgenis
from rd3.utils.utils import statusMsg, flattenDataset
from datatable import dt, f, as_type
import re

def uniqueValuesById(data, groupby, column, dropDuplicates=True, keyGroupBy=True):
  """Unique Values By Id
  For a datatable object, collapse all unique values by ID into a comma
  separated string.

  @param data datatable object
  @param groupby name of the column that will serve as the grouping variable
  @param column name of the column that contains the values to collapse
  @param dropDuplicates If True, all duplicate rows will be removed
  @param keyGroupBy If True, returned object will be keyed using the value named in groupby
  
  @return datatable object
  """
  df = data.to_pandas()
  df[column] = df.dropna(subset=[column]) \
    .groupby(groupby)[column] \
    .transform(lambda val: ','.join(set(map(str, val))))
  if dropDuplicates:
    df = df[[groupby, column]].drop_duplicates()
  output = dt.Frame(df)
  if keyGroupBy:
    output.key = groupby
  return output

#///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Fetch RD3 Data

if environ['JOB_ENV'] == 'PROD':
  statusMsg('Connecting to RD3-PROD....')
  rd3=Molgenis(url=environ['MOLGENIS_PROD_HOST'])
  rd3.login(environ['MOLGENIS_PROD_USR'],environ['MOLGENIS_PROD_PWD'])
else:
  statusMsg('Connecting to RD3-ACC....')
  rd3=Molgenis(environ['MOLGENIS_ACC_HOST'])
  rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])


# ~ 0a ~
# Pull All Subject Metadata
statusMsg('Pulling subject metadata....')

columns=','.join([
  'subjectID', 'sex1', 'fid', 'mid', 'pid', 'clinical_status', 'solved',
  'disease', 'phenotype', 'hasNotPhenotype', 'organisation', 'ERN', 'partOfRelease'
])

rawsubjects = rd3.get('solverd_subjects',attributes=columns,batch_size=10000)
subjectsflattened = flattenDataset(rawsubjects, columnPatterns='subjectID|id|value')
subjects=dt.Frame(subjectsflattened)

#///////////////////////////////////////

# ~ 1b ~
# fetch sample metadata
statusMsg('Pulling sample metadata....')
rawsamples = rd3.get(
  'solverd_samples',
  batch_size=10000,
  attributes='sampleID,belongsToSubject',
  q = 'retracted==N'
)

samplesflattened = flattenDataset(rawsamples,columnPatterns='subjectID')
samples = dt.Frame(samplesflattened)


# summarize sampleIDs by subjectID
statusMsg('Summarising sample metadata by subject....')
samplesBySubject = uniqueValuesById(
  data = samples[:, (f.belongsToSubject, f.sampleID)],
  groupby='belongsToSubject',
  column ='sampleID'
)

# summarize number of samples per subject
statusMsg('Preparing sample data for join....')

sampleCountsBySubject = samples[:, dt.count(), dt.by(f.belongsToSubject)]
sampleCountsBySubject.key = 'belongsToSubject'

samplesBySubject = samplesBySubject[:, :, dt.join(sampleCountsBySubject)]
samplesBySubject.names = {
  'belongsToSubject': 'subjectID',
  'sampleID': 'samples',
  'count': 'numberOfSamples'
}

# join data
statusMsg('Joining sample metadata with subjects....')
subjects.key = 'subjectID'
samplesBySubject.key = 'subjectID'
subjects = subjects[:, :, dt.join(samplesBySubject)]

#///////////////////////////////////////

# ~ 0c ~    
# fetch experiment metadata
statusMsg('Pulling experiment metadata....')
rawexperiments = rd3.get(
  'solverd_labinfo',
  batch_size=10000,
  attributes = 'experimentID,sampleID',
  q='retracted==N'
)

experimentsflattened = flattenDataset(rawexperiments,columnPatterns='sampleID')
experiments=dt.Frame(rawexperiments)

# join subject ID
statusMsg('Joining experiment data with samples....')
experiments.key = 'sampleID'
samples.key = 'sampleID'
experiments = experiments[:, :, dt.join(samples)]

# summarize data
statusMsg('Summarizing experiment data by subject and generating counts....')
experimentCountsBySubject = experiments[:, dt.count(f.experimentID), dt.by(f.belongsToSubject)]
experimentCountsBySubject.names = {'experimentID': 'numberOfExperiments'}

# summarize samples by subject
experimentsBySubject = uniqueValuesById(
  data = experiments[:, (f.belongsToSubject, f.experimentID)],
  groupby = 'belongsToSubject',
  column = 'experimentID'
)

# join counts with experiment summary
experimentsBySubject.key = 'belongsToSubject'
experimentCountsBySubject.key = 'belongsToSubject'
experimentsBySubject = experimentsBySubject[:, :, dt.join(experimentCountsBySubject)]

# join with main dataset
statusMsg('Joining experiment data with subjects....')
experimentsBySubject.names = {'experimentID': 'experiments', 'belongsToSubject': 'subjectID'}
experimentsBySubject.key = 'subjectID'
subjects.key = 'subjectID'

subjects = subjects[:, :, dt.join(experimentsBySubject)]

#///////////////////////////////////////

# ~ 0d ~
# Add File query
# Since the files table is quite large and it takes a while to pull the data,
# it is better to make a query URL that redirects to files tables.

# get file metadata -- subjects only
statusMsg('Getting file metadata.....')
rawfiles = rd3.get(
  entity='solverd_files',
  attributes='subjectID',
  batch_size=10000
)

files = dt.Frame(flattenDataset(rawfiles,columnPatterns="subjectID"))
files = dt.unique(files[f.subjectID!=None,'subjectID'])
fileSubjectIDs =files['subjectID'].to_list()[0]

statusMsg('Create redirect for files table....')
subjects['files'] = dt.Frame([
  f'?entity=solverd_files&hideselect=true&mod=data&filter=subjectID=={id}'
  if id in fileSubjectIDs else None
  for id in subjects['subjectID'].to_list()[0]
])

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Import Data
statusMsg('Detecting novel omics only samples....')

# compute 'hasOnlyNovelOmics' stauts
subjects['hasOnlyNovelOmics'] = dt.Frame([
  not bool(re.search('freeze', d))
  for d in subjects['partOfRelease'].to_list()[0]
])

# prep columns
statusMsg('Renaming columns....')
subjects[
  :, dt.update(
    hasOnlyNovelOmics = as_type(f.hasOnlyNovelOmics, str),
    clinical_status = as_type(f.clinical_status, str),
    numberOfSamples = as_type(f.numberOfSamples, str),
    numberOfExperiments = as_type(f.numberOfExperiments, str),
)]

# import
statusMsg('Importing overview dataset into RD3.....')
rd3.importDatatableAsCsv('solverd_overview', data = subjects)
