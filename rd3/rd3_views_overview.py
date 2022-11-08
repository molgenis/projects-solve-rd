#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_views_overview.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-05-16
#' MODIFIED: 2022-11-08
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

# rd3=Molgenis(environ['MOLGENIS_ACC_HOST'])
# rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

rd3=Molgenis(url=environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'],environ['MOLGENIS_PROD_PWD'])

# ~ 0a ~
# Pull All Subject Metadata
rawsubjects = rd3.get(
  'solverd_subjects',
  batch_size=10000,
  attributes=','.join([
    'subjectID', 'sex1', 'fid', 'mid', 'pid', 'clinical_status', 'solved',
    'disease', 'phenotype', 'hasNotPhenotype',
    'organisation', 'ERN', 'partOfRelease'
  ])
)

# flatten ref objects in dataset
for row in rawsubjects:
  # flatten sex1
  if 'sex1' in row:
    if row['sex1'] is not None:
      row['sex1'] = row['sex1']['id']
    else:
      row['sex1'] = None
      
  # flatten mid
  if 'mid' in row:
    if row['mid'] is not None:
      row['mid'] = row['mid']['subjectID']
    else:
      row['mid'] = None

  # flatten paternal ID
  if 'pid' in row:
    if row['pid'] is not None:
      row['pid'] = row['pid']['subjectID']
    else:
      row['pid'] = None

  # flatten phenotype
  if 'phenotype' in row:
    if not bool(row['phenotype']):
      row['phenotype'] = None
    elif not isinstance(row['phenotype'], str):
      row['phenotype'] = ','.join([ r['id'] for r in row['phenotype'] ])
    else:
      row['phenotype'] = row['phenotype']
    
  # flatten hasNotPhentype
  if 'hasNotPhenotype' in row:
    if not bool(row['hasNotPhenotype']):
      row['hasNotPhenotype'] = None
    elif not (isinstance(row['hasNotPhenotype'], str)):
      row['hasNotPhenotype'] = ','.join([ r['id'] for r in row['hasNotPhenotype'] ])
    else:
      row['hasNotPhenotype'] = row['hasNotPhenotype']

  # flatten disease codes
  if 'disease' in row:
    if bool(row['disease']):
      row['disease'] = ','.join([ nestedrow['id'] for nestedrow in row['disease'] ])
    else:
      row['disease'] = None

  # flatten organisations
  if 'organisation' in row:
    if bool(row['organisation']):
      row['organisation'] = ','.join([ r['value'] for r in row['organisation'] ])
    else:
      row['organisation'] = None

  # flatten ERNs
  if 'ERN' in row:
    if bool(row['ERN']):
      row['ERN'] = ','.join([ r['id'] for r in row['ERN'] ])
    else:
      row['ERN'] = None
      
  # collapse release rows
  row['partOfRelease'] = ','.join([ r['id'] for r in row['partOfRelease'] ])

subjects=dt.Frame(rawsubjects)
del subjects['_href']

#///////////////////////////////////////

# ~ 1b ~
# fetch sample metadata

rawsamples = rd3.get(
  'solverd_samples',
  batch_size=10000,
  attributes='sampleID,belongsToSubject',
  q = 'retracted==N'
)

# prep sample metadata
for row in rawsamples:
  row['belongsToSubject']=row.get('belongsToSubject',{}).get('subjectID')

samples=dt.Frame(rawsamples)
del samples['_href']

# summarize sampleIDs by subjectID
samplesBySubject = uniqueValuesById(
  data = samples[:, (f.belongsToSubject, f.sampleID)],
  groupby='belongsToSubject',
  column ='sampleID'
)

# summarize number of samples per subject
sampleCountsBySubject = samples[:, dt.count(), dt.by(f.belongsToSubject)]
sampleCountsBySubject.key = 'belongsToSubject'

samplesBySubject = samplesBySubject[:, :, dt.join(sampleCountsBySubject)]
samplesBySubject.names = {
  'belongsToSubject': 'subjectID',
  'sampleID': 'samples',
  'count': 'numberOfSamples'
}

# join data
subjects.key = 'subjectID'
samplesBySubject.key = 'subjectID'
subjects = subjects[:, :, dt.join(samplesBySubject)]

#///////////////////////////////////////

# ~ 0c ~    
# fetch experiment metadata

rawexperiments = rd3.get(
  'solverd_labinfo',
  batch_size=10000,
  attributes = 'experimentID,sampleID',
  q='retracted==N'
)

# prep experiment data
for row in rawexperiments:
  row['sampleID']=row.get('sampleID',{})[0].get('sampleID')

experiments=dt.Frame(rawexperiments)
del experiments['_href']

# join subject ID
experiments.key = 'sampleID'
samples.key = 'sampleID'
experiments = experiments[:, :, dt.join(samples)]

# summarize data
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
experimentsBySubject.names = {'experimentID': 'experiments', 'belongsToSubject': 'subjectID'}
experimentsBySubject.key = 'subjectID'
subjects.key = 'subjectID'

subjects = subjects[:, :, dt.join(experimentsBySubject)]

#///////////////////////////////////////

# ~ 0d ~
# Add File query
# Since the files table is quite large and it takes a while to pull the data,
# it is better to make a query URL that redirects to files tables.
# subjects['files'] = dt.Frame([
#    	f"?entity=solverd_files&hideselect=true&filter=(subjectID%3Dq%3D{id})"
#     for id in subjects['subjectID'].to_list()[0]
# ])

subjects['files'] = None

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Import Data

# compute 'hasOnlyNovelOmics' stauts
subjects['hasOnlyNovelOmics'] = dt.Frame([
  not bool(re.search('freeze', d))
  for d in subjects['partOfRelease'].to_list()[0]
])

# prep columns
subjects[
  :, dt.update(
    hasOnlyNovelOmics = as_type(f.hasOnlyNovelOmics, str),
    clinical_status = as_type(f.clinical_status, str),
    numberOfSamples = as_type(f.numberOfSamples, str),
    numberOfExperiments = as_type(f.numberOfExperiments, str),
)]

# import
rd3.importDatatableAsCsv('solverd_overview', data = subjects)

# fileTypeQuery="typeFile=in=(bai,bam,bed,cram,fastq,vcf)" # remove json and ped for now
# rawfiles=rd3.get(
#   'solverd_files',
#   attributes='EGA,sampleID,experimentID,subjectID'
# )

# for row in rawfiles:
#   if row.get('samples'):
#     row['samples']=row.get('samples')[0].get('id')
#   else:
#     row['samples']=None

#   if row.get('subjectID'):
#     row['subjectID'] = row.get('subjectID')[0].get('id')
#   else:
#     row['subjectID'] = None
    
#   if row.get('typeFile'):
#     row['typeFile']=row.get('typeFile',{}).get('identifier')
#   else:
#     row['typeFile']=None

# files=dt.Frame(rawfiles)
# del files['_href']
