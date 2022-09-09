#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_views_overview.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-05-16
#' MODIFIED: 2022-09-09
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
from rd3.utils.utils import statusMsg, createUrlFilter
from datatable import dt, f, as_type
from tqdm import tqdm
import pandas as pd
import urllib
import re

def uniqueValuesById(data, groupby, column, keyGroupBy=True):
  """Unique Values By Id
  For a datatable object, collapse all unique values by ID into a comma
  separated string.

  @param data datatable object
  @param groupby name of the column that will serve as the grouping variable
  @param column name of the column that contains the values to collapse
  
  @param datatable object
  """
  df = data.to_pandas()
  df[column] = df.groupby(groupby)[column].transform(lambda val: ','.join(set(val)))
  df = df[[groupby, column]].drop_duplicates()
  output = dt.Frame(df)
  if keyGroupBy: output.key = groupby
  return output

#///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Fetch RD3 Data

# rd3=Molgenis(environ['MOLGENIS_ACC_HOST'])
# rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

rd3=Molgenis(url=environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'],environ['MOLGENIS_PROD_PWD'])

# If new releases are added to RD3, add package identifier to the list below.
# Since the aim of the table is to be able to identify subjects that are
# lacking genetic data, it is best to separate novelomics releases from 
# regular freezes, and then merge the two arrays

availableReleases = [
 # regular
  'freeze1',
  'freeze2',
  'freeze3',
 
 # novel omics 
  'noveldeepwes',
  'novelrnaseq',
  'novellrwgs',
  'novelsrwgs',
  'novelwgs'
]

#///////////////////////////////////////

# ~ 0a ~
# Pull All Subject Metadata

# set subject attributes of interest for the view
subjectTableAttributes = ','.join([
  'id', 'subjectID',
  'sex1',
  'fid', 'mid', 'pid',
  'clinical_status', 'disease', 'phenotype', 'hasNotPhenotype',
  'organisation', 'ERN',
  'solved',
  'patch'
])

# fetch subject metadata for all available releases
statusMsg('Fetching all subject metadata....')

subjects=[]
for release in tqdm(availableReleases):
  pkgEntity = f"rd3_{release}_subject"
  data=rd3.get(pkgEntity, batch_size=10000, attributes=subjectTableAttributes)
  for row in data:
    row['sex1']=row.get('sex1',{}).get('identifier')
    row['mid']=row.get('mid',{}).get('id')
    row['pid']=row.get('pid',{}).get('id')
    if row.get('disease'):
      row['disease']=','.join([record['id'] for record in row['disease']])
    else:
      row['disease']=None
    if row.get('phenotype'):
      row['phenotype']=','.join([record['id'] for record in row['phenotype']])
    else:
      row['phenotype']=None
    if row.get('hasNotPhenotype'):
      row['hasNotPhenotype']=','.join([record['id'] for record in row['hasNotPhenotype']])
    else:
      row['hasNotPhenotype']=None
    row['organisation']=row.get('organisation',{}).get('identifier')
    row['ERN']=row.get('ERN',{}).get('identifier')
    row['patch']=','.join([record['id'] for record in row['patch']])
    row['release']=release
  subjects.extend(data)

subjects=dt.Frame(subjects)
del subjects['_href']
del subjectTableAttributes

#///////////////////////////////////////

# ~ 0b ~
# fetch sample metadata

# set attributes of interest
sampleAttribs=','.join(['id', 'sampleID', 'subject'])
sampleQuery="retracted==N"

# pull sample metadata
statusMsg('Fetching all sample metadata....')

samples=[]
for release in tqdm(availableReleases):
  pkgEntity=f"rd3_{release}_sample"
  data=rd3.get(pkgEntity, batch_size=10000, attributes=sampleAttribs, q=sampleQuery)
  for row in data:        
    row['subject']=row.get('subject',{}).get('id')
    row['release']=release
  samples.extend(data)
  
samples=dt.Frame(samples)
del samples['_href']
del sampleAttribs, sampleQuery
  
#///////////////////////////////////////

# ~ 0c ~    
# fetch experiment metadata

# set attributes of interest
labinfoAttribs = ','.join(['id', 'experimentID', 'sample'])

# retrieve labinfo metadata
statusMsg('Fetching all labinfo metadata....')

experiments=[]
for release in tqdm(availableReleases):
  pkgEntity = f"rd3_{release}_labinfo"
  data=rd3.get(pkgEntity, batch_size=10000, attributes=labinfoAttribs)
  for row in data:
    row['sample']=row.get('sample',{})[0].get('id')
    row['release']=release
  experiments.extend(data)
  
experiments=dt.Frame(experiments)
del experiments['_href']
del labinfoAttribs

#///////////////////////////////////////
    
# ~ 0d ~
# fetch file metadata

# set attributes of interest
fileAttribs = ','.join(['EGA','samples', 'subjectID', 'experimentID','typeFile'])
# fileTypeQuery="typeFile=in=(bai,bam,bed,cram,fastq,vcf)" # remove json and ped for now

# fetch data
statusMsg('Fetching all file metadata....')

files=[]
for release in tqdm(availableReleases):
  pkgEntity=f"rd3_{release}_file"
  data=rd3.get(pkgEntity, batch_size=1000, attributes=fileAttribs)
  for row in data:
    if row.get('samples'):
      row['samples']=row.get('samples')[0].get('id')
    else:
      row['samples']=None

    if row.get('subjectID'):
      row['subjectID'] = row.get('subjectID')[0].get('id')
    else:
      row['subjectID'] = None
      
    if row.get('typeFile'):
      row['typeFile']=row.get('typeFile',{}).get('identifier')
    else:
      row['typeFile']=None

    row['release']=release
  files.extend(data)

files=dt.Frame(files)
del files['_href']
del fileAttribs

#//////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Build Subject Metadata
# get unique subjects only: this will be the main object that we will be
# working with
subjectsDT = dt.unique(subjects['id'])

# ~ 1a ~
# Get Unique sex values by subject
statusMsg('Collapsing sex codes....')

subjectSexValues = uniqueValuesById(
  data = subjects[(f.sex1 != 'NA') & (f.sex1 != None), (f.id, f.sex1)],
  groupby = 'id',
  column = 'sex1'
)

subjectsDT.key = 'id'
subjectsDT = subjectsDT[:, :, dt.join(subjectSexValues)]
del subjectSexValues

# ~ 1b ~
# Collapse Family Identifier
# there are instances where there are multiple rows per subject. Family IDs
# need to be collapsed by ID. (handover to pandas)
statusMsg('Collapsing family identifiers....')

familyIDs = uniqueValuesById(
  data = subjects[(f.fid != 'NA') & (f.fid != None), (f.id, f.fid)],
  groupby = 'id',
  column = 'fid'
)

subjectsDT.key = 'id'
subjectsDT = subjectsDT[:, :, dt.join(familyIDs)]
del familyIDs

#///////////////////////////////////////

# ~ 1c ~
# Collapse Parental Identifiers
# There are multiple rows per subject, but they are always the same mother and
# father. There are a few edge cases where there are different identifiers, but
# it is likely that the values were updated or changed. Keep them for now. 
statusMsg('Collapsing maternal identifiers....')

maternalIDs = uniqueValuesById(
  data = subjects[(f.mid != None) & (f.mid != 'NA'), (f.id, f.mid)],
  groupby = 'id',
  column = 'mid'
)

subjectsDT.key = 'id'
subjectsDT = subjectsDT[:, :, dt.join(maternalIDs)]
del maternalIDs

paternalIDs = uniqueValuesById(
  data = subjects[(f.pid != None) & (f.pid != 'NA'), (f.id, f.pid)],
  groupby = 'id',
  column = 'pid'
)

subjectsDT.key = 'id'
subjectsDT = subjectsDT[:, :, dt.join(paternalIDs)]
del paternalIDs

#///////////////////////////////////////

# ~ 1d ~
# Collapse Clinical Status
statusMsg('Collapsing clinical status.....')

clinicalStatuses = subjects[(f.clinical_status != None), (f.id, f.clinical_status)]
clinicalStatuses[:, dt.update(clinical_status = as_type(f.clinical_status, dt.Type.str32))]
clinicalStatuses = uniqueValuesById(
  data = clinicalStatuses,
  groupby = 'id',
  column = 'clinical_status'
)

clinicalStatuses[:, dt.update(clinical_status = as_type(f.clinical_status, bool))]

subjectsDT.key = 'id'
subjectsDT = subjectsDT[:, :, dt.join(clinicalStatuses)]
del clinicalStatuses

#///////////////////////////////////////

# ~ 1e ~
# Collapse Disease Codes
statusMsg('Collapsing disease codes....')

diseasesCodesBySubject = uniqueValuesById(
  data = subjects[(f.disease != 'NA') & (f.disease != None), (f.id, f.disease)],
  groupby = 'id',
  column = 'disease'
)

# remove any duplicate codes within a string
diseasesCodesBySubject['disease'] = dt.Frame([
  ','.join(list(set(d.split(','))))
  for d in diseasesCodesBySubject['disease'].to_list()[0]
])

subjectsDT.key = 'id'
subjectsDT = subjectsDT[:, :, dt.join(diseasesCodesBySubject)]

del diseasesCodesBySubject

#///////////////////////////////////////

# ~ 1f ~
# Collapse HPO columns: observed and unobserved

statusMsg('Collapsing observed phenotype codes.....')

observedHpoBySubject = uniqueValuesById(
  data = subjects[(f.phenotype != None), (f.id, f.phenotype)],
  groupby = 'id',
  column = 'phenotype'
)

observedHpoBySubject['phenotype'] = dt.Frame([
  ','.join(list(set(d.split(','))))
  for d in observedHpoBySubject['phenotype'].to_list()[0]
])

subjectsDT.key = 'id'
subjectsDT = subjectsDT[:, :, dt.join(observedHpoBySubject)]

del observedHpoBySubject

statusMsg('Collapsing unobserved HPO codes.....')

unobservedHpoBySubject = uniqueValuesById(
  data = subjects[(f.hasNotPhenotype != None), (f.id, f.hasNotPhenotype)],
  groupby = 'id',
  column = 'hasNotPhenotype'
)

unobservedHpoBySubject['hasNotPhenotype'] = dt.Frame([
  ','.join(list(set(d.split(','))))
  for d in unobservedHpoBySubject['hasNotPhenotype'].to_list()[0]
])

subjectsDT.key = 'id'
subjectsDT = subjectsDT[:, :, dt.join(unobservedHpoBySubject)]

del unobservedHpoBySubject

#///////////////////////////////////////

# ~ 1g ~
# Collapse Organisations

organisationsBySubject = uniqueValuesById(
  data = subjects[(f.organisation != None) & (f.organisation != 'NA'), (f.id, f.organisation)],
  groupby = 'id',
  column = 'organisation'
)

subjectsDT.key = 'id'
subjectsDT = subjectsDT[:, :, dt.join(organisationsBySubject)]

del organisationsBySubject

#///////////////////////////////////////

# ~ 1h ~
# Collapse ERNs
statusMsg('Collapsing ERN....')

ernsBySubject = uniqueValuesById(
  data = subjects[(f.ERN != None) & (f.ERN != 'NA'), (f.id, f.ERN)],
  groupby = 'id',
  column = 'ERN'
)

subjectsDT.key = 'id'
subjectsDT = subjectsDT[:, :, dt.join(ernsBySubject)]

del ernsBySubject

#///////////////////////////////////////

# ~ 1i ~
# Collapse Solved Status
# Since solved status is a bool value, we need to convert to string before 
# the data is passed to pandas. After the data is summarized, change the
# data type back to bool.

statusMsg('Collapsing solved status....')

solvedStatusBySubject = subjects[(f.solved != None), (f.id, f.solved)]
solvedStatusBySubject[:, dt.update(solved = as_type(f.solved, str))]

solvedStatusBySubject = uniqueValuesById(
  data = solvedStatusBySubject,
  groupby = 'id',
  column = 'solved'
)

solvedStatusBySubject[:, dt.update(solved = as_type(f.solved, bool))]

subjectsDT.key = 'id'
subjectsDT = subjectsDT[:, :, dt.join(solvedStatusBySubject)]

del solvedStatusBySubject

#///////////////////////////////////////

# ~ 1j ~
# Collapse Patch
statusMsg('Collapsing subject releases.....')

releasesBySubject = uniqueValuesById(
  data = subjects[(f.patch != None) & (f.patch != 'NA'), (f.id, f.patch)],
  groupby = 'id',
  column = 'patch'
)

releasesBySubject['patch'] = dt.Frame([
  ','.join(list(set(d.split(','))))
  for d in releasesBySubject['patch'].to_list()[0]
])

subjectsDT.key = 'id'
subjectsDT = subjectsDT[:, :, dt.join(releasesBySubject)]

del releasesBySubject

statusMsg('Identifying subjects that exist only in a novel omics release....')
subjectsDT['hasOnlyNovelOmics'] = dt.Frame([
  not bool(re.search('freeze', d))
  for d in subjectsDT['patch'].to_list()[0]
])

#///////////////////////////////////////

# ~ ik ~
# Determining EMX Releases by subject

emxReleases = uniqueValuesById(
  data = subjects[(f.release != None), (f.id, f.release)],
  groupby = 'id',
  column = 'release'
)

emxReleases['release'] = dt.Frame([
  ','.join(list(set(d.split(','))))
  for d in emxReleases['release'].to_list()[0]
])

emxReleases.names = {'release': 'emxReleases'}

subjectsDT.key = 'id'
subjectsDT = subjectsDT[:, :, dt.join(emxReleases)]

#//////////////////////////////////////////////////////////////////////////////

# ~ 2 ~ 
# RESHAPE SAMPLES
# Sample metadata will need to be processed a bit differently than subject
# metadata. The idea is to have all samples listed horizontally by subject.
# This means that for each subject there will be a column for all samples
# released in DF1, DF2, DF3, and so on. It was done this way since so that
# references to other tables can be made.
statusMsg('Summarizing sample metadata....')

# Prepare samples dataset
# Make sure all rows without a subject id are removed
# remove sampleID column since it isn't necessary during the processing
samples = samples[f.subject != None, :]
del samples['sampleID']

# ~ 2a ~
# convert to pandas and widen dataset samples are spread by release
statusMsg('Spreading sample dataset to wide by subject and release....')
samplesPD = samples.to_pandas()
samplesWidenedBySubject = pd.pivot_table(
  samplesPD,
  index = 'subject',
  columns = 'release',
  values = 'id',
  aggfunc = ','.join
).reset_index()

# convert back to datatble object
samplesBySubject = dt.Frame(samplesWidenedBySubject)

# rename columns
samplesBySubject.names={
  'subject': 'id',
  'freeze1': 'df1Samples',
  'freeze2': 'df2Samples',
  'freeze3': 'df3Samples',
  'noveldeepwes': 'noveldeepwesSamples',
  'novelrnaseq': 'novelrnaseqSamples',
  'novellrwgs': 'novellrwgsSamples',
  'novelsrwgs': 'novelsrwgsSamples',
  'novelwgs': 'novelwgsSamples',
}

# join with subjects data
statusMsg('Joining sample data with subjects....')
samplesBySubject.key = 'id'

subjectsDT = subjectsDT[:, :, dt.join(samplesBySubject)]

del samplesBySubject

#///////////////////////////////////////

# ~ 2b ~
# Calculate number of samples by subject and join
sampleCountsBySubject = samples[:, dt.count(f.id), dt.by(f.subject)]

sampleCountsBySubject.names = {'id': 'numberOfSamples', 'subject': 'id'}
sampleCountsBySubject.key = 'id'

subjectsDT = subjectsDT[:, :, dt.join(sampleCountsBySubject)]

del sampleCountsBySubject

#//////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# RESHAPE EXPERIMENTS
# Like sample identifiers, experiment IDs need to be collapsed by subject and 
# release before spreading to wide format. This means that for each subjectID
# collapse experiment IDs by release, drop duplicate rows, and spread. Unlike
# the sample metadata, subject identifiers aren't readily available in the
# data. IDs can be joined using sample ID.
statusMsg('Summarizing experiment metadata....')


# ~ 3a ~
# Prepare Experiment Dataset
# join subjectID via samples dataset
sampleSubjectIDs = samples[:, ['id','subject']]
sampleSubjectIDs.names = {
  'id': 'sample',
  'subject': 'subjectID'
}

########### START TEMPORARY WORKAROUND ###########
# There are a few samples that are linked to two different subjects. Until
# these cases are resolved, identify them and remove them from the
# experiments dataset
statusMsg('Running workaround for duplicate sample-subject link....')

samplesSubjectsFlattened = uniqueValuesById(
  data = sampleSubjectIDs,
  groupby = 'sample',
  column = 'subjectID'
)

# build pattern
idsToRemove = samplesSubjectsFlattened[
  dt.re.match(f.subjectID, '.*,.*'), 'sample'
].to_list()[0]

pattern = f".*({'|'.join(idsToRemove)}).*"

sampleSubjectIDs = samplesSubjectsFlattened[dt.re.match(f.sample, pattern) == False, :]

############ END TEMPORARY WORKAROUND ############

# join subject identifiers to experiments
sampleSubjectIDs.key = 'sample'
experiments = experiments[:, :, dt.join(sampleSubjectIDs)]

del experiments['experimentID']
del sampleSubjectIDs
del samplesSubjectsFlattened
del idsToRemove, pattern

#///////////////////////////////////////

# ~ 3b ~
# spread data by subject and release
experimentsPD = experiments.to_pandas()
experimentsWidenedBySubject = pd.pivot_table(
  experimentsPD,
  index = 'subjectID',
  columns = 'release',
  values = 'id',
  aggfunc = ','.join
).reset_index()


# convert back to datatable object
experimentsBySubject = dt.Frame(experimentsWidenedBySubject)

# rename columns (uncomment the rest when data is available)
experimentsBySubject.names = {
  'subjectID': 'id',
  'freeze1': 'df1Experiments',
  'freeze2': 'df2Experiments',
  'freeze3': 'df3Experiments',
  # 'noveldeepwes': 'noveldeepwesExperiments',
  # 'novelrnaseq': 'novelrnaseqExperiments',
  # 'novellrwgs': 'novellrwgsExperiments',
  'novelsrwgs': 'novelsrwgsExperiments',
  'novelwgs': 'novelwgsExperiments',
}

experimentsBySubject.key = 'id'
subjectsDT.key = 'id'

subjectsDT = subjectsDT[:, :, dt.join(experimentsBySubject)]

del experimentsBySubject

#///////////////////////////////////////

# ~ 3c ~
# Calcuate the number of experiments per subject

statusMsg('Calculating experiment counts by subject....')
experimentCountsBySubject = experiments[f.subjectID != None, :][
  :, dt.count(f.id), dt.by(f.subjectID)
][
  :, {'id': f.subjectID, 'numberOfExperiments': f.id}
]

experimentCountsBySubject.key = 'id'
subjectsDT.key = 'id'

subjectsDT = subjectsDT[:, :, dt.join(experimentCountsBySubject)]

del experimentCountsBySubject

#//////////////////////////////////////////////////////////////////////////////

# ~ 4 ~
# RESHAPE FILES
# Since I have written another script that merges subject, sample, and experiment
# IDs into the file tables, we can go ahead and summarize the files at the
# subject level. We can ignore sample and experiment IDs here.
statusMsg('Summarizing file metadata....')


# ~ 4a ~
# Spread data by subject and release

filesBySubject = files[
  f.subjectID != None, {'id': f.subjectID, 'release': f.release}
]

# create query column
filesBySubject['query'] = dt.Frame([
  '?entity=rd3_{}_file&hideselect=true&filter=({})'.format(
    tuple[1],
    urllib.parse.quote(
      createUrlFilter('subjectID', [tuple[0]])
    )
  )
  for tuple in filesBySubject[:,['id', 'release']].to_tuples()
])

# widen dataset
filesBySubjectPD = filesBySubject.to_pandas()
filesBySubjectPD = filesBySubjectPD[['id','release','query']].drop_duplicates()

filesBySubjectWidened = pd.pivot_table(
  filesBySubjectPD,
  index = 'id',
  columns = 'release',
  values = 'query',
  aggfunc = ','.join
).reset_index()

# change to datatable object and rename columns
filesBySubject = dt.Frame(filesBySubjectWidened)

filesBySubject.names = {
  'freeze1': 'df1Files',
  'freeze2': 'df2Files',
  # 'freeze3': 'df3Files',
  # 'noveldeepwes': 'noveldeepwesFiles',
  # 'novelrnaseq': 'novelrnaseqFiles',
  # 'novellrwgs': 'novellrwgsFiles',
  'novelsrwgs': 'novelsrwgsFiles',
  # 'novelwgs': 'novelwgsFiles',
}

# join with main dataset
filesBySubject.key = 'id'
subjectsDT.key = 'id'

subjectsDT = subjectsDT[:, :, dt.join(filesBySubject)]

del filesBySubject
del filesBySubjectPD
del filesBySubjectWidened

#///////////////////////////////////////

# ~ 4b ~
# Summarize file counts by Subject
statusMsg('Calculating file counts by subject....')

fileCountsBySubject = files[
  f.subjectID != None, :
][
  :, dt.count(f.EGA), dt.by(f.subjectID)
][
  :, {'id': f.subjectID, 'numberOfFiles': f.EGA}
]

fileCountsBySubject.key = 'id'
subjectsDT.key = 'id'

subjectsDT = subjectsDT[:, :, dt.join(fileCountsBySubject)]

del fileCountsBySubject

#///////////////////////////////////////////////////////////////////////////////

# ~ 5 ~
# Import Data
statusMsg('Importing data....')


# clean up variables
subjectsDT['id'] = dt.Frame([
  d.split('_')[0]
  for d in subjectsDT['id'].to_list()[0]
])

subjectsDT['mid'] = dt.Frame([
  d.split('_')[0]
  if d else d
  for d in subjectsDT['mid'].to_list()[0]
])

subjectsDT['pid'] = dt.Frame([
  d.split('_')[0]
  if d else d
  for d in subjectsDT['pid'].to_list()[0]
])

# rename columns
subjectsDT.names = {'id': 'subjectID'}
# rd3.importDatatableAsCsv(pkg_entity='rd3_overview', data=subjectsDT)
subjectsDT.to_csv('data/rd3_overview.csv')
