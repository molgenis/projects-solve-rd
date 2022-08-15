#//////////////////////////////////////////////////////////////////////////////
# FILE: data_freeze_experiment_file.py
# AUTHOR: David Ruvolo
# CREATED: 2022-08-10
# MODIFIED: 2022-08-15
# PURPOSE: fill in missing sampleIDs, experimentIDs, and add subjectIDs
# STATUS: stable
# PACKAGES: **see below**
# COMMENTS: The purpose of this script is to refresh and fill in missing
# file to sample, subject, or experiment links. As more files are registered in
# RD3, this information may not be available at the time of import or regularly
# updated.
#//////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis import Molgenis
from rd3.utils.utils import dtFrameToRecords
from datatable import dt, f
from datatable.re import match
from dotenv import load_dotenv
from tqdm import tqdm
from os import environ
from os import path
load_dotenv()

# set RD3 release
currentRelease = 'freeze2'

# connect to RD3
rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

def uniqueValuesToString(data, searchAttr, dataAttr, value, usePattern=False):
  """Unique Values to String
  Filter a datatable object, get unique values, and return as a string

  @param data datatable object
  @param searchAttr column to search in
  @param dataAttr column to extract data from
  @param value value to search with

  @return comma separated string
  """
  try:
    if usePattern:
      pattern = f'.*{value}*.'
      results = data[match(f[searchAttr], pattern), :]
    else:
      results = data[f[searchAttr]==str(value),:]
    if results.nrows:
      return ','.join(dt.unique(results[:, f[dataAttr]]).to_list()[0])
    else:
      return None
  except TypeError as error:
    print('Error in ', value,'\n',str(error))
    pass

#///////////////////////////////////////

# ~ 1 ~
# Get metadata and prepare


# ~ 1a ~
# Pull File metadata
files = rd3.get(
  f'rd3_{currentRelease}_file',
  # batch_size=10000,
  attributes='EGA,name,typeFile,samples,patch,experimentID'
)

# flatten nested attributes
for row in files:
  row['typeFile'] = row.get('typeFile').get('identifier') if bool(row.get('typeFile')) else None
  row['samples'] = row.get('samples')[0].get('id') if bool(row.get('samples')) else None
  row['patch'] = ','.join([d['id'] for d in row.get('patch')]) if bool(row.get('patch')) else None

filesDT = dt.Frame(files)
del filesDT['_href']

# check unique patches and delete if applicable
# dt.unique(filesDT['patch'])
# del filesDT['patch']

# ~ 1b ~
# Get Experiment metadata
# get sample IDs via the labinfo table
freezeExperiments = rd3.get(
  f'rd3_{currentRelease}_labinfo',
  attributes='id,experimentID,sample',
  batch_size=10000
)

for row in freezeExperiments:
  row['sample'] = row.get('sample')[0].get('id') if bool(row.get('sample')) else None
    
freezeExperiments = dt.Frame(freezeExperiments)
del freezeExperiments['_href']
    
# ~ 1c ~
# Get Sample metadata
freezeSamples = rd3.get(
  f'rd3_{currentRelease}_sample',
  attributes='id,sampleID,subject',
  batch_size=10000
)

for row in freezeSamples:
  row['subject'] = row.get('subject').get('id') if bool(row.get('subject')) else None

freezeSamples = dt.Frame(freezeSamples)
del freezeSamples['_href']

# ~ 1d ~ 
# Get Subject Metadata
freezeSubjects = dt.Frame(
  rd3.get(f'rd3_{currentRelease}_subject',attributes='id,subjectID,fid')
)

del freezeSubjects['_href']

#//////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Fill in missing sampleIDs
# Using experimentID, find missing sample IDs. For the first pass, merge all
# IDs using experimentID.  

# filesDT[f.samples==None,:]
# filesDT[(f.samples==None) & (f.typeFile!='ped') & (f.typeFile!='json'),['name','samples','experimentID']]
# dt.unique(filesDT[(f.samples==None) & (f.typeFile!='ped') & (f.typeFile!='json'),['name','samples','experimentID']]['experimentID'])
# dt.unique(filesDT[f.samples==None,'typeFile'])
# filesDT[(f.samples==None) & (f.typeFile=='vcf'), :]

filesDT['samples'] = dt.Frame([
  uniqueValuesToString(
    data = freezeExperiments,
    searchAttr='experimentID',
    dataAttr='sample',
    value = tuple[1]
  )
  if (tuple[0] is None) and (tuple[1] is not None)
  else tuple[0]
  for tuple in tqdm(filesDT[:,['samples','experimentID']].to_tuples())
])

#///////////////////////////////////////

# ~ 3 ~
# Fill in missing experiment IDs
# ExperimentIDs need to be joined with the file metadata. This allows users
# to link samples with experiments, as well as find the file paths for given
# experiments. It is unlikely that experiment IDs do not exists, excluding
# PED and Phenopacket files. Even if 

# filesDT[f.experimentID==None,:]
# dt.unique(filesDT[f.experimentID==None,'typeFile'])
# filesDT[(f.experimentID==None) & (f.typeFile=='vcf'), :]
# filesDT[(f.experimentID==None) & (f.typeFile=='bam'), :]

if dt.unique(filesDT[match(f.typeFile, r'^(ped|json)$') == False, 'typeFile']).nrows:
  filesDT['experimentID'] = dt.Frame([
    uniqueValuesToString(
      data = freezeExperiments,
      searchAttr = 'sample',
      dataAttr = 'experimentID',
      value = tuple[1]
    )
    if (tuple[0] is None) and (tuple[1] is not None)
    else tuple[0]
    for tuple in tqdm(filesDT[:, ['experimentID', 'samples']].to_tuples())
  ])


#///////////////////////////////////////

# ~ 4 ~
# Merge subjectIDs
# Like the previous steps, join subjectIDs using sample identifiers. For the
# first run, you don't need to add 'subjectID' into the loop. For future updates,
# make sure loop returns a tuple with sampleID and subjectID. Add check so that
# subjectID is found only if subjectID and sampleID is NA.

filesDT['subjectID'] = dt.Frame([
  uniqueValuesToString(
    data = freezeSamples,
    searchAttr = 'id',
    dataAttr = 'subject',
    value = id
  )
  if id is not None or bool(id) else id
  for id in tqdm(filesDT['samples'].to_list()[0])
])

# filesDT[match(f.subjectID, 'P.*'), ['name','subjectID']]
# filesDT.nrows - filesDT[match(f.subjectID, 'P.*'), ['name','subjectID']].nrows

# merge additional subjectIDs via familyID extracted from the PED filename
# filesDT[f.typeFile=='ped', ['name','fid','subjectID']]

filesDT['fid'] = dt.Frame([
  path.basename(row[0]).split('.ped')[0].split('.')[0]
  if row[1] == 'ped' else None
  for row in filesDT[:, ['name','typeFile']].to_tuples()
])

filesDT['subjectID'] = dt.Frame([
  uniqueValuesToString(
    data = freezeSubjects,
    searchAttr='fid',
    dataAttr='id',
    value = tuple[1],
    usePattern=True
  )
  if (tuple[0] is None) and tuple[1] is not None
  else tuple[0]
  for tuple in tqdm(filesDT[:, ['subjectID', 'fid']].to_tuples())
])

# merge additional subjectsIDs via Phenopackets
filesDT['phenoSubjectId'] = dt.Frame([
  path.basename(d[0]).split('.json')[0].split('.')[0]
  if d[1] == 'json' else None
  for d in filesDT[:,['name', 'typeFile']].to_tuples()
])

# filesDT[f.typeFile=='json', ['name','subjectID']]
# filesDT[f.typeFile=='json', ['name','phenoSubjectId']]
# filesDT[(f.phenoSubjectId==None) & (f.typeFile=='ped'),['name','fid','phenoSubjectId']]
# filesDT[(f.subjectID==None) & (f.typeFile=='ped'),['name','fid','subjectID']]

filesDT['subjectID'] = dt.Frame([
  uniqueValuesToString(
    data=freezeSubjects,
    searchAttr='subjectID',
    dataAttr='id',
    value=tuple[2]
  )
  if (tuple[1] == 'json') and (not tuple[0])
  else tuple[0]
  for tuple in tqdm(filesDT[:, ['subjectID', 'typeFile', 'phenoSubjectId']].to_tuples())
])

filesDT[f.subjectID==None,:]
filesDT[(f.subjectID==None) & (f.typeFile=='ped'),:]
filesDT[f.samples==None,:]
filesDT[f.experimentID==None,:]


#///////////////////////////////////////

# ~ 5 ~
# Import data 

updateSampleIds = dtFrameToRecords(filesDT[f.samples!=None, (f.EGA, f.samples)])
updateSubjectIds = dtFrameToRecords(filesDT[f.subjectID!=None,(f.EGA,f.subjectID)])

rd3.updateColumn(
  entity=f'rd3_{currentRelease}_file',
  attr='samples',
  data=updateSampleIds
)

rd3.updateColumn(
  entity=f"rd3_{currentRelease}_file",
  attr='subjectID',
  data=updateSubjectIds
)