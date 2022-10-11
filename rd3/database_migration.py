#///////////////////////////////////////////////////////////////////////////////
# FILE: database_migration.py
# AUTHOR: David Ruvolo
# CREATED: 2022-10-11
# MODIFIED: 2022-10-11
# PURPOSE: script to migrate data from one instance to another
# STATUS: in.progress
# PACKAGES: **see below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from datatable import dt,f,as_type
from dotenv import load_dotenv
from datetime import datetime
from os import environ
load_dotenv()

rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

db = Molgenis(environ['MOLGENIS_TEST_HOST'])
db.login(environ['MOLGENIS_TEST_USR'], environ['MOLGENIS_TEST_PWD'])

#///////////////////////////////////////

def getSubjectMetadata(pkg_entity, **kwargs):
  data = rd3.get(pkg_entity, **kwargs)
  for row in data:
    del row['_href']
    del row['variant']
    row['sex1'] = row['sex1']['identifier'] if 'sex1' in row else None 
    row['mid'] = row['mid']['id'] if 'mid' in row else None
    row['pid'] = row['pid']['id'] if 'pid' in row else None
    
    if 'phenotype' in row:
      row['phenotype'] = ','.join([
        phenotype['id'] for phenotype in row['phenotype']
      ])
    
    if 'hasNotPhenotype' in row:
      row['hasNotPhenotype'] = ','.join([
        value['id'] for value in row['hasNotPhenotype']
      ]) if bool(row['hasNotPhenotype']) else None
      
    if 'disease' in row:
      row['disease'] = ','.join([
        value['id'] for value in row['disease']
      ]) if bool(row['disease']) else None
      
    row['organisation'] = row['organisation']['identifier'] if 'organisation' in row else None
    row['ERN'] = row['ERN']['identifier'] if 'ERN' in row else None
    
    row['patch'] = ','.join([
      patch['id'] for patch in row['patch']
    ]) if bool(row['patch']) else None
    
    row['recontact'] = row['recontact']['id'] if 'recontact' in row else None
    row['retracted'] = row['retracted']['id'] if 'retracted' in row else None
  return data

def getSubjectInfoMetadata(pkg_entity, **kwargs):
  data = rd3.get(pkg_entity, **kwargs)
  for row in data:
    del row['_href']
    if 'subjectID' in row:
      row['subjectID'] = row['subjectID']['id']
    if 'ageOfOnset' in row:
      row['ageOfOnset'] = row['ageOfOnset']['id']
    row['patch'] = ','.join([ patch['id'] for patch in row['patch'] ])
  return data 

def getSampleMetadata(pkg_entity, **kwargs):
  data = rd3.get(pkg_entity, **kwargs)
  for row in data:
    del row['_href']
    del row['materialType']
    row['subject'] = row['subject']['subjectID'] if 'subject' in row else None
    row['tissueType'] = row['tissueType']['identifier'] if 'tissueType' in row else None
    row['retracted'] = row['retracted']['id'] if 'retracted' in row else None
    row['organisation'] = row['organisation']['identifier'] if 'organisation' in row else None
    row['ERN'] = row['ERN']['identifier'] if 'ERN' in row else None
    row['patch'] = ','.join([
      patch['id'] for patch in row['patch']
    ]) if bool(row['patch']) else None
  return data
    
def getExperimentMetadata(pkg_entity, **kwargs):
  data = rd3.get(pkg_entity, **kwargs)
  for row in data:
    del row['_href']
    if 'sample' in row:
      row['sample'] = ','.join([ value['id'] for value in row['sample'] ])
    if 'libraryType' in row:
      row['libraryType'] = row['libraryType']['identifier']
    if 'library' in row:
      row['library'] = ','.join([ value['identifier'] for value in row['library'] ])
    if 'seqType' in row:
      row['seqType'] = row['seqType']['identifier']
    if 'retracted' in row:
      row['retracted'] = row['retracted']['id']
    row['patch'] = ','.join([ patch['id'] for patch in row['patch'] ])
  return data

def getFileMetadata(pkg_entity, **kwargs):
  data = rd3.get(pkg_entity, **kwargs)
  for row in data:
    del row['_href']
    if 'typeFile' in row:
      row['typeFile'] = row['typeFile']['identifier']
    if 'samples' in row:
      row['samples'] = ','.join([ sample['id'] for sample in row['samples'] ])
    if 'subjectID' in row:
      row['subjectID'] = ','.join([ subject['id'] for subject in row['subjectID'] ])
    row['patch'] = ','.join([ patch['id'] for patch in row['patch'] ])
  return data

def recodeERN(value):
  mappings = {
    'ERN-NMD': 'ern_euro_nmd',
    'ERN-RND': 'ern_rnd'
  }
  try:
    return mappings[value]
  except KeyError:
    raise KeyError('No mapping for', str(value), 'found')
  except TypeError:
    raise TypeError('No mapping for', str(value), 'found')
    
#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Pull Information for Test Export
# Since this is a test, find and pull data from families so that all xref/mref
# columns will work.

# ~ 1a ~
# get family IDs
df1_family = dt.Frame(rd3.get('rd3_freeze1_subject', attributes='fid',num=50))['fid']
df2_family = dt.Frame(rd3.get('rd3_freeze2_subject', attributes='fid',num=50))['fid']

df1_fids = ','.join(dt.unique(df1_family[f.fid!=None, 'fid']).to_list()[0])
df2_fids = ','.join(dt.unique(df2_family[f.fid!=None, 'fid']).to_list()[0])

df1_query = f"fid=in=({df1_fids})"
df2_query = f"fid=in=({df2_fids})"

# ~ 1b ~
# pull subject metadata
freeze1_subjects = getSubjectMetadata('rd3_freeze1_subject', q=df1_query)
freeze2_subjects = getSubjectMetadata('rd3_freeze2_subject', q=df2_query)
subjects = dt.rbind(
  dt.Frame(freeze1_subjects),
  dt.Frame(freeze2_subjects),
  force=True
)

subject_ids = dt.unique(subjects['id']).to_list()[0]
subject_query = ','.join([f"subject=q={id}" for id in subject_ids])

# ~ 1c ~
# pull subjectinfo

subjectinfo_query = subject_query.replace('subject=', 'subjectID=')
freeze1_subjectinfo = getSubjectInfoMetadata('rd3_freeze1_subjectinfo', q=subjectinfo_query)
freeze2_subjectinfo = getSubjectInfoMetadata('rd3_freeze2_subjectinfo', q=subjectinfo_query)

subjectinfo = dt.rbind(
  dt.Frame(freeze1_subjectinfo),
  dt.Frame(freeze2_subjectinfo),
  force = True
)

# ~ 1d ~
# pull sample metadata
freeze1_samples = getSampleMetadata('rd3_freeze1_sample', q=subject_query)
freeze2_samples = getSampleMetadata('rd3_freeze2_sample', q=subject_query)
samples = dt.rbind(
  dt.Frame(freeze1_samples),
  dt.Frame(freeze2_samples),
  force = True
)

# ~ 1d ~
# pull experiment metadata
sample_ids = dt.unique(samples['id']).to_list()[0]
sample_query = ','.join([f"sample=q={id}" for id in sample_ids])

freeze1_labs = getExperimentMetadata('rd3_freeze1_labinfo', q=sample_query)
freeze2_labs = getExperimentMetadata('rd3_freeze2_labinfo', q=sample_query)
experiments = dt.rbind(
  dt.Frame(freeze1_labs),
  dt.Frame(freeze2_labs),
  force = True
)

# ~ 1e ~
# Pull files

file_query = subject_query.replace('subject=', 'subjectID=')
freeze1_files = getFileMetadata('rd3_freeze1_file', q=file_query)
freeze2_files = getFileMetadata('rd3_freeze2_file', q=file_query)
files = dt.rbind(
  dt.Frame(freeze1_files),
  dt.Frame(freeze2_files),
  force = True
)

#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Transform RD3 data into RD3 v2


# ~ 2a ~
# Process Subjects

# drop 'id' column -- not needed in new version
del subjects['id']

# format maternal and paternal identifiers
subjects['mid'] = dt.Frame([
  id.split('_')[0] if id else id
  for id in subjects['mid'].to_list()[0]
])

subjects['pid'] = dt.Frame([
  id.split('_')[0] if id else id
  for id in subjects['pid'].to_list()[0]
])

# recode ERNs to new mapping table
# dt.unique(subjects['ERN'])
subjects['ERN'] = dt.Frame([
  recodeERN(value) if value else None
  for value in subjects['ERN'].to_list()[0]
])

# rename columns
subjects.names = {
  'phenopacketsID': 'mostRecentPhenopacketFile',
  'patch': 'partOfRelease'
}

#///////////////////////////////////////

# ~ 2c ~
# process subject info

del subjectinfo['id']

# recode subject IDs
subjectinfo['subjectID'] = dt.Frame([
  value.split('_')[0] if value else value
  for value in subjectinfo['subjectID'].to_list()[0]
])

subjectinfo[:, dt.update(dateOfBirth=as_type(f.dateOfBirth, str))]

# rename columns
subjectinfo.names = {
  'dateofBirth': 'dateOfBirth',
  'patch': 'partOfRelease'
}


#///////////////////////////////////////

# ~ 2c ~
# process samples

del samples['id']

# recode ERNs
samples['ERN'] = dt.Frame([
  recodeERN(value) if value else value
  for value in samples['ERN'].to_list()[0]
])

# update columns
samples.names = {
  'subject': 'belongsToSubject',
  'patch': 'partOfRelease'
}

#///////////////////////////////////////

# ~ 2d ~
# process experiments

del experiments['id']

# drop old ID suffix
experiments['sample'] = dt.Frame([
  value.split('_')[0] if value else value
  for value in experiments['sample'].to_list()[0]
])

dt.unique(experiments['sequencingCentre'])

# temporary recoding of sequencing center
dt.unique(experiments['sequencingCentre'])
experiments['sequencingCentre'] = dt.Frame([
  None if value == 'Unknown' else value
  for value in experiments['sequencingCentre'].to_list()[0]
])

# temporary recoding of sequencer
dt.unique(experiments['sequencer'])
experiments['sequencer'] = dt.Frame([
  None if value == 'Unknown' else value
  for value in experiments['sequencer'].to_list()[0]
])

# drop void columns
if str(experiments['sequencer'].type) == 'Type.void':
  del experiments['sequencer']

if str(experiments['sequencingCentre'].type) == 'Type.void':
  del experiments['sequencingCentre']

# rename columns
experiments.names = {
  'sample': 'sampleID',
  'patch': 'partOfRelease'
}

#///////////////////////////////////////

# ~ 2e ~
# Process files

# drop RD3 ID suffix
files['samples'] = dt.Frame([
  value.split('_')[0] if value else value
  for value in files['samples'].to_list()[0]
])

files['subjectID'] = dt.Frame([
  value.split('_')[0] if value else value
  for value in files['subjectID'].to_list()[0]
])

# rename columns
files.names = {
  'typeFile': 'fileFormat',
  'samples': 'sampleID',
  'patch': 'partOfRelease',
  'filepath_sandbox': 'fenderFilePath'
}

#///////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Import Data

# set metadata columns
createdBy = 'rd3-bot'
dateCreated = datetime.today().strftime('%Y-%m-%d')

subjects['dateRecordCreated'] = dateCreated
subjects['recordCreatedBy'] = createdBy

subjectinfo['dateRecordCreated'] = dateCreated
subjectinfo['recordCreatedBy'] = createdBy

samples['dateRecordCreated'] = dateCreated
samples['recordCreatedBy'] = createdBy

experiments['dateRecordCreated'] = dateCreated
experiments['recordCreatedBy'] = createdBy

files['dateRecordCreated'] = dateCreated
files['recordCreatedBy'] = createdBy


# import
db.importDatatableAsCsv('rd3_subjects', subjects)
db.importDatatableAsCsv('rd3_subjectinfo', subjectinfo)
db.importDatatableAsCsv('rd3_samples', samples)
db.importDatatableAsCsv('rd3_labinfo', experiments)
db.importDatatableAsCsv('rd3_files', files)

# cleaup
# db.delete('rd3_subjects')
# db.delete('rd3_samples')
# db.delete('rd3_labinfo')
# db.delete('rd3_files')
