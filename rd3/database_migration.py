#///////////////////////////////////////////////////////////////////////////////
# FILE: database_migration.py
# AUTHOR: David Ruvolo
# CREATED: 2022-10-11
# MODIFIED: 2022-10-11
# PURPOSE: script to migrate data from one instance to another
# STATUS: stable
# PACKAGES: **see below**
# COMMENTS: NA
#///////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis2 import Molgenis
from datatable import dt,f,as_type
from dotenv import load_dotenv
from datetime import datetime
from os import environ
from tqdm import tqdm
load_dotenv()

createdBy = 'rd3-bot'
dateCreated = datetime.today().strftime('%Y-%m-%d')

# rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
# rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# db = Molgenis(environ['MOLGENIS_TEST_HOST'])
# db.login(environ['MOLGENIS_TEST_USR'], environ['MOLGENIS_TEST_PWD'])

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
    if 'materialType' in row:
      if bool(row['materialType']):
        row['materialType'] = ','.join([ type['id'] for type in row['materialType']])
      else:
        row['materialType'] = None
    if 'pathologicalState' in row:
      row['pathologicalState'] = row['pathologicalState']['value']
    row['subject'] = row['subject']['subjectID'] if 'subject' in row else None
    row['tissueType'] = row['tissueType']['identifier'] if 'tissueType' in row else None
    row['retracted'] = row['retracted']['id'] if 'retracted' in row else None
    row['organisation'] = row['organisation']['identifier'] if 'organisation' in row else None
    row['ERN'] = row['ERN']['identifier'] if 'ERN' in row else None
    row['patch'] = ','.join([ patch['id'] for patch in row['patch'] ])
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
    'ERKNet': 'erknet',
    'ERN-GENTURIS': 'ern_genturis',
    'ERN-ITHACA': 'ern_ithaca',
    'ERN-NMD': 'ern_euro_nmd',
    'ERN-RND': 'ern_rnd',
    'ERNCRANIO': 'ern_cranio',
    'ERNEYE': 'ern_eye',
    'ERNEpiCARE': 'ern_epicare',
    'ERNEuroBloodNet': 'ern_eurobloodnet',
    'ERNGUARD-HEART': 'ern_guard_heart',
    'ERNICA': 'ernica',
    'ERNPaedCan': 'ern_paedcan',
    'ERNRITA': 'ern_rita',
    'ERNReCONNET': 'ern_reconnet',
    'Endo-ERN': 'endo_ern',
    'MetabERN': 'metabern',
    'UDN-Italy': 'udn_italy',
    'UDN-Spain': 'udn_spain',
    'VASCERN': 'vascern'
  }
  try:
    return mappings[value]
  except KeyError:
    raise KeyError('No mapping for', str(value), 'found')
  except TypeError:
    raise TypeError('No mapping for', str(value), 'found')


def uniqueValuesById(data, groupby, column, dropDuplicates=True, keyGroupBy=True):
  """Unique Values By Id
  For a datatable object, collapse all unique values by ID into a comma
  separated string.

  @param data datatable object
  @param groupby name of the column that will serve as the grouping variable
  @param column name of the column that contains the values to collapse
  @param dropDuplicates If True, all duplicate rows will be removed
  @param keyGroupBy If True, returned object will be keyed using the value named in groupby
  
  @param datatable object
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
  
  
# def getEarliestDate(array):
#   """Get Earliest Date
#   In an array of values, find the earliest date.
  
#   @param array an array containing one or more date values
#   @return string
#   """
#   dates = [datetime.fromisoformat(date) for date in array]
#   return min(dates)

#///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Pull and prepare Subject metadata

# ~ 1a ~
# pull subject metadata
freeze1_subjects = getSubjectMetadata('rd3_freeze1_subject')
freeze2_subjects = getSubjectMetadata('rd3_freeze2_subject')
freeze3_subjects = getSubjectMetadata('rd3_freeze3_subject')
noveldeepwes_subjects = getSubjectMetadata('rd3_noveldeepwes_subject')
novelepigenome = getSubjectMetadata('rd3_novelepigenome_subject')
novelsrwgs_subjects = getSubjectMetadata('rd3_novelsrwgs_subject')
novellrwgs_subjects = getSubjectMetadata('rd3_novellrwgs_subject')
novelrnaseq_subjects = getSubjectMetadata('rd3_novelrnaseq_subject')
novelwgs_subjects = getSubjectMetadata('rd3_novelwgs_subject')

subjects = dt.rbind(
  dt.Frame(freeze1_subjects),
  dt.Frame(freeze2_subjects),
  dt.Frame(freeze3_subjects),
  dt.Frame(noveldeepwes_subjects),
  dt.Frame(novelepigenome),
  dt.Frame(novelsrwgs_subjects),
  dt.Frame(novellrwgs_subjects),
  dt.Frame(novelrnaseq_subjects),
  dt.Frame(novelwgs_subjects),
  force=True
)

# ~ 1b ~
# Process Subjects

del subjects['id']
subjects.names = {
  'phenopacketsID': 'mostRecentPhenopacketFile',
  'patch': 'partOfRelease',
  'patch_comment': 'comments'
}

# drop 'id' column -- not needed in new version

# format maternal and paternal identifiers
subjects['mid'] = dt.Frame([
  id.split('_')[0] if id else id
  for id in subjects['mid'].to_list()[0]
])

subjects['pid'] = dt.Frame([
  id.split('_')[0] if id else id
  for id in subjects['pid'].to_list()[0]
])

# recode ERNs to new mapping table: dt.unique(subjects['ERN'])
subjects['ERN'] = dt.Frame([
  recodeERN(value) if value else None
  for value in subjects['ERN'].to_list()[0]
])

# remove `novelomics_original`: dt.unique(subjects['partOfRelease'])
subjects['partOfRelease'] = dt.Frame([
  release.replace('novelomics_original','').replace(',,',',')
  for release in subjects['partOfRelease'].to_list()[0]
])


# collapse unique values for subjects with more than one entry
# subjects[:, dt.count(), dt.by(f.subjectID)][f.count > 1, :]
for column in subjects.names:
  if column not in ['subjectID']:
    print('Processing column', f"subjects${column}")
    subjects[column] = uniqueValuesById(
      data = subjects[:, ['subjectID', column]],
      groupby = 'subjectID',
      column = column,
      dropDuplicates = False,
      keyGroupBy = False
    )[column]

# for multiple values in the column `date_solved`, return the earliest date
subjects['date_solved'] = dt.Frame([
  min([
    datetime.fromisoformat(date) for date in dates.split(',')
  ]).strftime('%Y-%m-%d')
  if dates is not None else dates
  for dates in subjects['date_solved'].to_list()[0]
])

# check for multiple values in `solved`. If True exists in the string, return
# True.
subjects['solved'] = dt.Frame([
  'True' in status.split(',') if status else status
  for status in subjects['solved'].to_list()[0]
])

# partOfRelease: get unique values only
subjects['partOfRelease'] = dt.Frame([
  ','.join(sorted(set(release.split(','))))
  for release in subjects['partOfRelease'].to_list()[0]
])

# maternal id: take the first one (original)
subjects['mid'] = dt.Frame([
  value.split(',')[0] if value else value
  for value in subjects['mid'].to_list()[0]
])

# paternal id: take the first one (original)
subjects['pid'] = dt.Frame([
  value.split(',')[0] if value else value
  for value in subjects['pid'].to_list()[0]
])

# sex1: get unique values only
subjects['sex1'] = dt.Frame([
  value.replace('UD,F','F').replace('F,M', 'F')
  if value else value
  for value in subjects['sex1'].to_list()[0]
])

# check retracted column -- make sure all values are 'Y' or 'N'
# subjects[dt.re.match(f.retracted, '.*,.*'),:]
# subjects[dt.re.match(f.retracted, '.*,.*'),'retracted']
subjects[f.retracted == 'N,Y', 'retracted'] = 'Y'

# make sure all retracted subjects have no metadata
columnsToKeep = ['subjectID','partOfRelease', 'retracted', 'comments']
for subject in subjects[f.retracted=='Y', 'subjectID'].to_list()[0]:
  for column in subjects.names:
    if column not in columnsToKeep:
      subjects[f.subjectID==subject, column] = None

    
# set metadata
subjects['dateRecordCreated'] = dateCreated
subjects['recordCreatedBy'] = createdBy

# pull distinct rows
subjects = subjects[:, dt.first(f[:]), dt.by(f.subjectID)]
retractedSubjects = subjects[f.retracted == 'Y', 'subjectID'].to_list()[0]


rd3.importDatatableAsCsv('solverd_subjects', subjects)

#///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# pull and process subjectinfo
freeze1_subjectinfo = getSubjectInfoMetadata('rd3_freeze1_subjectinfo')
freeze2_subjectinfo = getSubjectInfoMetadata('rd3_freeze2_subjectinfo')
freeze3_subjectinfo = getSubjectInfoMetadata('rd3_freeze3_subjectinfo')
noveldeepwes_subjectinfo = getSubjectInfoMetadata('rd3_noveldeepwes_subjectinfo')
novelepigenome_subjectinfo = getSubjectInfoMetadata('rd3_novelepigenome_subjectinfo')
novelsrwgs_subjectinfo = getSubjectInfoMetadata('rd3_novelsrwgs_subjectinfo')
novellrwgs_subjectinfo = getSubjectInfoMetadata('rd3_novellrwgs_subjectinfo')
novelrnaseq_subjectinfo = getSubjectInfoMetadata('rd3_novelrnaseq_subjectinfo')
novelwgs_subjectinfo = getSubjectInfoMetadata('rd3_novelwgs_subjectinfo')

subjectinfo = dt.rbind(
  dt.Frame(freeze1_subjectinfo),
  dt.Frame(freeze2_subjectinfo),
  dt.Frame(freeze3_subjectinfo),
  dt.Frame(noveldeepwes_subjectinfo),
  dt.Frame(novelepigenome_subjectinfo),
  dt.Frame(novelsrwgs_subjectinfo),
  dt.Frame(novellrwgs_subjectinfo),
  dt.Frame(novelrnaseq_subjectinfo),
  dt.Frame(novelwgs_subjectinfo),
  force = True
)

del subjectinfo['id']
subjectinfo.names = {
  'dateofBirth': 'dateOfBirth',
  'patch': 'partOfRelease',
  'patch_comment': 'comments'
}

# recode subject IDs
subjectinfo['subjectID'] = dt.Frame([
  value.split('_')[0] if value else value
  for value in subjectinfo['subjectID'].to_list()[0]
])

# remove novelomics original from release
subjectinfo['partOfRelease'] = dt.Frame([
  release.replace('novelomics_original','').replace(',,',',')
  for release in subjectinfo['partOfRelease'].to_list()[0]
])

# force class
subjectinfo[:, dt.update(dateOfBirth=as_type(f.dateOfBirth, str))]

# make sure all unique values are collapsed by row
for column in subjectinfo.names:
  if column not in ['subjectID']:
    print('Processing', f"subjectinfo${column}")
    subjectinfo[column] = uniqueValuesById(
      data = subjectinfo[:, ['subjectID', column]],
      groupby = 'subjectID',
      column = column,
      dropDuplicates = False,
      keyGroupBy = False
    )[column]

# fix partOfRelease
subjectinfo['partOfRelease'] = dt.Frame([
  ','.join(sorted(set(release.split(','))))
  for release in subjectinfo['partOfRelease'].to_list()[0]
])

# remove metadata for retracted subjects
subjects[f.retracted == 'Y', :].to_list()[0]
columnsToKeep = ['subjectID', 'partOfRelease', 'comments']
for subject in subjects[f.retracted == 'Y', 'subjectID'].to_list()[0]:
  for column in subjectinfo.names:
    if column not in columnsToKeep:
      subjectinfo[f.subjectID==subject, column] = None

# set import attribs
subjectinfo['dateRecordCreated'] = dateCreated
subjectinfo['recordCreatedBy'] = createdBy

subjectinfo = subjectinfo[:, dt.first(f[:]), dt.by(f.subjectID)]

rd3.importDatatableAsCsv('solverd_subjectinfo',subjectinfo)

#///////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Migrate sample metadata
freeze1_samples = getSampleMetadata('rd3_freeze1_sample')
freeze2_samples = getSampleMetadata('rd3_freeze2_sample')
freeze3_samples = getSampleMetadata('rd3_freeze3_sample')
noveldeepwes_samples = getSampleMetadata('rd3_noveldeepwes_sample')
novelepigenome_samples = getSampleMetadata('rd3_novelepigenome_sample')
novelsrwgs_samples = getSampleMetadata('rd3_novelsrwgs_sample')
novellrwgs_samples = getSampleMetadata('rd3_novellrwgs_sample')
novelrnaseq_samples = getSampleMetadata('rd3_novelrnaseq_sample')
novelwgs_samples = getSampleMetadata('rd3_novelwgs_sample')

samples = dt.rbind(
  dt.Frame(freeze1_samples),
  dt.Frame(freeze2_samples),
  dt.Frame(freeze3_samples),
  dt.Frame(noveldeepwes_samples),
  dt.Frame(novelepigenome_samples),
  dt.Frame(novelsrwgs_samples),
  dt.Frame(novellrwgs_samples),
  dt.Frame(novelrnaseq_samples),
  dt.Frame(novelwgs_samples),
  force = True
)

del samples['id']
samples.names = {
  'subject': 'belongsToSubject',
  'patch': 'partOfRelease',
  'patch_comment': 'comments'
}

# recode ERNs
samples['ERN'] = dt.Frame([
  recodeERN(value) if value else value
  for value in samples['ERN'].to_list()[0]
])

# remove novelomics original from release
samples['partOfRelease'] = dt.Frame([
  release.replace('novelomics_original','').replace(',,',',')
  for release in samples['partOfRelease'].to_list()[0]
])


# for each column, collapse unique values
# samples[:, dt.count(), dt.by(f.sampleID)][f.count > 1, :]
for column in samples.names:
  if column not in ['sampleID', 'dateRecordCreated', 'recordCreatedBy']:
    print('Processing', f"samples${column}")
    samples[column] = uniqueValuesById(
      data = samples[:, ['sampleID', column]],
      groupby = 'sampleID',
      column = column,
      dropDuplicates = False,
      keyGroupBy = False
    )[column]

# ---- OPTIONAL PROCESSING -----
# Fix subject associations
# correct samples that are linked to more than one subject
samples[dt.re.match(f.belongsToSubject, '.*,.*'),:]
samples[f.sampleID=='', 'belongsToSubject'] = ''
#
# samples['belongsToSubject'] = dt.Frame([
#   ''
#   if row[0] == '' else row[1]
#   for row in samples[:, (f.sampleID, f.belongsToSubject)].to_tuples()
# ])
# ------------------------------

# make sure all metadata of retracted samples are removed
columnsToKeep = ['sampleID', 'partOfRelease', 'retracted', 'comments']
for sample in samples[f.retracted == 'Y', :].to_list()[0]:
  for column in samples.names:
    if column not in columnsToKeep:
      samples[f.sampleID==sample, column] = None


# remove sample metadata if subjects were removed
for subject in retractedSubjects:
  sampleIDs = samples[f.belongsToSubject == subject, 'sampleID'].to_list()[0]
  if sampleIDs:
    for sample in sampleIDs:
      for column in samples.names:
        if column not in columnsToKeep:
          samples[f.sampleID == sample, column] = None
      

# set row metadata
samples['dateRecordCreated'] = dateCreated
samples['recordCreatedBy'] = createdBy

samples = samples[:, dt.first(f[:]), dt.by(f.sampleID)]
retractedSamples = samples[f.retracted == 'Y', 'sampleID'].to_list()[0]

rd3.importDatatableAsCsv('solverd_samples', samples)

#///////////////////////////////////////////////////////////////////////////////

# ~ 4 ~
# pull experiment metadata
freeze1_labinfo = getExperimentMetadata('rd3_freeze1_labinfo')
freeze2_labinfo = getExperimentMetadata('rd3_freeze2_labinfo')
freeze3_labinfo = getExperimentMetadata('rd3_freeze3_labinfo')
noveldeepwes_labinfo = getExperimentMetadata('rd3_noveldeepwes_labinfo')
novelepigenome_labinfo = getExperimentMetadata('rd3_novelepigenome_labinfo')
novelsrwgs_labinfo = getExperimentMetadata('rd3_novelsrwgs_labinfo')
novellrwgs_labinfo = getExperimentMetadata('rd3_novellrwgs_labinfo')
novelrnaseq_labinfo = getExperimentMetadata('rd3_novelrnaseq_labinfo')
novelwgs_labinfo = getExperimentMetadata('rd3_novelwgs_labinfo')

experiments = dt.rbind(
  dt.Frame(freeze1_labinfo),
  dt.Frame(freeze2_labinfo),
  dt.Frame(freeze3_labinfo),
  dt.Frame(noveldeepwes_labinfo),
  dt.Frame(novelepigenome_labinfo),
  dt.Frame(novelsrwgs_labinfo),
  dt.Frame(novellrwgs_labinfo),
  dt.Frame(novelrnaseq_labinfo),
  dt.Frame(novelwgs_labinfo),
  force = True
)

del experiments['id']
experiments.names = {
  'sample': 'sampleID',
  'patch': 'partOfRelease',
  'patch_comment': 'comments'
}

# drop old ID suffix
experiments['sampleID'] = dt.Frame([
  value.split('_')[0] if value else value
  for value in experiments['sampleID'].to_list()[0]
])

# recode sequencing center: dt.unique(experiments['sequencingCentre'])
experiments['sequencingCentre'] = dt.Frame([
  None if value in ['UNKNOWN', 'Unknown', 'other'] else value
  for value in experiments['sequencingCentre'].to_list()[0]
])

# recode sequencer: dt.unique(experiments['sequencer'])
experiments['sequencer'] = dt.Frame([
  None if value == 'Unknown' else value
  for value in experiments['sequencer'].to_list()[0]
])

# check for duplicate entries and collapse unique values
experiments[:, dt.count(), dt.by(f.experimentID)][f.count > 1, :]
for column in experiments.names:
  if column not in ['experimentID']:
    print('Processing', f"experiments${column}")
    experiments[column] = uniqueValuesById(
      data = experiments[:, ['experimentID', column]],
      groupby = 'experimentID',
      column = column,
      dropDuplicates = False,
      keyGroupBy = False 
    )[column]

# check for multiple string values
# experiments[dt.re.match(f.seqType, '.*,.*'), :]
# experiments[dt.re.match(f.seqType, '.*,.*'), (f.experimentID,f.sampleID,f.seqType,f.partOfRelease)].to_csv('data/solverd_multiple_sample_experiments.csv')

experiments['partOfRelease'] = dt.Frame([
  ','.join(sorted(set(release.split(','))))
  for release in experiments['partOfRelease'].to_list()[0]
])

# make sure all retracted experiments are removed
columnsToKeep = ['experimentID', 'retracted', 'comments', 'partOfRelease']
for experiment in experiments[f.retracted=='Y', :].to_list()[0]:
  for column in experiments.names:
    if column not in columnsToKeep:
      experiments[f.experimentID==experiment, column] = None

# remove experiments if sample was removed
for sample in retractedSamples:
  experimentIDs = experiments[f.sampleID == sample, 'experimentID'].to_list()[0]
  if experimentIDs:
    for experimentId in experimentIDs:
      for column in experiments.names:
        if column not in columnsToKeep:
          experiments[f.experimentID==experiment, column] = None

# set row metadata
experiments['dateRecordCreated'] = dateCreated
experiments['recordCreatedBy'] = createdBy

experiments = experiments[:, dt.first(f[:]), dt.by(f.experimentID)]
retractedExperiments = experiments[f.retracted=='Y', 'experimentID'].to_list()[0]

rd3.importDatatableAsCsv('solverd_labinfo', experiments)

#///////////////////////////////////////////////////////////////////////////////

# ~ 1e ~
# Pull files
freeze1_files = getFileMetadata('rd3_freeze1_file')
freeze2_files = getFileMetadata('rd3_freeze2_file')
freeze3_files = getFileMetadata('rd3_freeze3_file')
noveldeepwes_files = getFileMetadata('rd3_noveldeepwes_file')
novelepigenome_files = getFileMetadata('rd3_novelepigenome_file')
novelsrwgs_files = getFileMetadata('rd3_novelsrwgs_file')
novellrwgs_files = getFileMetadata('rd3_novellrwgs_file')
novelrnaseq_files = getFileMetadata('rd3_novelrnaseq_file')
novelwgs_files = getFileMetadata('rd3_novelwgs_file')

files = dt.rbind(
  dt.Frame(freeze1_files),
  dt.Frame(freeze2_files),
  dt.Frame(freeze3_files),
  dt.Frame(noveldeepwes_files),
  dt.Frame(novelepigenome_files),
  dt.Frame(novelsrwgs_files),
  dt.Frame(novellrwgs_files),
  dt.Frame(novelrnaseq_files),
  dt.Frame(novelwgs_files),
  force = True
)

files.names = {
  'typeFile': 'fileFormat',
  'samples': 'sampleID',
  'patch': 'partOfRelease',
  'filepath_sandbox': 'fenderFilePath'
}

# drop RD3 ID suffix
files['sampleID'] = dt.Frame([
  value.split('_')[0] if value else value
  for value in files['sampleID'].to_list()[0]
])

files['subjectID'] = dt.Frame([
  value.split('_')[0] if value else value
  for value in files['subjectID'].to_list()[0]
])

files['experimentID'] = dt.Frame([
  value.replace('_original', '') if value else value
  for value in files['experimentID'].to_list()[0]
])


# remove retracted identifiers (subject, sample, experiments)
files['subjectID'] = dt.Frame([
  None if id in retractedSubjects else id
  for id in files['subjectID'].to_list()[0]
])

files['sampleID'] = dt.Frame([
  None if id in retractedSamples else id
  for id in files['sampleID'].to_list()[0]
])

files['experimentID'] = dt.Frame([
  None if id in retractedExperiments else id
  for id in files['experimentID'].to_list()[0]
])

# check for unknown experiments
experimentsIDs = experiments['experimentID'].to_list()[0]
files['hasExperiment'] = dt.Frame([
  id in experimentsIDs
  for id in tqdm(files['experimentID'].to_list()[0])
])

samplesIDs = samples['sampleID'].to_list()[0]
files['hasSample'] = dt.Frame([
  id in sampleIDs
  for id in tqdm(files['sampleID'].to_list()[0])
])


# PROCESS UNKNOWN IDS
# !-------- ONLY RUN THIS IF YOU KNOW WHAT YOU ARE DOING. ----------!
# files[f.fileFormat!='json',:][:, dt.count(), dt.by(f.hasExperiment)]
# files[(f.fileFormat!='json') & (f.hasExperiment==False), :]
# dt.unique(files[(f.sampleID!=None) & (f.hasSample==False), 'sampleID'])
# dt.unique(files[(f.experimentID!=None) & (f.hasExperiment==False), 'experimentID'])
# dt.unique(
#   files[
#     (f.fileFormat!='json') & (f.hasExperiment==False),
#     (f.EGA, f.subjectID, f.sampleID, f.experimentID, f.partOfRelease, f.hasExperiment)
#   ][
#     f.experimentID != None,
#     # :
#     'experimentID'
#   ]
# )
# The following overwrites experimentIDs when the ID is valid, but there is
# no matching record in the experiments table. This should be very few (<5), but 
# it probably occurs when an experiment/sample/subject was retracted, but not
# updated in the file manifest data.
# files['experimentID'] = dt.Frame([
#   None if (row[0]) and (row[1] == False) else row[0]
#   for row in files[:, (f.experimentID, f.hasExperiment)].to_tuples()
# ])
# !----------------------------------------------------------------------------!

# add row-level metadata
files['dateRecordCreated'] = dateCreated
files['recordCreatedBy'] = createdBy

rd3.importDatatableAsCsv('solverd_files', files)