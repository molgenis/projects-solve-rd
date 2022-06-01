#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_data_overview_mapping.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-05-16
#' MODIFIED: 2022-06-01
#' PURPOSE: generate dataset for rd3_overview
#' STATUS: stable
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from rd3.api.molgenis import Molgenis
from rd3.utils.utils import (
    dtFrameToRecords,
    statusMsg,
    flattenBoolArray,
    flattenStringArray,
    flattenValueArray,
    createUrlFilter
)

from datatable import dt, f, first
from os import environ
from dotenv import load_dotenv
import numpy as np
import urllib
from tqdm import tqdm


# ~ 0 ~ 
# Fetch Metadata for all releases

# init database connection
statusMsg('Connecting to RD3....')
load_dotenv()
host=environ['MOLGENIS_ACC_HOST'] # or use `*_PROD_*`
rd3=Molgenis(url=host)
rd3.login(
    username=environ['MOLGENIS_ACC_USR'],
    password=environ['MOLGENIS_ACC_PWD']
)


# SET RELEASES
# If new releases are added to RD3, add package identifier to the list below.
availableReleases=[
    'freeze1',
    'freeze2',
    'freeze3',
    'noveldeepwes',
    'novelrnaseq',
    'novellrwgs',
    'novelsrwgs',
    'novelwgs'
]

statusMsg('Pulling metadata....')

# fetch subject metadata
subjects=[]
for release in availableReleases:
    statusMsg('Fetching subject metadata for',release)
    data=rd3.get(
        entity=f"rd3_{release}_subject",
        batch_size=10000,
        attributes=','.join([
            'id', 'subjectID',
            'sex1',
            'fid', 'mid', 'pid',
            'clinical_status', 'disease', 'phenotype', 'hasNotPhenotype',
            'organisation', 'ERN',
            'solved',
            'patch'
        ])
    )

    # clean data
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
    

# fetch sample metadata
samples=[]
for release in availableReleases:
    statusMsg('Fetching sample metadata for',release)
    data=rd3.get(
        entity=f"rd3_{release}_sample",
        batch_size=10000, 
        attributes=','.join(['id', 'sampleID', 'subject']),
        q="retracted==N"
    )
    
    # clean data
    for row in data:        
        row['subject']=row.get('subject',{}).get('id')
        # row['patch']=','.join([record['id'] for record in row['patch']])
        row['release']=release
    
    samples.extend(data)
    
# fetch experiment metadata
experiments=[]
for release in availableReleases:
    statusMsg('Fetching experiment metadata for',release)
    data=rd3.get(
        entity=f"rd3_{release}_labinfo",
        batch_size=10000,
        attributes=','.join(['id', 'experimentID', 'sample'])
    )
    
    # clean data
    for row in data:
        row['sample']=row.get('sample',{})[0].get('id')
        row['release']=release
        
    experiments.extend(data)
    

# fetch file metadata
files=[]
for release in availableReleases:
    statusMsg('Fetching file metadata for',release)
    data=rd3.get(
        entity=f"rd3_{release}_file",
        batch_size=1000,
        attributes=','.join(['EGA','experimentID','typeFile']),
        # remove PEDs and json for now
        q="typeFile=in=(bai,bam,bed,cram,fastq,vcf)"
    )
    
    for row in data:
        # if row.get('samples'):
        #     row['samples']=row.get('samples')[0].get('id')
        # else:
        #     row['samples']=None
        if row.get('typeFile'):
            row['typeFile']=row.get('typeFile',{}).get('identifier')
        else:
            row['typeFile']=None
        # if row.get('patch'):
        #     if isinstance(row['patch'], dict):
        #         row['patch']=row.get('patch',{}).get('id')
        #     if isinstance(row['patch'],list):
        #         row['patch']=','.join([record['id'] for record in row['patch']])
        #     else:
        #         row['patch']=row['patch']
        # else:
        #     row['patch']=None
        row['release']=release
    
    files.extend(data)

    
# convert objects to datatable objects
statusMsg('Converting to datatable objects....')
subjects=dt.Frame(subjects)
samples=dt.Frame(samples)
experiments=dt.Frame(experiments)
files=dt.Frame(files)

del subjects['_href']
del samples['_href']
del experiments['_href']
del files['_href']

#//////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# RESHAPE SUBJECTS
# Create a list of unique subjects only. Collapse all metadata
# by `subjectID`. Use the following code to view subjects with more than one
# record.
#
# subjects[
#     f.subjectID==subjects[:, dt.count(), dt.by(f.subjectID)][
#          f.count>=5, :
#     ].to_list()[0][:1][0],
#     :
# ]

# collapse sex1
statusMsg('Collapsing sex....')
subjects['sex1'] = dt.Frame([
    flattenValueArray(subjects[f.subjectID==d,'sex1'][f.sex1!=None,:].to_list()[0])
    for d in subjects[:, f.subjectID].to_list()[0]
])


# collapse family identifier
statusMsg('Collapsing family identifiers....')
subjects['fid'] = dt.Frame([
    flattenValueArray(subjects[f.subjectID==d,'fid'][f.fid!=None,:].to_list()[0])
    for d in subjects[:, f.subjectID].to_list()[0]
])

# collapse maternal identifier
statusMsg('Collapsing maternal identifiers....')
subjects['mid'] = dt.Frame([
    flattenValueArray(subjects[f.subjectID==d,'mid'][f.mid!=None,:].to_list()[0])
    for d in subjects[:, f.subjectID].to_list()[0]
])

subjects['mid'] = dt.Frame([
    d.split('_original')[0]
    for d in subjects['mid'].to_list()[0]
    if d is not None
])


# collapse paternal identifier
statusMsg('Collapsing paternal identifiers....')
subjects['pid'] = dt.Frame([
    flattenValueArray(subjects[f.subjectID==d,'pid'][f.pid!=None,:].to_list()[0])
    for d in subjects[:, f.subjectID].to_list()[0]
])

subjects['pid'] = dt.Frame([
    d.split('_original')[0]
    for d in subjects['pid'].to_list()[0]
    if d is not None
])


# collapse clinical status
statusMsg('Collapsing clinical status....')
subjects['clinical_status'] = dt.Frame([
    flattenBoolArray(
        array=subjects[f.subjectID==d,'clinical_status'][f.clinical_status!= None, :].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])


# collapse disease
statusMsg('Collapsing diagnoses....')
subjects['disease'] = dt.Frame([
    flattenStringArray(
        array=subjects[f.subjectID==d, 'disease'][f.disease != None, :].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])


# collapse phenotype
statusMsg('Collapsing observed phenotypes....')
subjects['phenotype'] = dt.Frame([
    flattenStringArray(
        array=subjects[f.subjectID==d, 'phenotype'][f.phenotype!=None,:].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])


# collapse hasNotPhenotype
statusMsg('Collapsing unobserved phenotypes....')
subjects['hasNotPhenotype'] = dt.Frame([
    flattenStringArray(
        array=subjects[f.subjectID==d, 'hasNotPhenotype'][
            f.hasNotPhenotype != None, :
        ].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])


# collapse organisation
statusMsg('Collapsing organisation....')
subjects['organisation'] = dt.Frame([
    flattenValueArray(
        array=subjects[f.subjectID==d, 'organisation'][
            f.organisation!=None, :
        ].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])


# collapse ERN
statusMsg('Collapsing ERNs....')
subjects['ERN'] = dt.Frame([
    flattenValueArray(array=subjects[f.subjectID==d, f.ERN].to_list()[0])
    for d in subjects[:, f.subjectID].to_list()[0][:1]
])


# collapse solved
statusMsg('Collapsing solved status....')
subjects['solved'] = dt.Frame([
    flattenBoolArray(array=subjects[f.subjectID==d, f.solved].to_list()[0])
    for d in subjects[:, f.subjectID].to_list()[0]
])

# collapse patch
statusMsg('Collapsing patch information....')
subjects['patch'] = dt.Frame([
    flattenStringArray(
        array=subjects[f.subjectID==d, f.patch][f.patch!=None, :].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])


# collapse release
statusMsg('Collapsing emx-release....')
subjects['associatedRD3Releases'] = dt.Frame([
    flattenValueArray(
        array=subjects[f.subjectID==d, f.release][f.release != None, :].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])

# DISTINCT RECORDS ONLY
# since all information has been flattend and repeated by subject, it is
# possible to select only the distinct records.
statusMsg('Complete! Selecting distinct records only....')

subjects = subjects[:, first(f[:]), dt.by(f.subjectID)]

#//////////////////////////////////////////////////////////////////////////////

# ~ 2 ~ 
# RESHAPE SAMPLES
# Sample metadata will need to be processed a bit differently than subject
# metadata. The idea is to have all samples listed horizontally by subject.
# This means that for each subject there will be a column for all samples
# released in DF1, DF2, DF3, and so on. It was done this way since so that
# references to other tables can be made.
statusMsg('Summarizing sample metadata....')

# recode subjectID --- extract subject ID only (i.e., remove '_original', etc.)
samples.names={'subject': 'subjectID'}
samples['subjectID']=dt.Frame([
    d.split('_')[0]
    if d is not None else None
    for d in samples['subjectID'].to_list()[0]
])


# summarize samples by subject and release, and then spread to wide format
samplesSummarized=dt.Frame()
sampleSubjectIDs=dt.unique(samples[f.subjectID!=None,'subjectID']).to_list()[0]
processedSubjectIDs=[]

for id in tqdm(sampleSubjectIDs):
    if id not in processedSubjectIDs:
        
        # pull subject level information
        tmpSamplesBySubjects=samples[f.subjectID==id, :]
        
        # collapse all sampleIDs by release --- this flattens all IDs so that
        # duplicate rows can be removed without removing unique sample IDs
        tmpSamplesBySubjects['idsCollapsed'] = dt.Frame([
            flattenValueArray(
                array=tmpSamplesBySubjects[f.release==d, 'id'].to_list()[0]
            )
            for d in tmpSamplesBySubjects['release'].to_list()[0]
        ])
        
        # spread data by subjectID and release the previous step collapses
        # multiple samples for a release so we can drop duplicate values here
        subjectSamplesSummarized=dt.Frame(
            tmpSamplesBySubjects
            .to_pandas()
            .drop_duplicates(subset=['subjectID','release'],keep='first')
            .pivot(index='subjectID', columns='release', values='idsCollapsed')
            .reset_index()
        )
        
        # bind to parent object
        subjectSamplesSummarized['numberOfSamples']=tmpSamplesBySubjects.nrows
        samplesSummarized=dt.rbind(
            samplesSummarized,
            subjectSamplesSummarized,
            force=True
        )
        
        # store processed ids
        processedSubjectIDs.append(id)

del subjectSamplesSummarized
del tmpSamplesBySubjects
del processedSubjectIDs
del sampleSubjectIDs

# rename columns
samplesSummarized.names={
    'freeze1': 'df1Samples',
    'freeze2': 'df2Samples',
    'freeze3': 'df3Samples',
    'noveldeepwes': 'noveldeepwesSamples',
    'novelrnaseq': 'novelrnaseqSamples',
    'novellrwgs': 'novellrwgsSamples',
    'novelsrwgs': 'novelsrwgsSamples',
    'novelwgs': 'novelwgsSamples',
}

subjects.key='subjectID'
samplesSummarized.key='subjectID'
subjects=subjects[:, :, dt.join(samplesSummarized)]


#//////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# RESHAPE EXPERIMENTS
# Like sample identifiers, experiment IDs need to be collapsed by subject and 
# release before spreading to wide format. This means that for each subjectID
# collapse experiment IDs by release, drop duplicate rows, and spread. Unlike
# the sample metadata, subject identifiers aren't readily available in the
# data. IDs can be joined using sample ID.
statusMsg('Summarizing experiment metadata....')


# join patient identifiers with experiment identifiers via sample identifiers
statusMsg('Joining sample and experiment identifiers....')
experiments.names={'sample':'sampleID'}
experiments['subjectID'] = dt.Frame([
    flattenValueArray(
        array=samples[f.id==d, 'subjectID'][f.subjectID!=None,:].to_list()[0]
    )
    for d in experiments['sampleID'].to_list()[0]
])

# spread experiments by release and subject
statusMsg('Spreading experiments by release and subject....')

experimentsSummarized = dt.Frame()
experimentSubjectIDs = dt.unique(experiments['subjectID']).to_list()[0]
processedSubjectIDs=[]

for id in tqdm(experimentSubjectIDs):
    if id not in processedSubjectIDs:
        
        # pull all subject-experiment rows
        tmpExperimentsBySubject=experiments[f.subjectID==id, :]
        
        # collapse all experiment IDs by release: this flattens all IDs so that
        # duplicate rows can be removed without removing unique sample IDs
        tmpExperimentsBySubject['idsCollapsed'] = dt.Frame([
            flattenValueArray(
                array=tmpExperimentsBySubject[f.release==d, 'id'].to_list()[0]
            )
            for d in tmpExperimentsBySubject['release'].to_list()[0]
        ])
        
        # spread data by subjectID and release the previous step collapses
        # multiple samples for a release so we can drop duplicate values here
        subjectExperimentsSummarized=dt.Frame(
            tmpExperimentsBySubject
            .to_pandas()
            .drop_duplicates(subset=['subjectID','release'],keep='first')
            .pivot(index='subjectID', columns='release', values='idsCollapsed')
            .reset_index()
        )
        
        # bind to parent object
        subjectExperimentsSummarized['numberOfExperiments']=tmpExperimentsBySubject.nrows
        experimentsSummarized=dt.rbind(
            experimentsSummarized,
            subjectExperimentsSummarized,
            force=True
        )
        
        # store processed ids
        processedSubjectIDs.append(id)

# rename columns - run with all key pairs uncommented. It is possible that
# new data has become availble since the prior run. Comment items if a KeyError
# is thrown
experimentsSummarized.names = {
    'freeze1': 'df1Experiments',
    'freeze2': 'df2Experiments',
    'freeze3': 'df3Experiments',
    # 'noveldeepwes': 'noveldeepwesExperiments',
    # 'novelrnaseq': 'novelrnaseqExperiments',
    # 'novellrwgs': 'novellrwgsExperiments',
    'novelsrwgs': 'novelsrwgsExperiments',
    'novelwgs': 'novelwgsExperiments',
}

experimentsSummarized.key='subjectID'
subjects=subjects[:, :, dt.join(experimentsSummarized)]

#//////////////////////////////////////////////////////////////////////////////

# ~ 4 ~
# RESHAPE FILES
statusMsg('Summarizing file metadata....')

# Computing Subject identifier
# Joining the data doesn't really work as there needs to be a unique identifier
# in the files table that is linked to an experiment, sample, or subject. This
# process largely depends on the availability of any identifier that can help
# us link files with another record.
#
# since this table does not reference the experiment table directly, all
# identifiers need to be formatted correctly. If the release is associated with
# one of the main data freezes, than the identifier should be given the suffix
# '_original'. This was formatted differently for other releases.
files['experimentID2']=dt.Frame([
    f'{d[0]}_original'
    if d[1] in ['freeze1','freeze2','freeze3']
    else d[0]
    for d in files[:, (f.experimentID, f.release)].to_tuples()
])


# for rows that do not already have a subject ID, find it in the experiment data
statusMsg('Finding subjectIDs in experiments....')
files['subjectID'] = dt.Frame([
    flattenValueArray(
        array=experiments[f.id==d, 'subjectID'].to_list()[0]
    )
    for d in files['experimentID2'].to_list()[0]
])

# check coverage (this should be zero)
files.nrows - files[f.subjectID!=None,:].nrows


# Summarize data
# The fastest way to summarize the data is to create row counts by subjectID,
# release, and experimentID.
statusMsg('Summarizing data by subject, release, and experiments....')
fileCountsGrouped=files[(f.subjectID!=''), :][
    :, dt.count(), dt.by(f.subjectID, f.release, f.experimentID)
]


# spread the data
fileSubjectIDs = dt.unique(fileCountsGrouped['subjectID']).to_list()[0]
filesSummarized=dt.Frame()
processedSubjectIDs=[]

for id in tqdm(fileSubjectIDs):
    if id not in processedSubjectIDs:
        
        # pull all subject-file rows
        tmpFilesBySubject=fileCountsGrouped[f.subjectID==id, :]
        
        # collapse all experiment IDs by release: this flattens all IDs so that
        # duplicate rows can be removed without removing unique file IDs
        tmpFilesBySubject['idsCollapsed'] = dt.Frame([
            '?entity=rd3_{}_file&hideselect=true&filter=({})'.format(
                d,
                urllib.parse.quote(
                    createUrlFilter(
                        columnName='experimentID',
                        array=tmpFilesBySubject[f.release==d, 'experimentID'].to_list()[0]
                    )
                )
            )
            for d in tmpFilesBySubject['release'].to_list()[0]
        ])
        
        # spread data by subjectID and release the previous step collapses
        # multiple samples for a release so we can drop duplicate values here
        subjectFilesSummarized=dt.Frame(
            tmpFilesBySubject
            .to_pandas()
            .drop_duplicates(subset=['subjectID','release'],keep='first')
            .pivot(index='subjectID', columns='release', values='idsCollapsed')
            .reset_index()
        )
        
        # bind to parent object
        subjectFilesSummarized['numberOfFiles']=sum(tmpFilesBySubject['count'].to_list()[0])
        filesSummarized=dt.rbind(
            filesSummarized,
            subjectFilesSummarized,
            force=True
        )


del subjectFilesSummarized
del tmpFilesBySubject
del fileSubjectIDs
del processedSubjectIDs


# Next, spread the data wide by release and create a dataexplorer URL for all 
# unique experiments by release.
# statusMsg('Spreading file metadata by release....')
# filesSummarized=dt.Frame([
#     {
#         'subjectID': d[0].split('_')[0],
#         'numberOfFiles': sum(filesCountsGrouped[f.subjectID==d[0],'count'].to_list()[0]),
#         d[1]: '?entity=rd3_{}_file&hideselect=true&filter=({})'.format(
#             d[1],
#             urllib.parse.quote(
#                 createUrlFilter(
#                     columnName='experimentID',
#                     array=filesCountsGrouped[
#                         (f.subjectID==d[0]) & (f.release==d[1]), 'experimentID'
#                     ].to_list()[0]
#                 )
#             )
#         )
#     }
#     for d in filesCountsGrouped[:, (f.subjectID, f.release)].to_tuples()
# ])[:, first(f[:]), dt.by(f.subjectID)]


# rename columns - run with all key pairs uncommented. It is possible that
# new data has become availble since the prior run. Comment items if a KeyError
# is thrown. Run the following command to check names:
# > filesSummarized.names
filesSummarized.names = {
    'freeze1': 'df1Files',
    'freeze2': 'df2Files',
    # 'freeze3': 'df3Files',
    # 'noveldeepwes': 'noveldeepwesFiles',
    # 'novelrnaseq': 'novelrnaseqFiles',
    # 'novellrwgs': 'novellrwgsFiles',
    'novelsrwgs': 'novelsrwgsFiles',
    # 'novelwgs': 'novelwgsFiles',
}

# join with subjects
subjects.key='subjectID'
filesSummarized.key='subjectID'
subjects=subjects[:, :, dt.join(filesSummarized)]

# subjects.names={'release': 'emxRelease'}


#///////////////////////////////////////

# import data
statusMsg('Importing data....')

# import subject IDs first
rd3.importData(
    entity='rd3_overview',
    data=subjects['subjectID'].to_pandas().to_dict('records')
)

# import row data
overviewData = subjects.to_pandas().replace({np.nan:None}).to_dict('records')
rd3.updateRows(entity='rd3_overview', data=overviewData)


# update values for a column
# rd3.updateColumn(
#     entity='rd3_overview',
#     attr='numberOfFiles',
#     data=dtFrameToRecords(
#         data=subjects[f.numberOfFiles!=None,['subjectID','numberOfFiles']]
#     )
# )

# rd3.updateColumn(
#     entity='rd3_overview',
#     attr='df1Files',
#     data=dtFrameToRecords(
#         data=subjects[f.df1Files!=None,['subjectID','df1Files']]
#     )
# )

# rd3.updateColumn(
#     entity='rd3_overview',
#     attr='df2Files',
#     data=dtFrameToRecords(
#         data=subjects[f.df2Files!=None,['subjectID','df2Files']]
#     )
# )

# rd3.updateColumn(
#     entity='rd3_overview',
#     attr='novelsrwgsFiles',
#     data=dtFrameToRecords(
#         data=subjects[f.novelsrwgsFiles!=None,['subjectID','novelsrwgsFiles']]
#     )
# )