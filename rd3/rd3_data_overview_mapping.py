#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_data_overview_mapping.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-05-16
#' MODIFIED: 2022-05-30
#' PURPOSE: generate dataset for rd3_overview
#' STATUS: stable
#' PACKAGES: **see below**
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////


from rd3.api.molgenis import Molgenis
from rd3.utils.utils import (
    statusMsg,
    flattenBoolArray, flattenStringArray, flattenValueArray
)

from datatable import dt, f, first
from os import environ
from dotenv import load_dotenv
import numpy as np
import urllib


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

# collapse sex1 >> genderAtBirth
statusMsg('Collapsing sex....')
subjects['genderAtBirth'] = dt.Frame([
    flattenValueArray(
        array=subjects[f.subjectID==d,f.sex1][f.sex1!=None,:].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])


# collapse family identifier >> belongsToFamily
statusMsg('Collapsing family identifiers....')
subjects['belongsToFamily'] = dt.Frame([
    flattenValueArray(
        array=subjects[f.subjectID==d,f.fid][f.fid!=None,:].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])

# collapse maternal identifier >> belongsToMother
statusMsg('Collapsing maternal identifiers....')
subjects['belongsToMother'] = dt.Frame([
    flattenValueArray(
        array=subjects[f.subjectID==d, f.mid][f.mid!=None,:].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])

subjects['belongsToMother'] = dt.Frame([
    d.split('_original')[0]
    for d in subjects['belongsToMother'].to_list()[0]
    if d is not None
])


# collapse paternal identifier >> belongsToFather
statusMsg('Collapsing paternal identifiers....')
subjects['belongsToFather'] = dt.Frame([
    flattenValueArray(
        array=subjects[f.subjectID==d,f.pid][f.pid!=None,:].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])

subjects['belongsToFather'] = dt.Frame([
    d.split('_original')[0]
    for d in subjects['belongsToFather'].to_list()[0]
    if d is not None
])


# collapse clinical status >> affectedStatus
statusMsg('Collapsing clinical status....')
subjects['affectedStatus'] = dt.Frame([
    flattenBoolArray(
        array=subjects[f.subjectID==d, f.clinical_status][
            f.clinical_status!= None, :
        ].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])


# collapse disease >> clinicalDiagnosis
statusMsg('Collapsing diagnoses....')
subjects['clinicalDiagnosis'] = dt.Frame([
    flattenStringArray(
        array=subjects[f.subjectID==d, f.disease][
            f.disease != None, :
        ].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])


# collapse phenotype >> observedPhenotype
statusMsg('Collapsing observed phenotypes....')
subjects['observedPhenotype'] = dt.Frame([
    flattenStringArray(
        array=subjects[
            f.subjectID==d, f.phenotype
        ][f.phenotype!=None,:].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])


# collapse hasNotPhenotype >> unobservedPhenotype
statusMsg('Collapsing unobserved phenotypes....')
subjects['unobservedPhenotype'] = dt.Frame([
    flattenStringArray(
        array=subjects[f.subjectID==d,f.hasNotPhenotype][
            f.hasNotPhenotype != None, :
        ].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])


# collapse organisation >> primaryAffiliatedInstitute
statusMsg('Collapsing organisation....')
subjects['primaryAffiliatedInstitute'] = dt.Frame([
    flattenValueArray(
        array=subjects[f.subjectID==d, f.organisation][
            f.organisation!=None, :
        ].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])


# collapse ERN >> participatesInStudy
statusMsg('Collapsing ERNs....')
subjects['participatesInStudy'] = dt.Frame([
    flattenValueArray(
        array=subjects[f.subjectID==d, f.ERN].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0][:1]
])


# collapse solved >> solvedStatus
statusMsg('Collapsing solved status....')
subjects['solvedStatus'] = dt.Frame([
    flattenBoolArray(
        array=subjects[f.subjectID==d, f.solved].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])

# collapse patch >> partofDataRelease
statusMsg('Collapsing patch information....')
subjects['partOfDataRelease'] = dt.Frame([
    flattenStringArray(
        array=subjects[
            f.subjectID==d, f.patch
        ][f.patch!=None, :].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])


# collapse release
statusMsg('Collapsing emx-release....')
subjects['emxRelease'] = dt.Frame([
    flattenValueArray(
        array=subjects[f.subjectID==d, f.release][
            f.release != None, :
        ].to_list()[0]
    )
    for d in subjects[:, f.subjectID].to_list()[0]
])

# DISTINCT RECORDS ONLY
# since all information has been flattend and repeated by subject, it is
# possible to select only the distinct records.
statusMsg('Complete! Selecting distinct records only....')

del subjects['id']
del subjects['sex1']
del subjects['fid']
del subjects['mid']
del subjects['pid']
del subjects['clinical_status']
del subjects['disease']
del subjects['phenotype']
del subjects['hasNotPhenotype']
del subjects['organisation']
del subjects['ERN']
del subjects['solved']
del subjects['patch']

subjects = subjects[:, first(f[:]), dt.by(f.subjectID)]

#//////////////////////////////////////////////////////////////////////////////

# ~ 2 ~ 
# RESHAPE SAMPLES

statusMsg('Summarizing sample metadata....')

samples.names={'subject': 'subjectID'}

# spread samples by release and subject
statusMsg('Spreading samples by release and subject....')
samplesSummarized=dt.Frame([
    {
        'subjectID': d[2].split('_')[0],
        'numberOfSamples': samples[f.subjectID==d[2], 'id'].nrows,
        d[1]: flattenValueArray(
            array = samples[
                (f.subjectID==d[2]) & (f.release==d[1]), 'id'
            ].to_list()[0]
        )
    }
    for d in samples[:,(f.id,f.release,f.subjectID)].to_tuples()
    if d[2] is not None
])[:, first(f[:]), dt.by(f.subjectID)]


# rename columns
samplesSummarized.names={
    'freeze1': 'df1Samples',
    'freeze2': 'df2Samples',
    'freeze3': 'df3Samples',
    'noveldeepwes': 'noveldeepwesSamples',
    'novelrnaseq': 'novelrnaseqSamples',
    'novelsrwgs': 'novelsrwgsSamples',
    'novelwgs': 'novelwgsSamples',
}

subjects.key='subjectID'
samplesSummarized.key='subjectID'
subjects=subjects[:, :, dt.join(samplesSummarized)]


#//////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# RESHAPE EXPERIMENTS

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
experimentSummarized = dt.Frame([
    {
        'subjectID': d[2].split('_')[0],
        'numberOfExperiments': experiments[f.subjectID == d[2], 'id'].nrows,
        d[1]: flattenValueArray(
            array=experiments[
                (f.subjectID==d[2]) & (f.release==d[1]), 'id' 
            ][
                f.id != None, :
            ].to_list()[0]
        )
    }
    for d in experiments[:, (f.id, f.release, f.subjectID)].to_tuples()
    if d[2] is not None
])[:, first(f[:]), dt.by(f.subjectID)]


# rename columns - run with all key pairs uncommented. It is possible that
# new data has become availble since the prior run. Comment items if a KeyError
# is thrown
experimentSummarized.names = {
    'freeze1': 'df1Experiments',
    'freeze2': 'df2Experiments',
    'freeze3': 'df3Experiments',
    # 'noveldeepwes': 'noveldeepwesExperiments',
    # 'novelrnaseq': 'novelrnaseqExperiments',
    'novelsrwgs': 'novelsrwgsExperiments',
    'novelwgs': 'novelwgsExperiments',
}

experimentSummarized.key='subjectID'
subjects=subjects[:, :, dt.join(experimentSummarized)]

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
filesCountsGrouped=files[(f.subjectID!=''), :][
    :, dt.count(), dt.by(f.subjectID, f.release, f.experimentID)
]

# Next, spread the data wide by release and create a dataexplorer URL for all 
# unique experiments by release.
statusMsg('Spreading file metadata by release....')
filesSummarized=dt.Frame([
    {
        'subjectID': d[0].split('_')[0],
        'numberOfFiles': sum(filesCountsGrouped[f.subjectID==d[0],'count'].to_list()[0]),
        d[1]: '?entity=rd3_{}_file&hideselect=true&filter=({})'.format(
            d[1],
            urllib.parse.quote(
                createUrlFilter(
                    columnName='experimentID',
                    array=filesCountsGrouped[
                        (f.subjectID==d[0]) & (f.release==d[1]), 'experimentID'
                    ].to_list()[0]
                )
            )
        )
    }
    for d in filesCountsGrouped[:, (f.subjectID, f.release)].to_tuples()
])[:, first(f[:]), dt.by(f.subjectID)]


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
