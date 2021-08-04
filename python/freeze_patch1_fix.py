#'////////////////////////////////////////////////////////////////////////////
#' FILE: freeze_patch1_fix.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-06-30
#' MODIFIED: 2021-08-04
#' PURPOSE: pull patch1 data and updates cases
#' STATUS: in.progress
#' PACKAGES: rd3tools
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import python.rd3tools as rd3tools
import re
from datetime import datetime
config = rd3tools.load_yaml_config('python/_config.yml')

# @name process__filename
# @description extract subjectID from filepath
# @param data list of dictionaries containing filenames and path
# @return a list of dictionaries with filename, filepath, subjectID
def process__filename(data):
    for d in data:
        match = re.search(r'^(P[0-9]{7})', d.get('filename'))
        if match:
            d['subjectID'] = match.group(1)
        else:
            d['subjectID'] = None

# @name process__filematches
# @description determine if an ID from one object exists in another object
# @param data the main dataset containg IDs you want to use to search another object
# @param ref object to search through
# @param label name of the attribute to create
def process__filematches(data, ref, label = 'status'):
    ids = rd3tools.flatten_attr(data = ref, attr = 'subjectID')
    for d in data:
        if d.get('subjectID') in ids:
            d[label] = 'yes'
        else:
            d[label] = 'no'

#'/////////////////////////////////////

# compile a list of files per release
pheno_files_f1 = rd3tools.cluster_list_files(
    path = config['releases']['base'] + config['releases']['freeze1']['phenopacket'],
    filter = '(.json)$'
)

pheno_files_f1p1 = rd3tools.cluster_list_files(
    path = config['releases']['base'] + config['releases']['freeze1-patch1']['phenopacket'],
    filter = '(.json)$'
)

# extract subject ID from filename
process__filename(data = pheno_files_f1)
process__filename(data = pheno_files_f1p1)

# check to see if a file exists in the other freeze
process__filematches(data = pheno_files_f1, ref = pheno_files_f1p1, label = 'inPatch1')
process__filematches(data = pheno_files_f1p1, ref = pheno_files_f1, label = 'inFreeze1')

# get counts
len(pheno_files_f1)
len(pheno_files_f1p1)
sum(d.get('inPatch1') == 'yes' for d in pheno_files_f1)
sum(d.get('inFreeze1') == 'yes' for d in pheno_files_f1p1)


# using phenopacket files, look for changes in the files
starttime = datetime.utcnow().strftime('%H:%M:%S.%f')[:-4]
patch_1_ids = []
for p in pheno_files_f1p1:
    rd3tools.status_msg('Processing data for {}...'.format(p['subjectID']))
    if p['inFreeze1']:
        # pull matching dictionary in Freeze1 original dataset
        rd3tools.status_msg('ID exists in Freeze1 Original')
        result = rd3tools.find_dict(
            data = pheno_files_f1,
            attr = 'subjectID',
            value = p['subjectID']
        )
        # make sure result exists before evaluating file contents
        if result:
            f1_contents = rd3tools.cluster_read_json(path = result[0]['filepath'])
            p1_contents = rd3tools.cluster_read_json(path = p['filepath'])
            f1 = rd3tools.pheno_extract_contents(
                contents = f1_contents,
                filename = result[0]['filename']
            )
            p1 = rd3tools.pheno_extract_contents(
                contents = p1_contents,
                filename = p['filename']
            )
            likely_p1 = -1
            # check `dateofBirth`
            if p1['dateofBirth']:
                rd3tools.status_msg('Evaluating `dateofBirth`...')
                if f1['dateofBirth']:
                    if (p1['dateofBirth'] != '') and (f1['dateofBirth'] == ''):
                        rd3tools.status_msg('Detected new sex value (likely P1)')
                        likely_p1 += 1
                    elif p1['dateofBirth'] == f1['dateofBirth']:
                        rd3tools.status_msg('Values are identiical')
                else:
                    rd3tools.status_msg('Likely a new value (likely p1)')
                    likely_p1 += 1
            else:
                rd3tools.status_msg('No `dateofBirth` detected')
            # check `sex`
            if p1['sex1']:
                rd3tools.status_msg('Evaluting values in `sex1`...')
                if f1['sex1']:
                    if (p1['sex1'] != '') and (f1['sex1'] == ''):
                        rd3tools.status_msg('Detected new sex value (likely P1)')
                        likely_p1 += 1
                    if (p1['sex1'] in ['F', 'M']) and (f1['sex1'] == 'U'):
                        rd3tools.status_msg('Deteched resolved unknown value (likely P1)')
                        likely_p1 += 1
                else:
                    rd3tools.status_msg('No value in freeze1. Likely a new value (likely P1')
                    likely_p1 += 1
            else:
                rd3tools.status_msg('No `sex1` value detected')
            # check phenotype codes
            if p1['phenotype']:
                rd3tools.status_msg('Evaluating phenotype codes...')
                if f1['phenotype']:
                    if len(p1['phenotype']) > len(f1['phenotype']):
                        rd3tools.status_msg('Phenotype lengths differ (likely P1)')
                        likely_p1 += 1
                    for p in p1['phenotype']:
                        if not (p in f1['phenotype']):
                            rd3tools.status_msg('Detected new phenotype code (likely P1)')
                            likely_p1 += 1
                else:
                    rd3tools.status_msg('Phenotypic data is new (likely P1)')
                    likely_p1 += 1
            else:
                rd3tools.status_msg('No phenotypic data detected')
            # check disease codes
            if p1['disease']:
                rd3tools.status_msg('Evaluating disease codes...')
                if f1['disease']:
                    if len(p1['disease']) > len(f1['disease']):
                        rd3tools.status_msg('Disease code lists differ (likely P1)')
                        likely_p1 += 1
                    for dx in p1['disease']:
                        if not (dx in f1['disease']):
                            rd3tools.status_msg('Detected new diseas code (likely P1)')
                            likely_p1 += 1
                else:
                    rd3tools.status_msg('Disease data is new (likely P1)')
                    likely_p1 += 1
            else:
                rd3tools.status_msg('No diagnostic data detected')
            # check onset codes
            if 'ageOfOnset' in p1:
                rd3tools.status_msg('Evaluating onset code...')
                if 'ageOfOnset' in f1:
                    if p1['ageOfOnset'] != f1['ageOfOnset']:
                        rd3tools.status_msg('Onset code is new (likely P1)')
                        likely_p1 += 1
                else:
                    rd3tools.status_msg('Onset code is new (likely P1)')
                    likely_p1 += 1
            else:
                rd3tools.status_msg('No onset code detected')
            # process counter
            if likely_p1 > -1:
                rd3tools.status_msg('ID {} is likely Patch1'.format(p1['id']))
                patch_1_ids.append(p1)
        else:
            rd3tools.status_msg(
                'Unable to find matching entry for {} despite `inFreeze1 = True`'
                .format(p['subjectID'])
            )
    else:
        rd3tools.status_msg(
            'File for {} not in `freeze1_original`'
            .format(p['subjectID'])
        )

endtime = datetime.utcnow().strftime('%H:%M:%S.%f')[:-4]
rd3tools.status_msg(
    'Completed processing in {}'
    .format(
        datetime.strptime(endtime,'%H:%M:%S.%f') - datetime.strptime(starttime, '%H:%M:%S.%f')
    )
)


#//////////////////////////////////////

# update data in RD3
patch_1_updates = []
for d in patch_1_ids:
    patch_1_updates.append({
        'id': d.get('id') + '_original',
        'patch': 'freeze1_original,freeze1_patch1'
    })

# start molgenis session
rd3 = rd3tools.molgenis(
    url = config['hosts']['prod'],
    token = config['tokens']['prod']
)

# push
rd3.batch_update_one_attr(
    entity = "rd3_freeze1_subject",
    attr = 'patch',
    values = patch_1_updates
)

rd3.batch_update_one_attr(
    entity = "rd3_freeze1_subjectinfo",
    attr = 'patch',
    values = patch_1_updates
)

#//////////////////////////////////////

# pull metadata from RD3
# freeze_subjects = rd3.get(
#     entity = config['releases']['freeze1-patch1']['subject'],
#     attributes='id,subjectID,sex1'
# )
# subject_ids = rd3tools.flatten_attr(data = freeze_subjects, attr = 'subjectID')

# pull file metadata
# rd3_phenopacket_metadata = rd3.get(
#     entity = config['releases']['freeze1-patch1']['file'],
#     attributes = 'EGA,name,md5,patch,typeFile,dateCreated',
#     q = 'typeFile==json'
# )

# rd3tools.flatten_attr(rd3_phenopacket_metadata, 'dateCreated', distinct=True)
# rd3tools.find_dict(rd3_phenopacket_metadata, 'dateCreated', '2021-05-14')[:10]

# Create filename column
# for freeze_file in rd3_phenopacket_metadata:
#     freeze_file['filename'] = os.path.basename(freeze_file['name'])
#     freeze_file['filename'] = re.split(r'(\.[0-9]{11,}\.cip)', freeze_file['filename'])[0]


#'////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Pull all available PED files stored on the cluster and process

freeze_subjects = rd3.get(
    entity = config['releases']['freeze1']['subject'],
    attributes='id,subjectID,sex1'
)
subject_ids = rd3tools.flatten_attr(data = freeze_subjects, attr = 'subjectID')


# @name ped__extract__id
# @description extract FamilyID from filename
# @param data dataset to process (output from `cluster_list_files`)
# @return a list of dictionaries
def ped__extract__id(data):
    for d in data:
        d['familyID'] = re.sub('((.ped)|(.ped.cip))$', '', d['filename'])


#'/////////////////////////////////////

ped_files_f1 = rd3tools.cluster_list_files(
    path = config['releases']['base'] + config['releases']['freeze1']['ped'],
    filter = '((.ped)|(.ped.cip))$'
)

ped_files_p1 = rd3tools.cluster_list_files(
    path = config['releases']['base'] + config['releases']['freeze1-patch1']['ped'],
    filter = '((.ped)|(.ped.cip))$'
)


# extract family ID from filename
ped__extract__id(data = ped_files_f1)
ped__extract__id(data = ped_files_p1)


# extract metadata
ped_f1 = []
ped_p1 = []


# freeze1 files
for file in ped_files_f1:
    rd3tools.status_msg('Processing file:', file['filename'])
    raw = rd3tools.cluster_read_file(file['filepath'])
    data = rd3tools.ped_extract_contents(
        contents = raw,
        ids = subject_ids,
        filename = file['filename']
    )
    if len(data) > 1:
        for line in data:
            if line['upload']:
                ped_f1.append(line)
    else:
        if data[0]['upload']:
            ped_f1.append(data[0])

del file, raw, data, line

# patch1 files
for file in ped_files_p1:
    rd3tools.status_msg('Processing file:', file['filename'])
    raw = rd3tools.cluster_read_file(file['filepath'])
    data = rd3tools.ped_extract_contents(
        contents = raw,
        ids = subject_ids,
        filename = file['filename']
    )
    if len(data) > 1:
        for line in data:
            if line['upload']:
                ped_p1.append(line)
    else:
        if data[0]['upload']:
            ped_p1.append(data[0])

del file, raw, data, line


# compare files
for p in ped_p1[:1]:
    p['isPatch1'] = False
    r = rd3tools.find_dict(ped_f1, 'id', p['id'])
    if r:
        f = r[0]
        if str(p['mid'] != 'None'):
            if str(f['mid'] == 'None'):
                p['isPatch1'] = True
        if str(p['pid'] != 'None'):
            if str(f['pid'] == 'None'):
                p['isPatch1'] = True
        if str(p['sex1'] != 'None'):
            if str(f['sex1'] == 'None'):
                p['isPatch1'] = True
        if str(p['clinical_status'] != 'None'):
            if str(f['clinical_status'] == 'None'):
                p['isPatch1'] = True
            else:
                if f['clinical_status'] != p['clinical_status']:
                    p['isPatch1'] = True


