#'////////////////////////////////////////////////////////////////////////////
#' FILE: freeze_patch1_fix.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-06-30
#' MODIFIED: 2021-06-30
#' PURPOSE: pull patch1 data and updates cases
#' STATUS: in.progress
#' PACKAGES: rd3tools
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import python.rd3tools as rd3tools
import re
config = rd3tools.load_yaml_config('python/_config.yml')


def process__filename(data):
    for d in data:
        match = re.search(r'^(P[0-9]{7})', d.get('filename'))
        if match:
            d['subjectID'] = match.group(1)
        else:
            d['subjectID'] = None


def process__filematches(data, ref, label = 'status'):
    ids = rd3tools.flatten_attr(data = ref, attr = 'subjectID')
    for d in data:
        if d.get('subjectID') in ids:
            d[label] = 'yes'
        else:
            d[label] = 'no'


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

patch_1_ids = []
for p in pheno_files_f1p1[:100]:
    if p['inFreeze1']:
        rd3tools.status_msg('Processing data for {}...'.format(p['subjectID']))
        result = rd3tools.find_dict(
            data = pheno_files_f1,
            attr = 'subjectID',
            value = p['subjectID']
        )[0]
        if result:
            f1_contents = rd3tools.cluster_read_json(path = result['filepath'])
            p1_contents = rd3tools.cluster_read_json(path = p['filepath'])
            f1 = rd3tools.pheno_extract_contents(
                contents = f1_contents,
                filename = result['filename']
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



#//////////////////////////////////////


# start molgenis session
rd3 = rd3tools.molgenis(
    url = config['hosts']['acc'],
    token = config['tokens']['acc']
)

# pull metadata from RD3
freeze_subjects = rd3.get(
    entity = config['releases']['freeze1-patch1']['subject'],
    attributes='id,subjectID,sex1'
)
subject_ids = rd3tools.flatten_attr(data = freeze_subjects, attr = 'subjectID')

# pull file metadata
rd3_phenopacket_metadata = rd3.get(
    entity = config['releases']['freeze1-patch1']['file'],
    attributes = 'EGA,name,md5,patch,typeFile,dateCreated',
    q = 'typeFile==json'
)

rd3tools.flatten_attr(rd3_phenopacket_metadata, 'dateCreated', distinct=True)
rd3tools.find_dict(rd3_phenopacket_metadata, 'dateCreated', '2021-05-14')[:10]

# Create filename column
# for freeze_file in rd3_phenopacket_metadata:
#     freeze_file['filename'] = os.path.basename(freeze_file['name'])
#     freeze_file['filename'] = re.split(r'(\.[0-9]{11,}\.cip)', freeze_file['filename'])[0]


#//////////////////////////////////////

# ~ 1 ~
# Pull all available PED files stored on the cluster
# and process

# pedfiles_raw = rd3tools.cluster_list_files(
#     path = config['releases']['base'] + config['releases']['freeze1-patch1']['ped']
# )

# remove uncessary files
# pedfiles = []
# for ped_file in pedfiles_raw:
#     if re.search(r'(\.ped|\.ped.cip)$', ped_file.get('file_name')):
#         pedfiles.append(ped_file)
# del ped_file

# check - should be 4871
# len(pedfiles)

# extract contents for PED files (this will take some time)
# raw_pedigree_data = []
# for file in pedfiles[:25]:
#     rd3tools.status_msg('Processing file {}'.format(file['file_name']))
#     contents = rd3tools.cluster_read_file(path = file['file_path'])
#     data = rd3tools.ped_extract_contents(
#         contents = contents,
#         ids = subject_ids,
#         filename = file['file_name']
#     )[0]
#     raw_pedigree_data.append(data)


# # clean up
# del file, contents, data

# pedigree_data = []
# for ped in raw_pedigree_data:
#     if ped['upload']:
#         pedigree_data.append(ped)


# #//////////////////////////////////////

