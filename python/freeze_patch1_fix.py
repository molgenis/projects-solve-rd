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
import os, re
config = rd3tools.load_yaml_config('python/_config.yml')

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


pheno_files_f1 = rd3tools.cluster_list_files(
    path = config['releases']['base'] + config['releases']['freeze1']['phenopacket']
)

pheno_files_f1p1 = rd3tools.cluster_list_files(
    path = config['releases']['base'] + config['releases']['freeze1-patch1']['phenopacket']
)



#//////////////////////////////////////

# ~ 1 ~
# Pull all available PED files stored on the cluster
# and process

pedfiles_raw = rd3tools.cluster_list_files(
    path = config['releases']['base'] + config['releases']['freeze1-patch1']['ped']
)


# remove uncessary files
pedfiles = []
for ped_file in pedfiles_raw:
    if re.search(r'(\.ped|\.ped.cip)$', ped_file.get('file_name')):
        pedfiles.append(ped_file)
del ped_file


# check - should be 4871
len(pedfiles)


# extract contents for PED files (this will take some time)
raw_pedigree_data = []
for file in pedfiles[:25]:
    rd3tools.status_msg('Processing file {}'.format(file['file_name']))
    contents = rd3tools.cluster_read_file(path = file['file_path'])
    data = rd3tools.ped_extract_contents(
        contents = contents,
        ids = subject_ids,
        filename = file['file_name']
    )[0]
    raw_pedigree_data.append(data)


# clean up
del file, contents, data

pedigree_data = []
for ped in raw_pedigree_data:
    if ped['upload']:
        pedigree_data.append(ped)


#//////////////////////////////////////

