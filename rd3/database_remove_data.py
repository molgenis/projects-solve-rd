"""Remove records from RD3
Delete records from all RD3 tables
"""

from os import environ, system
from tqdm import tqdm
from dotenv import load_dotenv
from datatable import dt, f, fread
from rd3tools.molgenis import Molgenis
from rd3tools.utils import print2, flatten_data, timestamp
load_dotenv()


def clear():
    """Clear terminal"""
    system('clear')


# connect to RD3 --- choose PROD or ACC
rd3 = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])
# rd3 = Molgenis(environ['MOLGENIS_ACC_HOST'])
# rd3.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

# load reference lists
ids_dt = fread('./data/ids_to_remove.csv')

for column_name in ids_dt.names:
    if 'Local' in column_name:
        del ids_dt[column_name]

ids_dt.names = {
    'PhenoStore_ID': 'id',
    'Family_ID': 'fid'
}

ids_to_remove = ids_dt['id'].to_list()[0]

# ///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Drop records from RD3 Stats schema
print2('Drop records from rd3stats_treedata...')

tree_dt = dt.Frame(
    rd3.get(
        entity='rd3stats_treedata',
        batch_size=10000,
        attributes='id,subjectID'
    )
)

tree_dt['should_remove'] = dt.Frame([
    value in ids_to_remove
    for value in tree_dt['subjectID'].to_list()[0]
])

if tree_dt[f.should_remove, :].nrows > 0:
    print2('Removing records from rd3stats_treedata....')
    rd3.delete_list(
        entity='rd3stats_treedata',
        entities=tree_dt[f.should_remove, 'id'].to_list()[0]
    )

del tree_dt

# ///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Drop records from RD3_Portal
print2('Drop records from rd3_portal....')

# ~ 2a ~
# Delete from rd3_portal_cluster
print2('Identifying records from rd3_portal_cluster....')

cluster_files_dt = dt.Frame(
    rd3.get(
        entity='rd3_portal_cluster',
        attributes='id,name',
        batch_size=10000
    )
)
del cluster_files_dt['_href']

cluster_files_dt['subjectID'] = dt.Frame([
    value.split('.')[0] for value in cluster_files_dt['name'].to_list()[0]
])

cluster_files_dt['should_remove'] = dt.Frame([
    value in ids_to_remove for value in cluster_files_dt['subjectID'].to_list()[0]
])

if cluster_files_dt[f.should_remove, :].nrows > 0:
    print('Deleting records from rd3_portal_cluster....')
    rd3.delete_list(
        entity='rd3_portal_cluster',
        entities=cluster_files_dt['id'].to_list()[0]
    )


del cluster_files_dt

# ///////////////////////////////////////

# ~ 2b ~
# Drop data from rd3_portal_recontact_solved
print2('Identifying records from rd3_portal_recontact_solved....')

solved_status_dt = dt.Frame(
    rd3.get(
        entity='rd3_portal_recontact_solved',
        attributes='id,subject',
        batch_size=10000
    )
)

solved_status_dt['should_remove'] = dt.Frame([
    value in ids_to_remove for value in solved_status_dt['subject'].to_list()[0]
])

if solved_status_dt[f.should_remove, :].nrows > 0:
    print2('\t- Deleting records')
    rd3.delete_list(
        entity='rd3_portal_recontact_solved',
        entities=solved_status_dt[f.should_remove, 'id'].to_list()[0]
    )


del solved_status_dt

# ///////////////////////////////////////

# ~ 2c ~
# Remove data from rd3_portal_gpap
print2('Identifying records from rd3_portal_gpap....')

gpap_dt = dt.Frame(
    rd3.get(
        entity='rd3_portal_gpap',
        attributes='participantID,rdconnectID',
        batch_size=10000
    )
)

gpap_dt['should_remove'] = dt.Frame([
    value in ids_to_remove for value in gpap_dt['participantID'].to_list()[0]
])


if gpap_dt[f.should_remove, :].nrows > 0:
    print2('\t- Deleting records')
    rd3.delete_list(
        entity='rd3_portal_gpap',
        entities=gpap_dt[f.should_remove, 'rdconnectID'].to_list()[0]
    )

del gpap_dt

# ///////////////////////////////////////

# ~ 2d ~
# Delete records from rd3_portal_cluster_freeze1
print2('Identifying records from rd3_portal_cluster_freeze*...')

cluster_tables = [
    'rd3_portal_cluster_freeze1',
    'rd3_portal_cluster_freeze1-patch1',
    'rd3_portal_cluster_freeze1-patch3',
    'rd3_portal_cluster_freeze2',
    'rd3_portal_cluster_freeze2-patch1',
    'rd3_portal_cluster_freeze3',
]

for table in cluster_tables:
    print2(f'Identifying records in {table}....')

    cluster_freeze_dt = dt.Frame(
        rd3.get(
            entity=table,
            attributes='subjectID,filepath',
            batch_size=10000
        )
    )

    # del cluster_freeze_dt['_href']

    cluster_freeze_dt['should_remove'] = dt.Frame([
        value in ids_to_remove for value in cluster_freeze_dt['subjectID'].to_list()[0]
    ])

    cluster_freeze_dt_nrows = cluster_freeze_dt[f.should_remove, :].nrows
    if cluster_freeze_dt_nrows > 0:
        print2(f'\tDeleting records ({cluster_freeze_dt_nrows})')
        rd3.delete_list(
            entity=table,
            entities=cluster_freeze_dt[
                f.should_remove, f.filepath
            ].to_list()[0]
        )
    else:
        print('\tNo records to delete')

# ///////////////////////////////////////

# ~ 2e ~
# Deleting records from rd3_portal_cluster_ped
print2('Identifying records in rd3_portal_cluster_ped....')

cluster_ped_dt = dt.Frame(
    rd3.get(
        entity='rd3_portal_cluster_ped',
        attributes='id,subjectID',
        batch_size=10000
    )
)

del cluster_ped_dt['_href']

cluster_ped_dt['should_remove'] = dt.Frame([
    value in ids_to_remove for value in cluster_ped_dt['subjectID'].to_list()[0]
])

if cluster_ped_dt[f.should_remove, :].nrows > 0:
    print2('\tDeleting records')
    rd3.delete_list(
        entity='rd3_portal_cluster_ped',
        entities=cluster_ped_dt[f.should_remove, 'id'].to_list()[0]
    )

# ///////////////////////////////////////

# ~ 2f ~
# Deleting records from rd3_portal_cluster_phenopacket
print2('Identifying records in rd3_portal_cluster_phenopacket....')

cluster_pheno_dt = dt.Frame(
    rd3.get(
        entity='rd3_portal_cluster_phenopacket',
        attributes='phenopacketsID,subjectID',
        batch_size=10000
    )
)

cluster_pheno_dt['should_remove'] = dt.Frame([
    value in ids_to_remove for value in cluster_pheno_dt['subjectID'].to_list()[0]
])


if cluster_pheno_dt[f.should_remove, :].nrows > 0:
    print2('\tDeleting records')
    rd3.delete_list(
        entity='rd3_portal_cluster_phenopacket',
        entities=cluster_pheno_dt[
            f.should_remove, 'phenopacketsID'
        ].to_list()[0]
    )

# ///////////////////////////////////////

# ~ 2g ~
# Deleting records from rd3_portal_novelomics_shipment
print2('Identifying records from rd3_portal_novelomics_shipment....')

shipment_dt = dt.Frame(
    rd3.get(
        entity='rd3_portal_novelomics_shipment',
        attributes='participant_subject,molgenis_id',
        batch_size=10000
    )
)

del shipment_dt['_href']

shipment_dt['should_remove'] = dt.Frame([
    value in ids_to_remove for value in shipment_dt['participant_subject'].to_list()[0]
])

if shipment_dt[f.should_remove, :].nrows > 0:
    print2('\tDeleting records')
    rd3.delete_list(
        entity='rd3_portal_novelomics_shipment',
        entities=shipment_dt[f.should_remove, 'molgenis_id'].to_list()[0]
    )

# ///////////////////////////////////////

# ~ 2h ~
# Deleting records from rd3_portal_novelomics_experiment
print2('Identifying records from rd3_portal_novelomics_experiment....')

experiments_dt = dt.Frame(
    rd3.get(
        entity='rd3_portal_novelomics_experiment',
        attributes='subject_id,molgenis_id',
        batch_size=10000
    )
)

del experiments_dt['_href']

experiments_dt['should_remove'] = dt.Frame([
    value in ids_to_remove
    for value in experiments_dt['subject_id'].to_list()[0]
])

if experiments_dt[f.should_remove, :].nrows > 0:
    print2('\tDeleting records....')
    rd3.delete_list(
        entity='rd3_portal_novelomics_experiment',
        entities=experiments_dt[f.should_remove, 'molgenis_id'].to_list()[0]
    )

# ///////////////////////////////////////

# ~ 2i ~
# Deleting records from rd3_portal_release_patches
print2('Identifying records from rd3_portal_release_patches....')

tables = [
    # 'rd3_portal_release_patches',
    'rd3_portal_release_freeze2',
    'rd3_portal_release_freeze3',
    # 'rd3_portal_release_new',
    'rd3_portal_release_novelwgs',
]

for table in tables:
    print2(f"Identifying records in {table}....")
    release_dt = dt.Frame(
        rd3.get(table, attributes='id,samples_subject', batch_size=10000)
    )
    # del release_dt['_href']

    release_dt['should_remove'] = dt.Frame([
        value in ids_to_remove for value in release_dt['samples_subject'].to_list()[0]
    ])

    if release_dt[f.should_remove, :].nrows > 0:
        print2(f'\tDeleting records ({release_dt[f.should_remove, :].nrows})')
        rd3.delete_list(
            entity='rd3_portal_release_patches',
            entities=release_dt[f.should_remove, 'id'].to_list()[0]
        )
    else:
        print2('\tNo records found')

# ///////////////////////////////////////

# ~ 2j ~
# Delete records from rd3_portal_release_experiments
print2('Identifying records in rd3_portal_release_experiments....')

release_expr_dt = dt.Frame(
    rd3.get(
        entity='rd3_portal_release_experiments',
        attributes='subject_id,molgenis_id',
        batch_size=10000
    )
)

del release_expr_dt['_href']

release_expr_dt['should_remove'] = dt.Frame([
    value in ids_to_remove for value in release_expr_dt['subject_id'].to_list()[0]
])

if release_expr_dt[f.should_remove, :].nrows > 0:
    print2(f'\tDeleting records ({release_expr_dt[f.should_remove, :].nrows})')
    rd3.delete_list(
        entity='rd3_portal_release_experiments',
        entities=release_expr_dt[f.should_remove, 'molgenis_id'].to_list()[0]
    )

# ///////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Deleting records from solverdportal_experiments
print2('Identifying records in solverdportal_experiments....')

solve_portal_expr_dt = dt.Frame(
    rd3.get(
        entity='solverdportal_experiments',
        attributes='participant_id,rdconnect_id',
        batch_size=10000
    )
)

del solve_portal_expr_dt['_href']

solve_portal_expr_dt['should_remove'] = dt.Frame([
    value in ids_to_remove for value in solve_portal_expr_dt['participant_id'].to_list()[0]
])

if solve_portal_expr_dt[f.should_remove, :].nrows > 0:
    print2('\tDeleting records')
    rd3.delete_list(
        entity='solverdportal_experiments',
        entities=solve_portal_expr_dt[
            f.should_remove, 'rdconnect_id'
        ].to_list()[0]
    )


# ///////////////////////////////////////////////////////////////////////////////

# ~ 4 ~
# Delete records from new RD3 model


# ~ 4a ~
# Drop records from solverd_overview
print2('Identifying records from rd3_overview and solverd_overview....')

overview_dat = rd3.get(
    entity='solverd_overview',
    attributes='subjectID,fid,samples,experiments,files,partOfRelease',
    batch_size=10000
)

overview_dt = dt.Frame(flatten_data(
    overview_dat, 'sampleID|experimentID|id|value'))

overview_dt['should_remove'] = dt.Frame([
    value in ids_to_remove
    for value in overview_dt['subjectID'].to_list()[0]
])

# overview_dt[f.should_remove, :].to_csv('./data/record_ids_to_add.csv')
removed_dt = overview_dt[f.should_remove, :]

if overview_dt[f.should_remove, :].nrows > 0:
    print2('Deleting records')
    rd3.delete_list(
        entity='solverd_overview',
        entities=overview_dt[f.should_remove, 'subjectID'].to_list()[0]
    )
    rd3.delete_list(
        entity='rd3_overview',
        entities=overview_dt[f.should_remove, 'subjectID'].to_list()[0]
    )


# ///////////////////////////////////////

# ~ 4b ~
# Delete records from solverd_files
all_files_dt = []
for batch in tqdm(range(0, len(ids_to_remove), 10)[8:]):
    QUERY = ','.join([
        f"subjectID=={id}"
        for id in ids_to_remove[batch:batch+10]
    ])

    raw_files_dt = rd3.get(
        entity='solverd_files',
        attributes='EGA,subjectID',
        q=QUERY
    )

    if bool(raw_files_dt):
        all_files_dt = all_files_dt + raw_files_dt


files_dt = dt.Frame(flatten_data(all_files_dt, 'subjectID'))

rd3.delete_list('solverd_files', files_dt['EGA'].to_list()[0])

# ///////////////////////////////////////

# ~ 4c ~
# Delete records from solverd_labinfo, solverd_samples, solverd_subjects
print2('Identifying records in solverd....')


# find samples that should be removed
samples = rd3.get('solverd_samples', attributes='sampleID,belongsToSubject')
samples_dt = dt.Frame(flatten_data(samples, 'subjectID'))

samples_dt['should_remove'] = dt.Frame([
    value in ids_to_remove
    for value in samples_dt['belongsToSubject'].to_list()[0]
])

samples_to_remove = samples_dt[f.should_remove, 'sampleID'].to_list()[0]

# identify experiments that should be removed
labinfo = rd3.get('solverd_labinfo', attributes='experimentID,sampleID')
labinfo_dt = dt.Frame(flatten_data(labinfo, 'sampleID'))

labinfo_dt['should_remove'] = dt.Frame([
    value in samples_to_remove
    for value in labinfo_dt['sampleID'].to_list()[0]
])

if labinfo_dt[f.should_remove, :].nrows > 0:
    print2('Deleting records')
    rd3.delete_list(
        'solverd_labinfo',
        labinfo_dt[f.should_remove, 'experimentID'].to_list()[0]
    )


if samples_dt[f.should_remove, :].nrows > 0:
    print2('Deleting records')
    rd3.delete_list(
        'solverd_samples',
        samples_dt[f.should_remove, 'sampleID'].to_list()[0]
    )

# delete records
print2('Deleting records from solverd_subjects....')
rd3.delete_list('solverd_subjects', ids_to_remove)
rd3.delete_list('solverd_subjectinfo', ids_to_remove)

# ///////////////////////////////////////////////////////////////////////////////

# ~ 5 ~
# Delete records from rd3_* sub packages

# ~ 5a ~
# Delete data from rd3_freeze1

freeze1_samples = rd3.get('rd3_freeze1_sample', attributes='id,subject')
freeze1_samples_dt = dt.Frame(flatten_data(freeze1_samples, 'subjectID'))

freeze1_samples_dt['should_remove'] = dt.Frame([
    value in ids_to_remove
    for value in freeze1_samples_dt['subject'].to_list()[0]
])

if freeze1_samples_dt[f.should_remove, :].nrows > 0:
    print('Remove records from the rest of the tables')


# ///////////////////////////////////////

# ~ 5b ~
# Delete data from rd3_freeze2

# get samples
freeze2_samples = rd3.get('rd3_freeze2_sample', attributes='id,subject')
freeze2_samples_dt = dt.Frame(flatten_data(freeze2_samples, 'subjectID'))

freeze2_samples_dt['should_remove'] = dt.Frame([
    value in ids_to_remove
    for value in freeze2_samples_dt['subject'].to_list()[0]
])

if freeze2_samples_dt[f.should_remove, :].nrows > 0:
    print2('Delete records from files, samples, and subjects')

# get experiments
# freeze2_labinfo = rd3.get('rd3_freeze2_labinfo', attributes='id,sample')
# freeze2_labinfo_dt = dt.Frame(flatten_data(freeze2_labinfo, 'id'))

# ///////////////////////////////////////

# ~ 5c ~
# Delete data from rd3_freeze3
# freeze3_files_dt = rd3.get(
#     'rd3_freeze3_file',
#     attributes='EGA,subjectID',
#     batch_size=10000
# )

# get experiments
freeze3_labinfo = rd3.get('rd3_freeze3_labinfo', attributes='id,sample')
freeze3_labinfo_dt = dt.Frame(flatten_data(freeze3_labinfo, 'id'))

# get samples
freeze3_samples = rd3.get('rd3_freeze3_sample', attributes='id,subject')
freeze3_samples_dt = dt.Frame(flatten_data(freeze3_samples, 'subjectID'))

freeze3_samples_dt['should_remove'] = dt.Frame([
    value in ids_to_remove
    for value in freeze3_samples_dt['subject'].to_list()[0]
])

freeze3_labinfo_dt['should_remove'] = dt.Frame([
    value in freeze3_samples_dt[f.should_remove, 'id'].to_list()[0]
    for value in freeze3_labinfo_dt['sample'].to_list()[0]
])

if freeze3_labinfo_dt[f.should_remove, :].nrows:
    print('Deleting records from labinfo....')
    rd3.delete_list(
        'rd3_freeze3_labinfo',
        freeze3_labinfo_dt[f.should_remove, 'id'].to_list()[0]
    )

if freeze3_samples_dt[f.should_remove, :].nrows:
    print('Removing records from rd3_freeze3_samples')
    rd3.delete_list(
        'rd3_freeze3_sample',
        freeze3_samples_dt[f.should_remove, 'id'].to_list()[0]
    )

freeze3_samples_dt[f.should_remove, :].to_csv('freeze3_samples_removed.csv')
freeze3_labinfo_dt[f.should_remove, :].to_csv(
    'freeze3_experiments_removed.csv')

rd3.delete_list('rd3_freeze3_subjectinfo', ids_to_remove)
rd3.delete_list('rd3_freeze3_subject', ids_to_remove)

# ///////////////////////////////////////

# ~ 5d ~
# Delete data from noveldeepwes

# get samples
deepwes_samples = rd3.get('rd3_noveldeepwes_sample', attributes='id,subject')
deepwes_samples_dt = dt.Frame(flatten_data(deepwes_samples, 'subjectID'))

deepwes_samples_dt['should_remove'] = dt.Frame([
    value in ids_to_remove
    for value in deepwes_samples_dt['subject'].to_list()[0]
])

if deepwes_samples_dt[f.should_remove, :].nrows > 0:
    print2('Delete records')


rd3.delete_list('rd3_noveldeepwes_subjectinfo', ids_to_remove)
rd3.delete_list('rd3_noveldeepwes_subject', ids_to_remove)

del deepwes_samples, deepwes_samples_dt

# ///////////////////////////////////////

# ~ 5e ~
# Delete data from rd3_novelepigenome_sample

epigenome_samples = rd3.get('rd3_novelepigenome_sample',
                            attributes='id,subject')

epigenome_samples_dt = dt.Frame(flatten_data(epigenome_samples, 'subjectID'))
epigenome_samples_dt['should_remove'] = dt.Frame([
    value in ids_to_remove
    for value in epigenome_samples_dt['subject'].to_list()[0]
])

if epigenome_samples_dt[f.should_remove, :].nrows > 0:
    print2('Delete records')


rd3.delete_list('rd3_novelepigenome_subjectinfo', ids_to_remove)
rd3.delete_list('rd3_novelepigenome_subject', ids_to_remove)

del epigenome_samples, epigenome_samples_dt

# ///////////////////////////////////////

# ~ 5f ~
# Delete data from rd3_novellrwgs_sample

lrwgs_samples = rd3.get('rd3_novellrwgs_sample', attributes='id,subject')
lrwgs_samples_dt = dt.Frame(flatten_data(lrwgs_samples, 'subjectID'))

lrwgs_samples_dt['should_remove'] = dt.Frame([
    value in ids_to_remove
    for value in lrwgs_samples_dt['subject'].to_list()[0]
])

if lrwgs_samples_dt[f.should_remove, :].nrows > 0:
    print2('Deleting records')

rd3.delete_list('rd3_novellrwgs_subjectinfo', ids_to_remove)
rd3.delete_list('rd3_novellrwgs_subject', ids_to_remove)

del lrwgs_samples_dt

# ///////////////////////////////////////

# ~ 5g ~
# Delete data from rd3_novelrnaseq

rnaseq_samples = rd3.get('rd3_novelrnaseq_sample', attributes='id,subject')
rnaseq_samples_dt = dt.Frame(flatten_data(rnaseq_samples, 'subjectID'))

rnaseq_samples_dt['should_remove'] = dt.Frame([
    value in ids_to_remove
    for value in rnaseq_samples_dt['subject'].to_list()[0]
])

if rnaseq_samples_dt[f.should_remove, :].nrows > 0:
    print2('Deleteing records')

rd3.delete_list('rd3_novelrnaseq_subjectinfo', ids_to_remove)
rd3.delete_list('rd3_novelrnaseq_subject', ids_to_remove)

del rnaseq_samples_dt

# ///////////////////////////////////////

# ~ 5h ~
# Delete data from rd3_novelsrwgs_*

srwgs_files = rd3.get('rd3_novelsrwgs_file', attributes='EGA,subjectID')
srwgs_files_dt = dt.Frame(flatten_data(srwgs_files, 'subjectID'))

srwgs_labinfo = rd3.get('rd3_novelsrwgs_labinfo', attributes='id,sample')
srwgs_labinfo_dt = dt.Frame(flatten_data(srwgs_labinfo, 'id'))

srwgs_sample = rd3.get('rd3_novelsrwgs_sample', attributes='id,subject')
srwgs_sample_dt = dt.Frame(flatten_data(srwgs_sample, 'subjectID'))

srwgs_files_dt['should_remove'] = dt.Frame([
    value in ids_to_remove for value in srwgs_files_dt['subjectID'].to_list()[0]
])

srwgs_sample_dt['should_remove'] = dt.Frame([
    value in ids_to_remove for value in srwgs_sample_dt['subject'].to_list()[0]
])

if srwgs_files_dt[f.should_remove, :].nrows > 0:
    print('Deleting records....')

if srwgs_sample_dt[f.should_remove, :].nrows > 0:
    print('Deleting records....')

rd3.delete_list('rd3_novelsrwgs_subjectinfo', ids_to_remove)
rd3.delete_list('rd3_novelsrwgs_subject', ids_to_remove)

del srwgs_files_dt, srwgs_labinfo_dt, srwgs_sample_dt

# ///////////////////////////////////////

# ~ 5i ~
# Delete data from rd3_novelwgs_*

wgs_labinfo = rd3.get('rd3_novelwgs_labinfo', attributes='id,sample')
wgs_labinfo_dt = dt.Frame(flatten_data(wgs_labinfo, 'id'))

wgs_samples = rd3.get('rd3_novelwgs_sample', attributes='id,subject')
wgs_samples_dt = dt.Frame(flatten_data(wgs_samples, 'subjectID'))

wgs_samples_dt['should_remove'] = dt.Frame([
    value in ids_to_remove
    for value in wgs_samples_dt['subject'].to_list()[0]
])

# wgs_labinfo_dt['should_remove'] = dt.Frame([
#     value in wgs_samples_dt[f.should_remove, 'subject'].to_list()[0]
#     for value in wgs_labinfo_dt['subjectID']
# ])

rd3.delete_list('rd3_novelwgs_subjectinfo', ids_to_remove)
rd3.delete_list('rd3_novelwgs_subject', ids_to_remove)

# ///////////////////////////////////////////////////////////////////////////////

# Keep a record of retracted samples

# ~ 6a ~
# Update existing subjects with retracted information
curr_retracted = rd3.get('solverd_subjects', q="retracted==Y")
curr_retracted_dt = dt.Frame(
    flatten_data(curr_retracted, 'subjectID|id|value'))

# prep datasets for join
curr_retracted_dt.key = 'subjectID'
ids_dt.names = {'id': 'subjectID'}
ids_dt.key = 'subjectID'

curr_retracted_dt = curr_retracted_dt[:, :, dt.join(ids_dt)]
# curr_retracted_dt[:, (f.subjectID, f.fid, f['fid.0'])]

# collapse family identifiers
curr_retracted_dt['fid'] = dt.Frame([
    ','.join(list(set(filter(None, list(row)))))
    for row in curr_retracted_dt[:, ['fid', 'fid.0']].to_tuples()
])

del curr_retracted_dt['fid.0']


# update row metadata
curr_retracted_dt['comments'] = dt.Frame([
    f"{value}; readded FID on {timestamp()}"
    if bool(value) else None
    for value in curr_retracted_dt['comments'].to_list()[0]
])

curr_retracted_dt['dateRecordUpdated'] = timestamp()

rd3.import_dt('solverd_subjects', curr_retracted_dt)


# compile list of subject, samples, and experiments with FIDs
# retracted_subjects_dt = curr_retracted_dt.copy()
# retracted_samples = rd3.get(
#     'solverd_samples',
#     attributes='sampleID,belongsToSubject',
#     q='retracted==Y'
# )

# retracted_samples_dt = dt.Frame(flatten_data(retracted_samples, 'subjectID'))

# retracted_expr = rd3.get(
#     'solverd_labinfo',
#     attributes='experimentID,sampleID',
#     q='retracted==Y'
# )

# retracted_expr_dt = dt.Frame(flatten_data(retracted_expr, 'sampleID'))

# retracted_data = []
# for _id in tqdm(retracted_expr_dt['experimentID'].to_list()[0]):
#     sample_id = retracted_expr_dt[f.experimentID == _id, 'sampleID'][0, 0]
#     if sample_id:
#         subject_id = retracted_samples_dt[f.sampleID == sample_id,
#                                           'belongsToSubject'][0, 0]

#         subject_fid = retracted_subjects_dt[f.subjectID ==
#                                             subject_id, 'fid'][0, 0]

#         retracted_data.append({
#             'subject_id': subject_id,
#             'family_id': subject_fid,
#             'sample_id': sample_id,
#             'experiment_id': _id
#         })


# dt.Frame(retracted_data).to_csv('data/retracted_subject_experiments.csv')

# ///////////////////////////////////////

# ~ 6b ~

# create new import using ids_to_remove object

retracted_dt = dt.Frame({'subjectID': ids_to_remove})
retracted_dt['retracted'] = 'Y'
retracted_dt['dateRecordCreated'] = timestamp()
retracted_dt['dateRecordUpdated'] = timestamp()
retracted_dt['recordCreatedBy'] = 'rd3-bot'
retracted_dt['wasUpdatedBy'] = 'rd3-bot'
retracted_dt['comments'] = f'record retracted on {timestamp()}'
# rd3.import_dt(pkg_entity='solverd_subjects', data=retracted_dt)

# ///////////////////////////////////////

# ~ 6c ~

# create datasets to re-add into RD3 that removes all metadata except for
# the identifiers and retacted status
removed_dt['retracted'] = 'Y'
removed_dt['dateRecordCreated'] = timestamp()
removed_dt['dateRecordUpdated'] = timestamp()
removed_dt['recordCreatedBy'] = 'rd3-bot'
removed_dt['wasUpdatedBy'] = 'rd3-bot'
removed_dt['comments'] = f'record retracted on {timestamp()}'

# prepare subjects
removed_subjects_dt = removed_dt[:, [
    'subjectID',
    'retracted',
    'dateRecordCreated',
    'dateRecordUpdated',
    'recordCreatedBy',
    'wasUpdatedBy',
    'comments'
]]

removed_samples_dt = removed_dt[:, [
    'samples',
    'subjectID',
    'retracted',
    'dateRecordCreated',
    'dateRecordUpdated',
    'recordCreatedBy',
    'wasUpdatedBy',
    'comments'
]]

removed_samples_dt.names = {
    'samples': 'sampleID',
    'subjectID': 'belongsToSubject'
}

removed_labinfo_dt = removed_dt[:, [
    'experiments',
    'samples',
    'retracted',
    'dateRecordCreated',
    'dateRecordUpdated',
    'recordCreatedBy',
    'wasUpdatedBy',
    'comments'
]]

removed_labinfo_dt.names = {
    'experiments': 'experimentID',
    'samples': 'sampleID'
}

rd3.import_dt('solverd_subjects', removed_subjects_dt)
rd3.import_dt('solverd_samples', removed_samples_dt)
rd3.import_dt('solverd_labinfo', removed_labinfo_dt)


# ///////////////////////////////////////////////////////////////////////////////

# ~ 999 ~
# Merge freeze from ACC to PROD

# import data exported from ACC
removed_dt = fread('85_subjects_with_freeze.csv')
del removed_dt['fid'], removed_dt['files']

# retrieve data from PROD
removed_subjects = rd3.get('solverd_subjects', q='retracted==Y')
removed_subjects_dt = dt.Frame(flatten_data(
    removed_subjects, 'subjectID|id|value'))

removed_samples = rd3.get('solverd_samples', q='retracted==Y')
removed_samples_dt = dt.Frame(flatten_data(
    removed_samples, 'sampleID|subjectID|id|value'))

removed_labs = rd3.get('solverd_labinfo', q='retracted==Y')
removed_labs_dt = dt.Frame(flatten_data(removed_labs, 'sampleID|id|value'))

# add release to removed_subjects_dt
removed_subjects_dt['in_id_list'] = dt.Frame([
    value in removed_dt['subjectID'].to_list()[0]
    for value in removed_subjects_dt['subjectID'].to_list()[0]
])

removed_subjects_dt[f.in_id_list, 'partOfRelease'] = 'freeze3_orginal'


# add release to removed samples
removed_samples_dt['in_id_list'] = dt.Frame([
    value in removed_dt['samples'].to_list()[0]
    for value in removed_samples_dt['sampleID'].to_list()[0]
])

# removed_samples_dt[f.in_id_list,:]
# removed_samples_dt[f.in_id_list,'partOfRelease']
# dt.unique(removed_samples_dt[f.in_id_list,'partOfRelease'])
removed_samples_dt[f.in_id_list, 'partOfRelease'] = 'freeze3_original'

# add release to removed labs
removed_labs_dt['in_id_list'] = dt.Frame([
    value in removed_dt['experiments'].to_list()[0]
    for value in removed_labs_dt['experimentID'].to_list()[0]
])

# removed_labs_dt[f.in_id_list,:]
# removed_labs_dt[f.in_id_list,'partOfRelease']
# dt.unique(removed_labs_dt[f.in_id_list,'partOfRelease'])
removed_labs_dt[f.in_id_list, 'partOfRelease'] = 'freeze3_original'

# update meta in datasets
removed_subjects_dt['dateRecordUpdated'] = timestamp()
removed_samples_dt['dateRecordUpdated'] = timestamp()
removed_labs_dt['dateRecordUpdated'] = timestamp()

rd3.import_dt('solverd_subjects', removed_subjects_dt)
rd3.import_dt('solverd_samples', removed_samples_dt)
rd3.import_dt('solverd_labinfo', removed_labs_dt)
