#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_new_release_processing.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-09-23
#' MODIFIED: 2021-09-23
#' PURPOSE: Process new RD3 releases
#' STATUS: working
#' PACKAGES: datatable
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

import python.rd3tools as rd3tools
from datatable import dt, f, ifelse, sort, by, first


# Init Molgenis Session
rd3tools.status_msg('Initializing Molgenis session...')
rd3 = rd3tools.molgenis(url = '')
rd3.login('', '')


# Pull required datasets
rd3tools.status_msg('Pulling data and reference entities...')
release = rd3.get('rd3_portal_release_freeze2', batch_size = 10000)
organisations = rd3.get('rd3_organisation')
erns = rd3.get('rd3_ERN')


#//////////////////////////////////////

# ~ 1 ~
# Identify new values for key reference entities

# ~ a ~
# Validate ERNS
rd3tools.status_msg('Looking for new ERNs...')
new_erns = list(
    set(
        rd3tools.flatten_attr(release, 'samples_ERN', distinct = True) +
        rd3tools.flatten_attr(release, 'subject_ERN', distinct = True)
    )
)
old_erns = rd3tools.flatten_attr(erns, 'identifier')

# Check ERNs: throw error if value not in RD3 (this important)
for ern in new_erns:
    if ern not in old_erns:
        raise ValueError(
            'Error in ERNs: {} does not exist in. Perhaps it should be recoded?'
            .format(ern)
        )

# Recode ERNs where needed then rerun previous steps
# Unless the ERN is new, you will never have to add a new ERN
for row in release:
    if row['samples_ERN'] == 'ERN-RITA':
        row['samples_ERN'] = 'ERNRITA'
    if row['subject_ERN'] == 'ERN-RITA':
        row['subject_ERN'] = 'ERNRITA'


# ~ b ~
# Validate Organisations
# Make sure new organisations are added before importing main data
rd3tools.status_msg('Looking for new organisations...')
new_orgs = rd3tools.flatten_attr(release, 'organisation_name', distinct = True)
old_orgs = rd3tools.flatten_attr(organisations, 'identifier')
orgs_to_add = []

for org in new_orgs:
    if org not in old_orgs:
        raise ValueError(
            'Error in Organisations: {} does not exist. Perhaps it should be recoded?'
            .format(org)
        )
        
# recode or add depending on the value: rerun previous steps until there are
# no more errors
# for row in release:
#     # recode organisations
#     if row['organisation_name'] == '':
#         row['organisation_name'] = ''
#     # prep for import
#     if row['organisation_name'] == '':
#         orgs_to_add.append({
#             'name': org,
#             'identifier': org
#         })

# Import if new orgs are found
if orgs_to_add:
    rd3tools.status_msg(
        'Found {} new organisations. Will import now...'
        .format(len(orgs_to_add))
    )
    rd3.add('rd3_organisation', orgs_to_add)
    
        
# cleanup
del new_erns, old_erns, ern, row, new_orgs, old_orgs, orgs_to_add, org


#//////////////////////////////////////

# ~ 2 ~
# Create RD3 Release Tables

release = dt.Frame(release)

# ~ a ~
# Create Subject Table
# Not much is needed. Most of the data comes from the PEDIGREE and PHENOPACKET
# files.
subjects = release[
    :,
    {
        'id': f.samples_subject + '_original',
        'subjectID': f.samples_subject,
        'organisation': f.subject_organisation,
        'ERN': f.subject_ERN,
        'solved': ifelse(
            f.subject_solved == 'unsolved',
            False,
            ifelse(
                f.subject_solved == 'solved',
                True,
                f.subject_solved
            )
        ),
        # 'date_solved': f.subject_date_solved, # optional: if available
        'matchMakerPermission': f.subject_matchMakerPermission,
        'recontact': f.subject_recontact,
        'patch': 'freeze2_original'
    },
    sort('id')
][:, first(f[1:]), by(f.id)]


# ~ b ~
# Create Subject Info table
# There isn't much to add at this point as most of the data in this
# table comes from other sources or has never been collected.
subjectInfo = subjects[:, ['id', 'subjectID', 'patch']]

# ~ c ~
# Create Samples
# This table is populated by the portal table
samples = release[
    :,
    {
        'id': f.samples_id + '_original',
        'sampleID': 'VS' + f.samples_id,
        'alternativeIdentifier': f.samples_alternativeIdentifier,
        'subject': f.subject_id + '_original',
        'tissueType': f.samples_tissueType,
        'organisation': f.subject_organisation,
        'ERN': f.samples_ERN,
        'patch': 'freeze2_original'
    },
    sort('id')
]

# ~ d ~
# Create Labinfo table
labinfo = release[
    :,
    {
        'id': f.labinfo_sample + '_original',
        'experimentID': f.labinfo_sample + '_original',
        'sample': 'VS' + f.labinfo_sample + '_original',
        'capture': f.labinfo_capture,
        'libraryType': ifelse(
            f.labinfo_libraryType == 'genomics',
            'Genomic',
            f.labinfo_libraryType
        ),
        'library': f.labinfo_library,
        # 'sequencer': f.labinfo_sequencer, # if available
        'seqType': ifelse(
            f.labinfo_seqType == 'Whole Exome Sequencing',
            'WXS',
            ifelse(
                f.labinfo_seqType == 'Whole Genome Sequencing with or without PCR',
                'WGS',
                f.labinfo_seqType
            )
        ),
        'patch': 'freeze2_original'
    }
]


#//////////////////////////////////////

# ~ 3 ~
# Import Data

# Update Patch table with new release info (DO THIS FIRST!)
rd3.add(
    entity = 'rd3_patch',
    data = {
        'id': 'freeze2_original',
        'patch_date': '2021-03-15',
        'description': 'Freeze2 Original Data'
    }
)


rd3.update_table(subjects, 'rd3_freeze2_subject')
rd3.update_table(subjectInfo, 'rd3_freeze2_subjectinfo')
rd3.update_table(samples, 'rd3_freeze2_samples')
rd3.update_table(labinfo, 'rd3_freeze2_labinfo')
