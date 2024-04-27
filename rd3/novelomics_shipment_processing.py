"""Solve-RD Novelomics: new shipment file processing
FILE: novelomics_shipment_processing.py
AUTHOR: David Ruvolo
CREATED: 2022-11-15
MODIFIED: 2024-03-14
PURPOSE: Process new shipment manifest files - register new subjects and samples
STATUS: stable
PACKAGES: **see below**
COMMENTS: NA
"""

from os import environ
import functools
import operator
import re
from dotenv import load_dotenv
from datatable import dt, f
from tqdm import tqdm

from rd3tools.molgenis import Molgenis
from rd3tools.datatable import dt_as_recordset
from rd3tools.utils import (as_key_pairs, timestamp,
                            recode_value, flatten_data)
load_dotenv()

rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])


def get_wrapped_values(val: str = None):
    """Get Wrapped Values
    In a string, extract the value between two parentheses.
    :param val: a string that may or may not contain parentheses
    :type val: str

    :return: extracted text
    :rtype: str or NoneType
    """
    search = re.search(r'([\(].*?[\)])', val)
    if not bool(search):
        return None
    match = search.group()
    return re.sub(r'[\(\)]', '', match)


# ///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Retrieve metadata

raw_subjects = rd3_prod.get('solverd_subjects', batch_size=1000)
subjects_flat = flatten_data(raw_subjects, 'subjectID|id|value')
subjects_dt = dt.Frame(subjects_flat)
subject_ids = subjects_dt['subjectID'].to_list()[0]

raw_subjectinfo = rd3_prod.get('solverd_subjectinfo', batch_size=1000)
subjectinfo_flat = flatten_data(raw_subjectinfo, 'id')
subjectinfo_dt = dt.Frame(subjectinfo_flat)
subjectinfo_dt['dateOfBirth'] = subjectinfo_dt[
    :, dt.as_type(f.dateOfBirth, dt.Type.str32)
]

raw_samples = rd3_prod.get('solverd_samples', batch_size=1000)
samples_flat = flatten_data(raw_samples, "subjectID|id|value")
samples_dt = dt.Frame(samples_flat)
sample_ids = samples_dt['sampleID'].to_list()[0]

QUERY = "processed==false"
shipment_raw = rd3_prod.get('rd3_portal_novelomics_shipment', q=QUERY)
shipment_dt = dt.Frame(shipment_raw)
del shipment_dt['_href']

# if you need to delete unprocessed records due to data errors, then
# run the following commands
# rd3_prod.delete_list(
#     entity='rd3_portal_novelomics_shipment',
#     entities=shipment_dt['molgenis_id'].to_list()[0]
# )

# ///////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Process New Shipment metadata
# In this step, process the new shipment manifest data as a whole. Make sure
# values are recoded into RD3 terminology (where applicable) and other columns
# are in the correct format. In the final step, determine which of the incoming
# samples already exist in RD3 as this will determine the import method.
# First, run through all lookup tables and check for new values.

# ~ 1a ~
# Detect new releases
# Use the column `type_of_analysis` to determine if the incoming data is part
# of a new or existing release. As of now, all shipment mantifests will
# contain new "releases" or updates to an existing one. It isn't necessary to
# indicate patches for novel omics releases when importing new sample data.
releaseinfo = dt.Frame(
    rd3_prod.get('solverd_info_datareleases', attributes='id'))['id']

releases = dt.unique(shipment_dt['type_of_analysis'])[:, {
    'id': f.type_of_analysis,
    'type': 'freeze',
    'name': f.type_of_analysis,
    'date': timestamp()
}]

if 'RNA-seq' in releases['id'].to_list()[0]:
    print('Type of analysis is "RNA-seq" and should be recoded as SR-RNAseq')
    # releases[f.id=='RNA-seq','id'] = 'SR-RNAseq'

# format IDs
releases['id'] = dt.Frame([
    f"novel{value.strip().lower().replace('-','')}_original"
    for value in releases['id'].to_list()[0]
])

releases['isNewRelease'] = dt.Frame([
    id not in releaseinfo['id'].to_list()[0]
    for id in releases['id'].to_list()[0]
])

releases['typeOfAnalysis'] = dt.Frame([
    value.lower().replace('-', '')
    for value in releases['name'].to_list()[0]
])

releases['name'] = dt.Frame([
    f"Novel Omics {name}"
    for name in releases['name'].to_list()[0]
])

release_ids = as_key_pairs(
    data=dt_as_recordset(data=releases),
    key_attr='typeOfAnalysis',
    value_attr='id'
)

# If there are new releases, import them
if releases[f.isNewRelease, :].nrows > 0:
    print('There are new releases to import!!!!')
    # new_patches = releases[f.isNewRelease,f[:].remove(f.isNewRelease)]
    # new_patches['createdBy'] = 'rd3-bot'
    # rd3_prod.import_dt('solverd_info_datareleases', newPatches)

# ///////////////////////////////////////

# ~ 1b ~
# Check ERNs and recode into RD3 terminology

erns = dt.Frame(rd3_prod.get('solverd_info_erns', attributes='id'))['id']
ern_mappings = as_key_pairs(
    data=dt_as_recordset(data=erns),
    key_attr='id',
    value_attr='id'
)

# check incoming data, update mappings (if applicable), and rerun
new_ern_values = dt.unique(shipment_dt['ERN']).to_list()[0]
for value in new_ern_values:
    if value.lower() not in ern_mappings:
        print(f'Value "{value}" not in ERN mapping dataset')

# update mappings and rerun previous incomingErnValues check until all mappings are resolved
ern_mappings.update({
    'epicare': 'ern_epicare',
    'ern-epicare': 'ern_epicare',
    'ern-genturis': 'ern_genturis',
    'ern-ithaca': 'ern_ithaca',
    'ern-nmd': 'ern_euro_nmd',
    'genturis': 'ern_genturis',
    'ern-rita': 'ern_rita',
    'ern-rnd': 'ern_rnd',
    'ithaca': 'ern_ithaca',
    'nmd': 'ern_euro_nmd',
    'rita': 'ern_rita',
    'rnd': 'ern_rnd',
    'udn-spain': 'udn_spain'
})

# ///////////////////////////////////////

# ~ 1c ~
# Check Organisation mappings
# Pull organisations to identify new values. If there are any new organisations,
# import them into RD3 and rerun step 1c.i. All values should be in RD3 before
# moving to the next step.

# ~ 1c.i ~
orgs = dt.Frame(
    rd3_prod.get(
        entity='solverd_info_organisations',
        attributes='value'
    )
)['value']

org_mappings = as_key_pairs(
    data=dt_as_recordset(data=orgs),
    key_attr='value',
    value_attr='value'
)

# check incoming data, update mappings (if applicable), and rerun
new_org_values = dt.unique(shipment_dt['organisation']).to_list()[0]
for value in new_org_values:
    if value not in org_mappings:
        print(f"Value '{value}' not in Organistion mappings")

# ~ 1c.ii ~
# find organisations to import
# new_orgs = dt.Frame({'organisation': new_org_values})
# new_orgs['isNew'] = dt.Frame([
#   value not in org_mappings.keys()
#   for value in new_orgs['organisation'].to_list()[0]
# ])

# new_orgs = new_orgs[f.isNew,'organisation']
# new_orgs[['value', 'description']] = new_orgs['organisation']
# del new_orgs['organisation']

# import new organisations and rerun organisation mapping step
# rd3_prod.import_dt('solverd_info_organisations', new_orgs)

# ///////////////////////////////////////

# ~ 1d ~
# Create tissue type mappings
tissue_types_dt = dt.Frame(
    rd3_prod.get('solverd_lookups_tissueType', attributes='id'))['id']

tissue_types_dt['mappingID'] = dt.Frame([
    value.lower() for value in tissue_types_dt['id'].to_list()[0]
])

tissue_type_mappings = as_key_pairs(
    data=dt_as_recordset(data=tissue_types_dt),
    key_attr='mappingID',
    value_attr='id'
)

# check incoming data, update mappings (if applicable), and rerun
incoming_tissue_types = dt.unique(
    shipment_dt[f.tissue_type != None, 'tissue_type']).to_list()[0]

incoming_tissue_types.sort(key=str.lower)
for value in incoming_tissue_types:
    if value.lower() not in tissue_type_mappings:
        print(f"Value '{value}' not in tissue type mappings")

# update mappings for cases that are simple recodes
tissue_type_mappings.update({
    'adipose tissue': 'Adipose',
    'blood': 'Whole Blood',
    'cell pellet': 'Cells',
    'exelid': 'Eyelid',
    'fat skin': 'Adipose - Subcutaneous',
    'fibroblasts': 'Cells - Cultured fibroblasts',
    "fetus skin": "Foetus",
    'fetus': 'Foetus',
    'ffpe': 'Tumor',
    'heart': 'Heart',
    'muscle': 'Muscle - Skeletal',
    'pbmc': 'Peripheral Blood Mononuclear Cells',
    'whole blood': 'Whole Blood',
    'subcutaneous fat': 'Adipose - Subcutaneous',
    'tissue': 'Tissue - unspecified',
})

# ///////////////////////////////////////

# ~ 1e ~
# Create anatomical location mappings

if 'anatomical_location' in shipment_dt.names:
    print('Manually check anatomical location mappings!')

    # As of 06 Dec 2022, the value 'blood' can be ignored as it cannot be mapped
    # to a more specific term.
    # As of 15 March 2024, terms that do not exist in RD3 will be labelled other.
    # The original value will be placed in a new column. This was implemented as
    # it isn't possible to determine the exact location from the supplied value.
    anatomical_location_mappings = {
        'chest': '74964007',  # Other
        'left': '74964007',  # Other
        'nose': '74964007',  # Other
        'retro right auricular area': '74964007',  # Other
        'right': '74964007',  # Other
        'scalp': '74964007',  # Other
        'other': '74964007',  # Other

        'chest skin': '74160004',  # Skin of Chest
        'skin scalp': '43067004',  # Skin of Scalp
        # Entire skin of postauricular region
        'right retro auricular skin': '244080005',
        'skin': '314818000',  # Skin Tissue
    }

    # check incoming data, update mappings (if applicable), and rerun
    new_anatomical_locations = dt.unique(
        shipment_dt[
            f.anatomical_location != None,
            'anatomical_location'
        ]
    ).to_list()[0]

    for value in new_anatomical_locations:
        if value.lower() not in anatomical_location_mappings:
            print(f"Value '{value}' not in anatomical location mappings")

    # if there are mappings, update here.
    # anatomical_location_mappings.update({ ... })

# ///////////////////////////////////////

# ~ 1f ~
# Create material type mappings
material_types = dt.Frame(
    rd3_prod.get(
        entity='solverd_lookups_materialType',
        attributes='id'
    )
)['id']

material_types['mappingID'] = dt.Frame([
    value.lower() for value in material_types['id'].to_list()[0]
])

material_type_mappings = as_key_pairs(
    data=dt_as_recordset(data=material_types),
    key_attr='mappingID',
    value_attr='id'
)

# check incoming data, update mappings (if applicable), and rerun
incomingmaterial_types = dt.unique(
    shipment_dt[f.sample_type != None, 'sample_type']).to_list()[0]

for value in incomingmaterial_types:
    if value.lower() not in material_type_mappings:
        print(f"Value '{value}' does not exist in material type mappings")

material_type_mappings.update({
    'ffpe': 'TISSUE (FFPE)',
    'total rna': 'RNA'
})

# ///////////////////////////////////////

# ~ 1d ~
# Create pathological state mappings
pathological_state = dt.Frame(
    rd3_prod.get(entity='solverd_lookups_pathologicalstate'))['value']

pathological_state['mappingID'] = dt.Frame([
    value.lower() for value in pathological_state['value'].to_list()[0]
])

pathological_state_mappings = as_key_pairs(
    data=dt_as_recordset(pathological_state),
    key_attr='mappingID',
    value_attr='value'
)

# check incoming data, update mappings (if applicable), and rerun
if 'pathological_state' in shipment_dt.names:
    new_pathological_states = dt.unique(
        shipment_dt[
            f.pathological_state != None,
            'pathological_state'
        ]
    ).to_list()[0]

    for value in new_pathological_states:
        if value.lower() not in pathological_state_mappings:
            print(
                f"Value '{value}' does not exist in pathological state mappings")

  # if there are any values, enter them below ->
    pathological_state_mappings.update({
        'affected area': 'Affected',
        'safe area': 'Normal'
    })

# ///////////////////////////////////////////////////////////////////////////////

# ~ 1e ~
# Format and recode columns and create new ones

# make sure all P numbers are uppercase (we've had some lowercase values in the past)
shipment_dt['participant_subject'] = dt.Frame([
    value.strip().upper() for value in shipment_dt['participant_subject'].to_list()[0]
])

# transform incoming analysis so that it can be mapped to a release
shipment_dt['type_of_analysis'] = dt.Frame([
    value.strip().lower().replace('-', '') if value else value
    for value in shipment_dt['type_of_analysis'].to_list()[0]
])

shipment_dt['partOfRelease'] = dt.Frame([
    recode_value(mappings=release_ids, value=value, label='Release')
    for value in shipment_dt['type_of_analysis'].to_list()[0]
])

shipment_dt['ERN'] = dt.Frame([
    recode_value(ern_mappings, value.strip().lower(), 'ERN')
    for value in shipment_dt['ERN'].to_list()[0]
])

# clean organisation
shipment_dt['organisation'] = dt.Frame([
    value.strip().lower() if value else value
    for value in shipment_dt['organisation'].to_list()[0]
])

shipment_dt['organisation'] = dt.Frame([
    re.sub(r'\s+', '-', value) if value else value
    for value in shipment_dt['organisation'].to_list()[0]
])

shipment_dt['organisation'] = dt.Frame([
    recode_value(org_mappings, value.strip().lower(), 'Organistion')
    for value in shipment_dt['organisation'].to_list()[0]
])

# recode anatomical location (if available)
if 'anatomical_location' in shipment_dt.names:
    shipment_dt['tmp_anatomical_location'] = dt.Frame([
        recode_value(
            mappings=anatomical_location_mappings,
            value=value.lower(),
            label='anatomical locations'
        ) if value else value
        for value in shipment_dt['anatomical_location'].to_list()[0]
    ])

    # identifier cases with "other"
    shipment_dt['anatomical_location_comment'] = dt.Frame([
        row[1] if row[0] == "74964007" else None
        for row in shipment_dt[:, ['tmp_anatomical_location', 'anatomical_location']].to_tuples()
    ])

    shipment_dt['anatomical_location'] = shipment_dt['tmp_anatomical_location']
    del shipment_dt['tmp_anatomical_location']

# recode sample types (i.e., materialType)
shipment_dt['sample_type'] = dt.Frame([
    'TISSUE (FFPE)'
    if row[0] and (row[0].lower() == 'ffpe')
    else (
        recode_value(
            mappings=material_type_mappings,
            value=row[1].lower(),
            label='Sample Type'
        ) if row[1] is not None else row[1]
    )
    for row in shipment_dt[:, (f.tissue_type, f.sample_type)].to_tuples()
])

# update sample types for DEEP-WES samples
if 'extracted_protocol' in shipment_dt.names:
    shipment_dt['sample_type'] = dt.Frame([
        'TISSUE (FFPE)'
        if row[1].lower().strip() == 'tumor' and row[2].lower().strip() == 'tissue (ffpe)'
        else row[0]
        for row in shipment_dt[:, (f.sample_type, f.tissue_type, f.extracted_protocol)].to_tuples()
    ])

# recode tissue types
shipment_dt['tissue_type'] = dt.Frame([
    recode_value(tissue_type_mappings, value.strip().lower(), 'Tissue')
    for value in shipment_dt['tissue_type'].to_list()[0]
])

# extract values inside parenthesis
# get_wrapped_values(row[0]) if '(' in row[0] else row[1]
# for row in shipment_dt[:, (f.tissue_type, f.extracted_protocol)].to_tuples()

if 'extracted_protocol' in shipment_dt.names:
    shipment_dt['extracted_protocol'] = dt.Frame([
        get_wrapped_values(value) if bool(value) and '(' in value else value
        for value in shipment_dt[:, (f.extracted_protocol)].to_list()[0]
    ])

# add alternative identifier if not present
if 'alternative_sample_identifier' not in shipment_dt.names:
    shipment_dt['alternative_sample_identifier'] = None

if 'pathological_state' in shipment_dt.names:
    shipment_dt['pathological_state'] = dt.Frame([
        recode_value(
            mappings=pathological_state_mappings,
            value=value.lower().strip(),
            label='Pathological State'
        )
        if value else None
        for value in shipment_dt['pathological_state'].to_list()[0]
    ])
else:
    shipment_dt['pathological_state'] = None


if 'tumor_cell_fraction' in shipment_dt.names:
    shipment_dt['tumor_cell_fraction'] = dt.Frame([
        None if value == 'UK' else value
        for value in shipment_dt['tumor_cell_fraction'].to_list()[0]
    ])
else:
    shipment_dt['tumor_cell_fraction'] = None

# ///////////////////////////////////////

# ~ 1f ~
# Detect if there are any new subjects and samples
# New novel omics subjects and samples are submitted via the shipment manifest
# file. In this file there are three categories of records:
#
#  1) New subjects and new samples
#  2) Existing subjects with new samples
#  3) Existing subjects with existing samples
#
# It is important to identify these groups as this informs us how to process
# the data. It is highly unlikely that there will ever be a new subject with
# and old record, but it's good to check. In terms of importing the data,
# records will be imported in the following way:
#
# 1) New subjects and samples:
#     - import subject metadata into: subjects and subjectinfo
#     - import samples as is
# 2) Existing subjects with new samples
#     - update release information in subjects, subjectinfo
#     - the rest of the subject metadata can be ignored
#     - import samples as is
# 3) Existing subjects with existing samples
#     - compare changes in key attributes (sample type, material, protocol, etc)
#       to identifiy changes and to determine what to change.
#     - update release information in subjects, subjectinfo, samples
#     - the rest of the subject metadata can be ignored

# triage incoming subjects: which subjects exist in RD3?
existing_subjects_reduce = functools.reduce(
    operator.or_, (f.participant_subject == value for value in subject_ids))

subjects_update_dt = shipment_dt[existing_subjects_reduce, :]

shipment_dt['isNewSubject'] = dt.Frame([
    value not in subjects_update_dt['participant_subject'].to_list()[0]
    for value in shipment_dt['participant_subject'].to_list()[0]
])

# triage incoming samples: which samples exist in RD3?
existing_samples_reduce = functools.reduce(
    operator.or_, (f.sample_id == value for value in sample_ids))

samples_update_dt = shipment_dt[existing_samples_reduce, :]
shipment_dt['isNewSample'] = dt.Frame([
    id not in sample_ids for id in shipment_dt['sample_id'].to_list()[0]
])

# shipment_dt[:, dt.count(), dt.by(f.isNewSubject, f.isNewSample)]
# shipment_dt[(f.isNewSubject==False) & (f.isNewSample ==False),:]

# ~ 1g ~
# Rename columns
shipment_dt.names = {
    'participant_subject': 'subjectID',
    'sample_id': 'sampleID',
    'alternative_sample_identifier': 'alternativeIdentifier',
    'tissue_type': 'tissueType',
    'sample_type': 'materialType',
    'pathological_state': 'pathological_state',
    'tumor_cell_fraction': 'percentageTumorCells',
    'anatomical_location': 'anatomicalLocation',
    'anatomical_location_comment': 'anatomicalLocationComment',
}

# ///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Validate existing subjects and samples for conflicts
# For all incoming samples that already exist in RD3, it is important to check
# for any conflicts between the two records as these will need to be resolved
# independently. The following attributes must be checked.
#
# - subjectID: was the sample incorrectly associated with a subject?
# - tissueType: flag conflicts for review
# - materialType: flag conflicts for review
# - batchNumber: add if not already present
# - extractedProtocol: add if not already present
# - anatomicalLocation: flag conflicts for review
# - pathological_state: flag conflicts for review
# - alternativeSampleIdentifiers: add if not already present
# - percentageTumorCells: flag conflicts for review
# - partOfRelease: add if not already present
#
# Some attributes can be automatically updated with out review (see notes above).

samples_with_conflicts = []
samples_with_updates = []

# If this step returns records, continue with the rest of the code. Otherwise, you
# may skip to the next section. :-)
samples_to_validate = shipment_dt[(
    f.isNewSubject == False) & (f.isNewSample == False), :]

if samples_to_validate.nrows != 0:
    print(
        'There are subjects and samples that already exist in RD3 in the new dataset!',
        'These records must be validated before data can be imported.'
    )
else:
    print('Nothing to validate. You may skip to the next section')

# identify records that need to be validated
samples_to_compare = samples_dt[
    functools.reduce(operator.or_, (
        f.sampleID == value for value in samples_to_validate['sampleID'].to_list()[0])
    ), :
]

# define object that will store all conflicting data
columns_with_major_conflicts = [
    'subjectID',
    'tissueType',
    'materialType',
    'extractedProtocol',
    'anatomicalLocation',
    'pathological_state',
    'percentageTumorCells',
]

# define object that will stored rows that can be updated without review
# for these cases, confirm that the incoming value does not exist for this
# record (in the string). If it does not, join the two strings.
columns_with_minor_conflicts = [
    'batch',
    'alternativeSampleIdentifiers',
    'partOfRelease'
]

sample_ids_to_validate = samples_to_validate['sampleID'].to_list()[0]
for sample_id in tqdm(sample_ids_to_validate):

    new_sample = dt_as_recordset(
        samples_to_validate[f.sampleID == sample_id, :])[0]
    curr_sample = dt_as_recordset(samples_dt[f.sampleID == sample_id, :])[0]

    # identify records that require manually verification
    for column in columns_with_major_conflicts:
        if (column in new_sample) and (column in curr_sample):
            if new_sample[column] is not None and curr_sample[column] is not None:
                if new_sample[column] not in curr_sample[column]:
                    new_row = {
                        'sampleID': sample_id,
                        'subjectID': new_sample['subjectID'],
                    }
                    curr_values = curr_sample[column].split(',')
                    curr_values.append(new_sample[column])
                    new_row[column] = ','.join(list(set(curr_values)))
                    samples_with_conflicts.append(new_row)

    # identify columns that can automatically imported
    for column in columns_with_minor_conflicts:
        if (column in new_sample) and (column in curr_sample):
            if new_sample[column] not in curr_sample[column]:
                new_row = {
                    'sampleID': sample_id,
                    'subjectID': new_sample['subjectID']
                }
                curr_values = curr_sample[column].split(',')
                curr_values.append(new_sample[column])
                new_row[column] = ','.join(list(set(curr_values)))
                samples_with_updates.append(new_row)

NUM_ISSUES = len(samples_with_conflicts)
NUM_UPDATES = len(samples_with_updates)
print(
    "Detected", NUM_ISSUES, f"conflict{'s'[:NUM_ISSUES^1]}",
    "that require manual verification"
)
print(
    "Detected", NUM_UPDATES, f"updates{'s'[:NUM_UPDATES^1]}",
    "that can be imported without review"
)

# ///////////////////////////////////////////////////////////////////////////////

# ~ 3 ~
# Import data
# Data will be imported using several methods.
#
# 1) IMPORT NEW SUBJECTS: All new subjects should be imported first. Metadata
#    should be also imported into the `subjectinfo` table. Import using the
#    `importDataTableAsCsv` method.
# 2) IMPORT NEW SAMPLES: All new samples can be imported as is. Select columns
#    and import using `importDataTableAsCsv`. Make sure release information
#    is updated in the subjects table.
# 3) UPDATING RECORDS: Using the object `samples_with_updates`,
#    batch update each attribute.

shipment_dt['dateRecordCreated'] = timestamp()
shipment_dt['recordCreatedBy'] = 'rd3-bot'

# ~ 3a ~
# Import new subject metadata
new_subjects_dt = shipment_dt[
    f.isNewSubject,
    (
        f.subjectID,
        f.organisation,
        f.ERN,
        f.partOfRelease,
        f.dateRecordCreated,
        f.recordCreatedBy
    )
]

# if there are no more subjects, then you can skip to 3b
if not new_subjects_dt.nrows:
    print('No subjects to import. You may skip this step')

if dt.unique(new_subjects_dt['subjectID']).nrows != new_subjects_dt.nrows:
    print(
        'WARNING: More than one sample per subject was submitted.',
        'Consider processing data separately.'
    )

new_subjectinfo_dt = new_subjects_dt[
    :, (f.subjectID, f.partOfRelease, f.dateRecordCreated, f.recordCreatedBy)
]

rd3_prod.import_dt('solverd_subjects', new_subjects_dt)
rd3_prod.import_dt('solverd_subjectinfo', new_subjectinfo_dt)

# ///////////////////////////////////////

# ~ 3b ~
# Import new sample metadata
new_samples_dt = shipment_dt[
    f.isNewSample, (
        f.sampleID,
        f.alternativeIdentifier,
        f.subjectID,
        f.tissueType,
        f.materialType,
        f.pathological_state,
        f.percentageTumorCells,
        f.anatomicalLocation,
        f.anatomicalLocationComment,
        f.partOfRelease,
        f.batch,
        f.organisation,
        f.ERN,
        f.dateRecordCreated,
        f.recordCreatedBy)]

new_samples_dt['retracted'] = 'N'
new_samples_dt.names = {'subjectID': 'belongsToSubject'}

rd3_prod.import_dt('solverd_samples', new_samples_dt)

# ///////////////////////////////////////

# ~ 3c ~
# Update subject release information

existing_subjects_dt = shipment_dt[f.isNewSubject == False, :]
existing_subject_ids = existing_subjects_dt['subjectID'].to_list()[0]

# Is the current analysis associated with this participant?
for subj_id in tqdm(existing_subject_ids):
    curr_subj_row = subjects_dt[f.subjectID == subj_id, :]
    curr_releases = curr_subj_row[:, 'partOfRelease'].to_list()[0][0]

    new_subj_row = existing_subjects_dt[f.subjectID == subj_id, :]
    new_analysis_type = new_subj_row[:, 'type_of_analysis'].to_list()[0][0]
    new_release = recode_value(release_ids, new_analysis_type)

    if new_release not in curr_releases:
        print('Updating releases....')
        curr_new_releases = f"{curr_releases},{new_release}"
        subjects_dt[
            f.subjectID == subj_id,
            ['partOfRelease', 'should_import']
        ] = (curr_new_releases, True)

        subjectinfo_dt[
            f.subjectID == subj_id,
            ['partOfRelease', 'should_import']
        ] = (curr_new_releases, True)

rd3_prod.import_dt('solverd_subjects', subjects_dt[f.should_import, :])
rd3_prod.import_dt('solverd_subjectinfo', subjectinfo_dt[f.should_import, :])

# ///////////////////////////////////////

# ~ 3d ~
# Update release and batch info in the samples table

if bool(samples_with_updates):
    print("There are samples to update. Please run the following step")
else:
    print('New samples to update. You may skip this step.')

update_samples_dt = dt.Frame(samples_with_updates)
update_sample_ids = update_samples_dt['sampleID'].to_list()[0]

for sample_id in update_sample_ids:
    curr_row = samples_dt[f.sampleID == sample_id, :]
    new_row = update_samples_dt[f.sampleID == sample_id, :]

    # check releases
    if 'partofRelease' in update_samples_dt.names:
        curr_release = curr_row['partOfRelease'].to_list()[0][0]
        new_release = new_row['partOfRelease'].to_list()[0][0]

        if new_release not in curr_release:
            print('Updating release info....')
            curr_new_releases = f"{curr_release},{new_release}"
            samples_dt[
                f.sampleID == sample_id,
                ['partOfRelease', 'should_import']
            ] = (curr_new_releases, True)

    # check batches
    if 'batch' in update_samples_dt.names:
        curr_batch = curr_row['batch'].to_list()[0][0]
        new_batch = new_row['batch'].to_list()[0][0]

        if new_batch not in curr_batch:
            print('Updating batch info....')
            curr_new_batch = f"{curr_batch}, {new_batch}"
            samples_dt[
                f.sampleID == sample_id,
                ['batch', 'should_import']
            ] = (curr_new_batch, True)


rd3_prod.import_dt('solverd_samples', samples_dt[f.should_import, :])

# ///////////////////////////////////////

# ~ 3e ~
# Update processed status in the portal table

processed_ids = []
shipment_updates_dt = dt.Frame(shipment_raw)

if update_samples_dt:
    processed_ids += update_samples_dt['sampleID'].to_list()[0]

if new_samples_dt:
    processed_ids += new_samples_dt['sampleID'].to_list()[0]

for proc_id in processed_ids:
    shipment_updates_dt[f.sample_id == proc_id, 'processed'] = True


if shipment_updates_dt[f.processed, :].nrows != shipment_updates_dt.nrows:
    print('There are still samples that are not processed. Please review')

shipment_updates_dt['processed'] = shipment_updates_dt[
    :, dt.as_type(f.processed, dt.Type.str32)
]

rd3_prod.import_dt('rd3_portal_novelomics_shipment', shipment_updates_dt)

# ///////////////////////////////////////////////////////////////////////////////

rd3_prod.logout()
