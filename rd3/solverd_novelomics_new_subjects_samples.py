"""Solve-RD Novelomics: new shipment file processing
FILE: solverd_novelomics_processing.py
AUTHOR: David Ruvolo
CREATED: 2022-11-15
MODIFIED: 2024-01-22
PURPOSE: Import new novelomics data
STATUS: stable
PACKAGES: **see below**
COMMENTS: NA
"""

from os import environ
import functools
import operator
import re
from dotenv import load_dotenv
from datatable import dt, f, first, as_type
from tqdm import tqdm

from rd3tools.molgenis import Molgenis
from rd3tools.datatable import dt_as_recordset
from rd3tools.utils import (
    as_key_pairs,
    timestamp,
    recode_value,
    flatten_data,
    print2
)
load_dotenv()


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


rd3_prod = Molgenis(environ['MOLGENIS_PROD_HOST'])
rd3_prod.login(environ['MOLGENIS_PROD_USR'], environ['MOLGENIS_PROD_PWD'])

# rd3_acc = Molgenis(environ['MOLGENIS_ACC_HOST'])
# rd3_acc.login(environ['MOLGENIS_ACC_USR'], environ['MOLGENIS_ACC_PWD'])

# ///////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Retrieve metadata

# get existing RD3 subjects
raw_subjects = rd3_prod.get(
    entity='solverd_subjects',
    attributes='subjectID,partOfRelease',
    batch_size=10000
)

subjects_flat = flatten_data(raw_subjects, "id")
subjects_dt = dt.Frame(subjects_flat)
subject_ids = subjects_dt['subjectID'].to_list()[0]


# get existing samples
raw_samples = rd3_prod.get(
    entity='solverd_samples',
    attributes='sampleID,batch,partOfRelease',
    batch_size=10000
)
samples_flat = flatten_data(raw_samples, "subjectID|id|value")
samples_dt = dt.Frame(samples_flat)
sample_ids = samples_dt['sampleID'].to_list()[0]


# get new shipment metadata
QUERY = "processed=false"
shipment_raw = rd3_prod.get('rd3_portal_novelomics_shipment', q=QUERY)
shipment_dt = dt.Frame(shipment_raw)
del shipment_dt['_href']

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
    rd3_prod.get('solverd_info_datareleases', attributes='id')
)['id']

releases = dt.unique(shipment_dt['type_of_analysis'])[:, {
    'id': f.type_of_analysis,
    'type': 'freeze',
    'name': f.type_of_analysis,
    'date': timestamp()
}]

# only do this once
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
# Make sure all unique incoming ERN values are mapped to an official ERN code

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

# update mappings and rerun previous incomingErnValues check until
# all mappings are resolved
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
# rd3_acc.import_dt('solverd_info_organisations', new_orgs)
# rd3_prod.import_dt('solverd_info_organisations', new_orgs)

# ///////////////////////////////////////

# ~ 1d ~
# Create tissue type mappings
tissue_types_dt = dt.Frame(
    rd3_prod.get(
        entity='solverd_lookups_tissueType',
        attributes='id'
    )
)['id']

tissue_types_dt['mappingID'] = dt.Frame([
    value.lower()
    for value in tissue_types_dt['id'].to_list()[0]
])

tissue_type_mappings = as_key_pairs(
    data=dt_as_recordset(data=tissue_types_dt),
    key_attr='mappingID',
    value_attr='id'
)

# check incoming data, update mappings (if applicable), and rerun
new_tissue_types = dt.unique(
    shipment_dt[f.tissue_type != None, 'tissue_type']
).to_list()[0]

new_tissue_types.sort(key=str.lower)
for value in new_tissue_types:
    if value.lower() not in tissue_type_mappings:
        print(f"Value '{value}' not in tissue type mappings")

tissue_type_mappings.update({
    'blood': 'Whole Blood',
    'cell pellet': 'Cells',
    'fibroblasts': 'Cells - Cultured fibroblasts',
    'fetus': 'Foetus',
    'ffpe': 'Tumor',
    'heart': 'Heart',
    'muscle': 'Muscle - Skeletal',
    'pbmc': 'Peripheral Blood Mononuclear Cells',
    'whole blood': 'Whole Blood'
})

# ///////////////////////////////////////

# ~ 1e ~
# Create anatomical location mappings

if 'anatomical_location' in shipment_dt.names:
    print('Checking anatomical location mappings....')

    # As of 06 Dec 2022, the value 'blood' can be ignored as it cannot be mapped
    # to a more specific term
    anatomical_location_mappings = {
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
    value.lower()
    for value in material_types['id'].to_list()[0]
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
    rd3_prod.get(
        entity='solverd_lookups_pathological_state'
    )
)['value']

pathological_state['mappingID'] = dt.Frame([
    value.lower()
    for value in pathological_state['value'].to_list()[0]
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
  # pathological_state_mappings.update({ ... })

# ///////////////////////////////////////////////////////////////////////////////

# ~ 1e ~
# Format and recode columns and create new ones

# make sure all P numbers are uppercase (we've had some lowercase values in the past)
shipment_dt['participant_subject'] = dt.Frame([
    value.strip().upper()
    for value in shipment_dt['participant_subject'].to_list()[0]
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
    recode_value(
        mappings=ern_mappings,
        value=value.strip().lower(),
        label='ERN'
    )
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
    recode_value(
        mappings=org_mappings,
        value=value.strip().lower(),
        label='Organistion'
    )
    for value in shipment_dt['organisation'].to_list()[0]
])

# recode anatomical location (if available)
if 'anatomical_location' in shipment_dt.names:
    shipment_dt['anatomical_location'] = dt.Frame([
        recode_value(
            mappings=anatomical_location_mappings,
            value=value.lower(),
            label='anatomical locations'
        ) if value else value
        for value in shipment_dt['anatomical_location'].to_list()[0]
    ])

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
    recode_value(
        mappings=tissue_type_mappings,
        value=value.strip().lower(),
        label='Tissue'
    )
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
    operator.or_,
    (f.participant_subject == value for value in subject_ids)
)

subjects_update_dt = shipment_dt[existing_subjects_reduce, :]
shipment_dt['isNewSubject'] = dt.Frame([
    value not in subjects_update_dt['participant_subject'].to_list()[0]
    for value in shipment_dt['participant_subject'].to_list()[0]
])

# triage incoming samples: which samples exist in RD3?
existing_samples_reduce = functools.reduce(
    operator.or_,
    (f.sample_id == value for value in sample_ids)
)

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
    'tumor_cell_fraction': 'percentageTumorCells'
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

# If this step returns records, continue with the rest of the code. Otherwise, you
# may skip to the next section. :-)
samples_to_validate = shipment_dt[
    (f.isNewSubject == False) & (f.isNewSample == False), :
]

if samples_to_validate.nrows == 0:
    print('Nothing to validate :-)')

# identify records that need to be validated
samples_to_compare = samples_dt[
    functools.reduce(
        operator.or_,
        (f.sampleID ==
         value for value in samples_to_validate['sampleID'].to_list()[0])
    ), :
]

# define object that will store all conflicting data
samples_with_conflicts = []
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
samples_with_updates = []
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
    # for column in columns_with_major_conflicts:
    #   if (column in incomingSample) and (column in existingSample):
    #     if incomingSample[column] != existingSample[column]:
    #       print(f"Incoming sample {id} has conflicting {column} values")
    #       samples_with_conflicts.append({
    #         'incomingValue': incomingSample[column],
    #         'existingValue': existingSample[column],
    #         'message': f"values in {column} do not match"
    #       })

    # identify columns that can automatically imported
    for column in columns_with_minor_conflicts:
        if (column in new_sample) and (column in curr_sample):
            if new_sample[column] not in curr_sample[column]:
                new_row = {
                    'sampleID': id,
                    'subjectID': new_sample['subjectID']
                }
                curr_values = curr_sample[column].split(',')
                curr_values.append(new_sample[column])
                new_row[column] = ','.join(list(set(curr_values)))
                samples_with_updates.append(new_row)

NUM_ISSUES = len(samples_with_conflicts)
NUM_UPDATES = len(samples_with_updates)
print2(
    "Detected", NUM_ISSUES, f"conflict{'s'[:NUM_ISSUES^1]}",
    "that require manual verification"
)
print2(
    "Detected", NUM_UPDATES, f"updates{'s'[:NUM_UPDATES^1]}",
    "that can be imported without review"
)

# ///////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
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

# ~ 2a ~
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
][:, first(f[:]), dt.by(f.subjectID)]

# import into solverd_subjects
# rd3_acc.import_dt('solverd_subjects', new_subjects_dt)
rd3_prod.import_dt('solverd_subjects', new_subjects_dt)

# import into solverd_subjectinfo
# rd3_acc.import_dt(
#   pkg_entity='solverd_subjectinfo',
#   data = new_subjects_dt[
#     :, (f.subjectID, f.partOfRelease, f.dateRecordCreated, f.recordCreatedBy)
#   ]
# )

rd3_prod.import_dt(
    pkg_entity='solverd_subjectinfo',
    data=new_subjects_dt[
        :, (f.subjectID, f.partOfRelease, f.dateRecordCreated, f.recordCreatedBy)
    ]
)

# ~ 2b ~
# Import new sample metadata
newSamples = shipment_dt[
    f.isNewSample,
    (
        f.sampleID,
        f.alternativeIdentifier,
        f.subjectID,
        f.tissueType,
        f.materialType,
        f.pathological_state,
        f.percentageTumorCells,
        f.partOfRelease,
        f.batch,
        f.organisation,
        f.ERN,
        f.dateRecordCreated,
        f.recordCreatedBy
    )
]

newSamples.names = {'subjectID': 'belongsToSubject'}

# rd3_acc.import_dt('solverd_samples', newSamples)
rd3_prod.import_dt('solverd_samples', newSamples)

# ~ 2b ~
# Update subject release information

existingSubjects = shipment_dt[f.isNewSubject == False, :]

# Is the current analysis associated with this participant?
for subj_id in existingSubjects['subjectID'].to_list()[0]:
    subjectReleases = subjects_dt[
        f.subjectID == subj_id, (f.partOfRelease)
    ].to_list()[0][0]

    newRelease = recode_value(
        mappings=release_ids,
        value=existingSubjects[
            f.subjectID == subj_id, 'type_of_analysis'
        ].to_list()[0][0]
    )

    if newRelease not in subjectReleases:
        print('Updating releases....')
        subjectReleases = f"{subjectReleases},{newRelease}"
        subjects_dt[
            f.subjectID == subj_id, (f.partOfRelease)
        ] = subjectReleases
        subjects_dt[f.subjectID == subj_id, 'updatedRecord'] = True

existing_subjects_to_update = dt_as_recordset(
    data=subjects_dt[f.updatedRecord, (f.subjectID, f.partOfRelease)]
)

# update in subjects
# rd3_acc.batchUpdate('solverd_subjects','partOfRelease',updateExistingSubjects)
rd3_prod.batchUpdate(
    'solverd_subjects',
    'partOfRelease',
    existing_subjects_to_update
)

# update in subject info
# rd3_acc.batchUpdate('solverd_subjectinfo','partOfRelease',updateExistingSubjects)
rd3_prod.batchUpdate(
    'solverd_subjectinfo',
    'partOfRelease',
    existing_subjects_to_update
)


# ~ 2c ~
# Update Columns
# Importing update metadata is split into two parts based on column names. Data
# will either go into the subjects or samples table.

samples_with_updates = dt.Frame(samples_with_updates)

# update partOfRelease
rd3_prod.batchUpdate(
    pkg_entity='solverd_samples',
    column='partOfRelease',
    data=dt_as_recordset(
        samples_with_updates[
            f.partOfRelease != None,
            (f.sampleID, f.partOfRelease)]
    )
)

rd3_prod.batchUpdate(
    pkg_entity='solverd_samples',
    column='batch',
    data=dt_as_recordset(
        samples_with_updates[f.batch != None, (f.sampleID, f.batch)]
    )
)


# records to update in subjects
# for column in samples_with_updates.names:
#     if column in ['partOfRelease']:
#         print(f"Updating column {column}....")
#         rd3_prod.batchUpdate(
#           pkg_entity='solverd_subjects',
#           column = column,
#           data = dt_as_recordset(
#             data = samples_with_updates[:, ['subjectID', column]]
#           )
#         )

# # records to update in samples
# for column in samples_with_updates.names:
#     if column in columns_with_minor_conflicts:
#         print(f"Updating column {column}....")
#         rd3_prod.batchUpdate(
#           pkg_entity='solverd_samples',
#           column=column,
#           data=dt_as_recordset(
#             data = samples_with_updates[:, ['sampleID', column]]
#           )
#         )

# ///////////////////////////////////////

# ~ 2d ~
# Update processed status in the portal table
# Create a list of all molgenis IDs where

portal_updates_dt = dt.Frame(
    samples_with_updates['sampleID'].to_list()[0] +
    newSamples['sampleID'].to_list()[0]
)
portal_updates_dt.names = {'C0': 'sampleID'}

# (f.sampleID == value for value in samples_to_validate['sampleID'].to_list()[0])
molgenis_ids_dt = shipment_dt[
    functools.reduce(
        operator.or_,
        (f.sampleID ==
         value for value in portal_updates_dt['sampleID'].to_list()[0])
    ),
    (f.sampleID, f.molgenis_id)
]

# join data
molgenis_ids_dt.key = 'sampleID'
portal_updates_dt.key = 'sampleID'
portal_updates_dt = portal_updates_dt[:, :, dt.join(molgenis_ids_dt)]

portal_updates_dt['processed'] = True
portal_updates_dt[:, dt.update(processed=as_type(f.processed, str))]

# rd3_acc.batchUpdate(
#   pkg_entity='rd3_portal_novelomics_shipment',
#   column = 'processed',
#   data = dt_as_recordset(portal_updates_dt[:, (f.molgenis_id, f.processed)])
# )

rd3_prod.batchUpdate(
    pkg_entity='rd3_portal_novelomics_shipment',
    column='processed',
    data=dt_as_recordset(
        portal_updates_dt[:, (f.molgenis_id, f.processed)])
)

# update portal information
for molgenis_id in portal_updates_dt['molgenis_id'].to_list()[0]:
    shipment_dt[f.molgenis_id == molgenis_id, 'processed'] = True

# review cases that haven't been processed:
# Periodically, there may be a few cases that weren't marked as processed and weren't
# flagged for review. It is likely that there is additional information regarding this
# subject or sample that isn't a red flag. Make sure these cases are reviewed before
# exiting the script.
# Variables that will likely throw a message are:
#   alternativelIdentifier
#   batch
if shipment_dt[f.processed == False, :].nrows > 0:
    print('Warning: there are cases need to be reviewed!!!!')
    # shipment_dt[f.processed==False,:]
    # shipment_dt[f.processed==False,'processed'] = True


# ///////////////////////////////////////

# Alternatively, update all records. If you use this method, make sure you
# pull the data from each instance
# portalUpdate = dt.Frame(
#   rd3_prod.get(
#     entity='rd3_portal_novelomics_shipment',
#     q='processed==false'
#   )
# )
# portalUpdate['processed'] = 'true'

# rd3_prod.importDatatableAsCsv('rd3_portal_novelomics_shipment',portalUpdate)

# update prod
# portalUpdate = dt.Frame(
#   rd3_prod.get(
#     entity='rd3_portal_novelomics_shipment',
#     q='processed==false')
# )
# portalUpdate['processed'] = 'true'
# rd3_prod.importDatatableAsCsv('rd3_portal_novelomics_shipment',shipment_dt)


# ///////////////////////////////////////

rd3_prod.logout()
