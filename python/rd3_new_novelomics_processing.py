#'////////////////////////////////////////////////////////////////////////////
#' FILE: rd3_new_novelomics_processing.py
#' AUTHOR: David Ruvolo
#' CREATED: 2022-03-08
#' MODIFIED: 2022-03-08
#' PURPOSE: process new novelomics data in the portal
#' STATUS: in.progress
#' PACKAGES: datatable, 
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////


from python.rd3tools import Molgenis, status_msg, recodeValue, to_keypairs, to_records
from datatable import dt, f
import functools
import operator

# SET RELEASE INFORMATRION
patchinfo = {
    'name': 'novelwgs',                   # name of the RD3 Release
    'id': 'novelwgs_original',            # ID labels `<name>_original`
    'type': 'freeze',                     # 'freeze' or 'patch'
    'date': '2022-03-08',                 # Date of release, yyyy-mm-dd
    'description': 'Novel Omics WGS'      # a nice description
}

# for local dev use only
from dotenv import load_dotenv
from os import environ
load_dotenv()
host = environ['MOLGENIS_HOST_ACC']
token = environ['MOLGENIS_TOKEN_ACC']
# host = environ['MOLGENIS_HOST_PROD']
# token = environ['MOLGENIS_TOKEN_PROD']

# use if running in Molgenis
# host = 'http://localhost/api/'
# token = '${molgenisToken}'

# connect to db
rd3 = Molgenis(url=host, token=token)

#//////////////////////////////////////////////////////////////////////////////

# ~ 0 ~
# Pull Data
# The source of the novelomics releases come from rd3_portal_novelomics. Data
# is sent from EGA and Tubingen, and sometimes supplied by CNAG. To run this
# script, pull both novelomics portal tables, reference entities, and create
# a list of existing subject and sample IDs.
#
# Pull mapping tables or define them below.

# ~ 0a ~
# Pull portal tables
status_msg('Pulling data from the portal')
shipment = dt.Frame(
    rd3.get(
        entity='rd3_portal_novelomics_shipment',
        batch_size=10000
    )
)

experiment = dt.Frame(
    rd3.get(
        entity='rd3_portal_novelomics_experiment',
        batch_size=10000
    )
)

del shipment['_href']
del experiment['_href']

# ~ 0b ~
# Compile existing subject and sample IDs

# get existing subjects
existingSubjects = dt.rbind(
    dt.Frame([
        rd3.get(
            entity='rd3_noveldeepwes_subject',
            attributes='id,subjectID,patch'
        )
    ]),
    dt.Frame([
        rd3.get(
            entity='rd3_novelsrwgs_subject',
            attributes='id,subjectID,patch'
        )
    ])
    , Force = True
)

# get existing samples
existingSamples = dt.rbind(
    dt.Frame([
        rd3.get(
            entity='rd3_noveldeepwes_sample',
            attributes='id,sampleID,patch'
        )
    ])[:, {
        'id': f.id,
        'subjectID': f.subjectID,
        'patch': f.patch,
        'release': 'deepwes'
    }],
    dt.Frame([
        rd3.get(
            entity='rd3_novelsrwgs_subject',
            attributes='id,sampleID,patch'
        )
    ])[:, {
        'id': f.id,
        'subjectID': f.subjectID,
        'patch': f.patch,
        'release': 'srwgs'
    }],
)


# ~ 0c ~
# Pull reference datasets and define mappings
# Define mappings to recode raw data into RD3 terminology. Make sure all
# mappings are lowered. When using the function `recodeValue`, make sure
# the input value is also lowered.

# ~ 0c.i ~
# Create ERN Mappings

erns = dt.Frame(rd3.get('rd3_ERN'))
erns['id'] = dt.Frame([d.lower() for d in erns['identifier'].to_list()[0]])

# convert to dictionary and append unique mappings
ernMappings = to_keypairs(
    data= to_records(
        data=erns
    ),
    keyAttr='id',
    valueAttr='identifier'
)

ernMappings.update({
    'genturis': 'ERN-GENTURIS',
    'ithaca': 'ERN-ITHACA',
    'nmd': 'ERN-NMD',
    'rnd': 'ERN-RND'
})

# ~ 0c.ii ~
# Create organisation mappings

organisations = dt.Frame(rd3.get('rd3_organisation'))
organisations['id'] = dt.Frame([
    d.lower() for d in organisations['identifier'].to_list()[0]
])

# convert to dictionary and append unique mappings
organisationMappings = to_keypairs(
    data = to_records(data = organisations),
    keyAttr = 'id',
    valueAttr = 'identifier'
)

organisationMappings.update({
    'malgorzata  dec-cwiek': 'malgorzata-dec-cwiek'
})

# ~ 0c.iii ~
# Create tissue types mappings

tissueTypes = dt.Frame(rd3.get('rd3_tissueType'))
tissueTypes['id'] = dt.Frame([
    d.lower() for d in tissueTypes['identifier'].to_list()[0]
])

# convert to dict
tissueTypeMappings = to_keypairs(
    data = to_records(data = tissueTypes),
    keyAttr='id',
    valueAttr='identifier'
)

tissueTypeMappings.update({
    'blood': 'Whole Blood',
    'ffpe': 'Tumor',
    'fibroblasts': 'Cells - Cultured fibroblasts',
    'pbmc': 'Peripheral Blood Mononuclear Cells',
    'whole blood': 'Whole Blood'
})

# ~ 0c.iv ~
# Create material type mappings

materialTypes = dt.Frame(rd3.get('rd3_materialType'))
materialTypes['id2'] = dt.Frame([
    d.lower() for d in materialTypes['id'].to_list()[0]
])

materialTypeMappings = to_keypairs(
    data = to_records(data = materialTypes),
    keyAttr = 'id2',
    valueAttr='id'
)

materialTypeMappings.update({
    'total rna': 'RNA',
    'ffpe': 'TISSUE (FFPE)'
})




#//////////////////////////////////////////////////////////////////////////////

# ~ 1 ~
# Process Shipment Data
# Before data is transformed into the shape of the RD3 Release structure
# merge datasets and recode values as one object. Afterwards, subset data by
# analysis type.

# ~ 1a ~
# Format identifiers in the shipment dataset
# Like the main RD3 freezes, each record should receive a unique identifier
# that corresponds to the release. For example, if a record has an ID of
# "12345" and the type of analysis is "Deep-WES", then the new identifier
# should be "12345_deepwes_original". Apply the same treatment to sample- and
# subject identifiers

# clean `type_of_analysis`
shipment['typeOfAnalysis'] = dt.Frame([
    d.lower().replace('-','').strip()
    for d in shipment['type_of_analysis'].to_list()[0]
])

# concat sample identifier and analysis type
shipment['sampleID'] = dt.Frame([
    '_'.join(d) + '_original'
    for d in shipment[:,['sample_id','typeOfAnalysis']].to_tuples()
])

# concat subject identifier and analysis type
shipment['subjectID'] = dt.Frame([
    '_'.join(d) + '_original'
    for d in shipment[:, ['participant_subject', 'typeOfAnalysis']].to_tuples()
])

# ~ 1b ~
# Recode Attributes
# Using the mappings defined in previous step (step 0), recode the relevant
# attributes. Make sure all uses of the `recodeValue` method use `d.lower()`.

# recode values to align to rd3_ern
shipment['ERN'] = dt.Frame([
    recodeValue(
        mappings = ernMappings,
        value = d.lower(),
        label = 'ERN'
    )
    for d in shipment['ERN'].to_list()[0]
])


# recode values to align to rd3_organisation
shipment['organisation'] = dt.Frame([
    recodeValue(
        mappings = organisationMappings,
        value = d.lower(),
        label = 'Organisation'
    )
    for d in shipment['organisation'].to_list()[0]
])

# recode values to align to rd3_tissueType
shipment['tissue_type'] = dt.Frame([
    recodeValue(
        mappings = tissueTypeMappings,
        value = d.lower(),
        label = 'Tissue Type'
    )
    for d in shipment['tissue_type'].to_list()[0]
])

# recode values to align to rd3_materialTypes
shipment['sample_type'] = dt.Frame([
    recodeValue(
        mappings = materialTypeMappings,
        value = d.lower(),
        label = 'materialType'
    )
    for d in shipment['sample_type'].to_list()[0]
])

# ~ 1c ~
# Triage Subjects and Samples
# Using a list of existing subject- and sample identifiers, identify new
# samples and subjects.

shipment['isNewSubject'] = dt.Frame([
    d not in existingSubjects['subjectID'].to_list()[0]
    for d in shipment['particpant_subject'].to_list()[0]
])

shipment['isNewSample'] = dt.Frame([
    d not in existingSamples['sampleID'].to_list()[0]
    for d in shipment['sample_id'].to_list()[0]
])

#//////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Create Subjects


# !!!!! PICK UP HERE !!!!!
subjects = shipment[
    :, dt.first(f[:]), dt.by(f.subjectID, f.typeOfAnalysis) 
][:,{
    'id': f.subjectID,
    'subjectID': f.participant_subject,
    'patch': patchinfo['id'],
    'organisation': f.organisation,
    'ERN': f.ERN
}]

