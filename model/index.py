#'////////////////////////////////////////////////////////////////////////////
#' FILE: emx.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-09-16
#' MODIFIED: 2022-03-07
#' PURPOSE: incorporate YAML to EMX generator
#' STATUS: stable
#' PACKAGES: yamlemxconvert
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from model.modeltools import setEmxRelease, writeEmxTemplate
from yamlemxconvert.convert import Convert

# ~ 0 ~
# Compile EMX for RD3 Portal Releases
# Comple the YAML-EMX markup for new RD3 staging tables
# See `model/rd3_portal_release.yaml` for additional notes

convertPortalReleaseEmx = Convert(files = [
    'model/base_rd3_portal.yaml', # import portal first
    'model/rd3_portal_release.yaml'
])

convertPortalReleaseEmx.convert()

# set release information
<<<<<<< HEAD
convertPortalReleaseEmx.entities[0]['name'] = 'rd3_portal_release_freeze3'
=======
convertPortalReleaseEmx.entities[0]['name'] = 'freeze3'
>>>>>>> 9bd472a (create staging table for DF3)
convertPortalReleaseEmx.entities[0]['label'] = 'Freeze 3'
convertPortalReleaseEmx.entities[0]['description'] = ' Staging table for Freeze 3 (2022-03-09)'

for d in convertPortalReleaseEmx.attributes:
<<<<<<< HEAD
    d['entity'] = convertPortalReleaseEmx.entities[0]['name']
=======
    d['entity'] = f"rd3_portal_release_{convertPortalReleaseEmx.entities[0]['name']}"
>>>>>>> 9bd472a (create staging table for DF3)

convertPortalReleaseEmx.write(
    name = 'rd3_portal_release',
    format = 'xlsx',
    outDir = 'dist/'
)


# ~ 1 ~
# Compile EMX for RD3 Portal Novelomics (i.e., shipment and experiment manifest files)
convertPortalNovelomicsEmx = Convert(
    files = [
        'src/emx/base_rd3_portal.yaml',
        'src/emx/rd3_portal_novelomics.yaml'
    ]
)

convertPortalNovelomicsEmx.convert()
convertPortalNovelomicsEmx.write(
    name = 'rd3_portal_novelomics',
    format = 'xlsx',
    outDir = 'dist/'
)


# generate CSV templates
writeEmxTemplate(
    entities = convertPortalNovelomicsEmx.entities,
    attributes = convertPortalNovelomicsEmx.attributes,
    format = 'csv',
    outDir = 'templates'
)

# writeEmxTemplate(
#     entities = convertPortalNovelomicsEmx.entities,
#     attributes = convertPortalNovelomicsEmx.attributes,
#     format = 'xlsx',
#     outDir = 'templates'
# )


#//////////////////////////////////////////////////////////////////////////////

# ~ 2 ~
# Convert EMX for RD3 Portal Demographics Table

# convertPortalDemographicsEmx = Convert(
#     files = [
#         'src/emx/rd3_portal_demographics.yaml'
#     ]
# )

# convertPortalDemographicsEmx.convert()
# convertPortalDemographicsEmx.packages
# convertPortalDemographicsEmx.entities
# convertPortalDemographicsEmx.attributes

# convertPortalDemographicsEmx.write(
#     name = 'rd3_portal_demographics',
#     format = 'xlsx',
#     outDir = 'dist/'
# )


#//////////////////////////////////////////////////////////////////////////////

# ~ 3 ~ 
# Convert EMX for RD3 Release (i.e., new freeze)

convertFreezeEmx = Convert(
    files = [
        'model/base_rd3.yaml',
        'model/base_rd3_freeze.yaml'
    ]
)

convertFreezeEmx.convert()

# recode RD3 release: use freezeN as pattern
# rNumr = "freeze3"
# rName = "Freeze3"

rNumr = "novelwgs"
rName = "Novel Omics WGS"

# rNumr = "noveldeepwes"
# rName = "Novel Omics Deep-WES"

# rNumr = "novelsrwgs"
# rName = "Novel Omics SR-WGS"

rFile = 'rd3_' + rNumr

setEmxRelease(convertFreezeEmx.packages, releaseNumr = rNumr, releaseTitle = rName)
setEmxRelease(convertFreezeEmx.entities, releaseNumr = rNumr, releaseTitle = rName)
setEmxRelease(convertFreezeEmx.attributes, releaseNumr = rNumr, releaseTitle = rName)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# OPTIONAL: NOVELOMICS RELEASES
# since models are split across multiple files, the following steps will merge
# the novelomics models with the main release model.

# convert yaml
# convertNovelomicsEmx = Convert(files = ['src/emx/rd3_novelomics.yaml'])
# convertNovelomicsEmx.convert()
# convertNovelomicsEmx.packages
# convertNovelomicsEmx.entities
# convertNovelomicsEmx.attributes

# remove existing `labinfo` entities
# convertFreezeEmx.entities = [
#     d for d in convertFreezeEmx.entities if not ('labinfo' == d.get('name'))
# ]

# # remove all `labinfo` attributes
# convertFreezeEmx.attributes = [
#     d for d in convertFreezeEmx.attributes if not (
#         'rd3_novelomics_labinfo' == d.get('entity')
#     )
# ]

# # Merge EMX structures
# convertFreezeEmx.entities = convertFreezeEmx.entities + convertNovelomicsEmx.entities
# convertFreezeEmx.attributes = convertFreezeEmx.attributes + convertNovelomicsEmx.attributes
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Save model
convertFreezeEmx.write(name = rFile, format = 'xlsx', outDir = 'dist/')


#//////////////////////////////////////////////////////////////////////////////

# ~ 4 ~
# GENERAL PORTAL TABLES

convertPortal = Convert(files = ['src/emx/rd3_portal_cluster.yaml'])
convertPortal.convert()
convertPortal.packages
convertPortal.entities
convertPortal.attributes

convertPortal.write(
    name = 'rd3_portal_cluster',
    format = 'xlsx',
    outDir = 'dist/'
)