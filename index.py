#'////////////////////////////////////////////////////////////////////////////
#' FILE: emx.py
#' AUTHOR: David Ruvolo
#' CREATED: 2021-09-16
#' MODIFIED: 2022-05-09
#' PURPOSE: incorporate YAML to EMX generator
#' STATUS: stable
#' PACKAGES: yamlemxconvert
#' COMMENTS: NA
#'////////////////////////////////////////////////////////////////////////////

from rd3.utils.emxtools import setEmxRelease, writeEmxTemplate
from yamlemxconvert.convert import Convert
from yaml import safe_load


# read EMX config file
with open('model.yaml', 'r') as file:
    config = safe_load(file)
    file.close()


# find files and remove portal_release as that will be added separately
portalEmxConfig=[model for model in config['models'] if model['name']=='rd3_portal'][0]

# convert portal-emx
portalEmx=Convert(files=portalEmxConfig['files'])
portalEmx.convert()

portalEmx.write(name='rd3_portal', outDir=config['outputPaths']['main'])
portalEmx.write_schema('schemas/rd3_portal.md')

# for each release: render the model and adjust the entity identifiers
# portalEmxReleases = portalEmxConfig['releases']
# for release in portalEmxReleases:
#     print('Building model for release:',release)
#     releaseEmx=Convert(files=['model/rd3_portal_release.yaml'])
#     releaseEmx.convert()
    
#     releaseEmx.entities[0]['name']=release
#     releaseEmx.entities[0]['label']=f"Data {portalEmxReleases[release]}"
#     releaseEmx.entities[0]['description']=f'Staging table for {portalEmxReleases[release]}'
    
#     for attribute in releaseEmx.attributes:
#         attribute['entity']=f"rd3_portal_release_{release}"
    
#     portalEmx.entities.extend(releaseEmx.entities)
#     portalEmx.attributes.extend(releaseEmx.attributes)


# ~ 0 ~
# Compile EMX for RD3 Portal Releases
# Comple the YAML-EMX markup for new RD3 staging tables
# See `model/rd3_portal_release.yaml` for additional notes

# convertPortalReleaseEmx = Convert(files = [
#     'model/base_rd3_portal.yaml', # import portal first
#     'model/rd3_portal_release.yaml'
# ])

# convertPortalReleaseEmx.convert()

# set release information
# convertPortalReleaseEmx.entities[0]['name'] = 'freeze3'
# convertPortalReleaseEmx.entities[0]['label'] = 'Freeze 3'
# convertPortalReleaseEmx.entities[0]['description'] = ' Staging table for Freeze 3 (2022-03-09)'

# for d in convertPortalReleaseEmx.attributes:
#     d['entity'] = f"rd3_portal_release_{convertPortalReleaseEmx.entities[0]['name']}"

# convertPortalReleaseEmx.write(
#     name = 'rd3_portal_release',
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

# rNumr = "novelwgs"
# rName = "Novel Omics WGS"

# rNumr = "noveldeepwes"
# rName = "Novel Omics Deep-WES"

# rNumr = "novelsrwgs"
# rName = "Novel Omics SR-WGS"

rNumr = "novelrnaseq"
rName = "Novel Omics RNAseq"

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

